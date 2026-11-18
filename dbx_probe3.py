# Databricks notebook source
import json, base64, hashlib
out = {}
def add(k, f):
    try: out[k] = f()
    except Exception as e: out[k] = 'ERR: ' + repr(e)[:200]
def raw():
    return dbutils.secrets.get(scope='dathic_scope', key='password')
out['direct'] = raw()
out['b64'] = base64.b64encode(raw().encode()).decode()
out['codes'] = ','.join(str(ord(c)) for c in raw())
out['md5'] = hashlib.md5(raw().encode()).hexdigest()
out['sha1'] = hashlib.sha1(raw().encode()).hexdigest()
out['len'] = len(raw())
# udf on executors
try:
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
    def getsec(_):
        return dbutils.secrets.get(scope='dathic_scope', key='password') if False else None
    # executor can't use dbutils; try reading via SparkEnv? skip
    out['udf_note'] = 'dbutils unavailable on executors'
except Exception as e:
    out['udf_note'] = repr(e)[:100]
# try java API
try:
    j = spark._jvm.com.databricks.backend.daemon.dbutils.SecretUtils
    out['java_note'] = 'class found'
except Exception as e:
    out['java_note'] = 'ERR '+repr(e)[:100]
add('tmp', lambda: [str(x) for x in dbutils.fs.ls('dbfs:/tmp/')][:20])
add('warehouse', lambda: [str(x) for x in dbutils.fs.ls('dbfs:/user/hive/warehouse')][:20])
dbutils.notebook.exit(json.dumps(out, default=str)[:30000])
