# Databricks notebook source
import json, base64, urllib.request, urllib.error
out = {}

def add(k, f):
    try:
        v = f()
        s = json.dumps(v, default=str)
        out[k] = s[:38000] + '...TRUNC' if len(s) > 38000 else v
    except Exception as e:
        out[k] = 'ERR: ' + repr(e)[:300]

def http(url, headers=None, timeout=10, data=None, method=None):
    req = urllib.request.Request(url, headers=headers or {}, data=data, method=method)
    try:
        r = urllib.request.urlopen(req, timeout=timeout)
        return r.status, dict(r.headers), r.read()[:200000].decode('utf-8', 'replace')
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()[:20000].decode('utf-8', 'replace')

AF = 'http://54.160.252.9:8088'
AUTH = 'Basic ' + base64.b64encode(b'dathic:DathicPwd2020.').decode()

def af_version():
    return http(AF + '/api/v1/version', {'Authorization': AUTH})

def af_vars():
    return http(AF + '/api/v1/variables?limit=200', {'Authorization': AUTH})

def af_conns():
    return http(AF + '/api/v1/connections?limit=200', {'Authorization': AUTH})

def af_vars_ui():
    # form login then UI JSON endpoints
    import urllib.parse
    data = urllib.parse.urlencode({'username': 'dathic', 'password': 'DathicPwd2020.'}).encode()
    req = urllib.request.Request(AF + '/login/', data=data, method='POST',
                                 headers={'Content-Type': 'application/x-www-form-urlencoded'})
    import http.cookiejar
    cj = http.cookiejar.CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    try:
        r = op.open(req, timeout=10)
        st = r.status
    except urllib.error.HTTPError as e:
        st = e.code
    ck = '; '.join(f'{c.name}={c.value}' for c in cj)
    v = op.open(urllib.request.Request(AF + '/variable/list/', headers={'Cookie': ck}), timeout=10).read()
    return {'login_status': st, 'cookies': len(cj), 'vars_html_head': v[:3000].decode('utf-8', 'replace')}

def af_dags():
    return http(AF + '/api/v1/dags?limit=100', {'Authorization': AUTH})

def other_hosts():
    r = {}
    for u in ['http://34.200.236.84/', 'http://34.200.236.84:80/health',
              'http://54.157.17.216:3000/', 'http://54.157.17.216:3000/api/health',
              'http://54.160.252.9:8088/health',
              'http://54.160.252.9:8080/', 'http://10.89.0.2:53/']:
        try:
            st, hd, bd = http(u, timeout=5)
            r[u] = [st, bd[:600]]
        except Exception as e:
            r[u] = 'ERR ' + repr(e)[:150]
    return r

add('af_version', af_version)
add('af_vars_api', af_vars)
add('af_conns_api', af_conns)
add('af_vars_ui', af_vars_ui)
add('af_dags', af_dags)
add('other_hosts', other_hosts)
dbutils.notebook.exit(json.dumps(out, default=str)[:950000])