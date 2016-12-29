#encoding:utf-8
import urllib2
import json
url = 'https://www.juxinli.com/api/access_report_token?client_secret=6faf4041d77340c197dc8e49f625f0d1&hours=24&org_name=cardvalue'
req = urllib2.Request(url)
res = urllib2.urlopen(req)
jstr= res.read()
print jstr
if len(jstr) > 0 :
        decoded = json.loads(jstr.decode('utf-8', 'ignore'))
        print  decoded
        if decoded["success"] =='true':
             decoded["access_token"]
        else:
            print '----get JXL token failed'
            exit()