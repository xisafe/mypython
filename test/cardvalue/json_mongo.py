import json
fp = open('/Users/hua/json2.txt')
lines = fp.read()
j=json.load(lines)
fp.close()