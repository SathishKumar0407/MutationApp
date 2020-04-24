import json
env = json.load(open("env.json", 'r'))

#OCR related values
DB2_CONN_URL = env['DB2_CONN_URL']
PORT = env['PORT']
DB2_CS_CONN = env['DB2_CS_CONN']
DB2_CR_CONN = env['DB2_CR_CONN']

qrycache = json.load(open("querycache.json", 'r'))
QUERY_CACHE = qrycache['QUERY_CACHE']
