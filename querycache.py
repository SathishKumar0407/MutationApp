import json
import re
from constants import QUERY_CACHE

class QueryCache(object):
    def __init__(self,scenario):
        self.scenario = scenario

    #filter out the order/group by clause from the query
    def searchquery(self):
        query_cache = QUERY_CACHE
        fnlquery = ''
        for i in range(len(query_cache)):
            if (self.scenario.upper() == query_cache[i]['scenario'].upper()):
                fnlquery = query_cache[i]['query']
                break

        output = {'data': '', 'error': ''}
        if (fnlquery != ''):
            output['data'] = fnlquery
        else:
            output['error'] = 'NO_SCENARIO_MATCHES'

        return(output)
