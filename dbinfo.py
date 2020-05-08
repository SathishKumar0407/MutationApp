#pylint: disable=missing-docstring
import json
# for get and post request
import requests
from constants import DB2_CONN_URL

class ConnectDatabase:

    def __init__(self,query,schemaname):
        self.query = query
        self.schemaname = schemaname

    def executequery(self):
        output_json = {}
        output_json['data'] = ""
        output_json['error'] = ""

        url_request = DB2_CONN_URL
        
        #construct the url to call the db
        operation_url = url_request + '?query=' + self.query + '&schemaName=' + self.schemaname
        #print(operation_url)
        analysis = ''
        try:
            response_final = requests.get(operation_url)
            #Response from DB
            #print('response_final', response_final.json())
            if (response_final.status_code == 200):
                resp_json = response_final.json()
                data = resp_json['data']
                error = resp_json['error']

                if (error != ''):
                    analysis = 'INVALID_QUERY'
                elif (int(data[0]['CNT']) > 0):
                    analysis = 'VALID_QUERY'
                else:
                    analysis = 'NO_DATA'
            else:
                analysis = 'CONN_ERROR'
        except:
            #print('##################API_REQUEST_ERROR#######################')
            analysis = 'API_REQUEST_ERROR'

        return analysis
