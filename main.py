#pylint: disable=missing-docstring
import json
import re
from flask import Flask, request, jsonify, Response
from queryparser import ParseQuery
from querytuning import TuneQuery
from querycache import QueryCache
import pandas as pd

pd.options.display.max_columns = None

from constants import PORT

app = Flask(__name__)

@app.route("/", methods=['GET', 'POST'])
def main():
    return "ADAT - Mutation Model"

@app.route('/pong', methods=['GET'])
def pong():
    return Response(status=200)

def constructresp(status_code, sub_code, message, input, action):
    response = jsonify({
        'status': status_code,
        'sub_code': sub_code,
        'message': message,
        'input': input,
        'output': action
    })
    response.status_code = status_code
    return response

#function to identify the query from query cache using scenario
def findqueryusingscenario(scenario):
    output = {'data': '' ,'error': ''}
    if scenario != '':
        try:
            #initiate the query cache process
            querycacheobj = QueryCache(scenario)
            fnlqrycachejson = querycacheobj.searchquery()

            if (fnlqrycachejson['data'] != ''):
                output['data'] = fnlqrycachejson['data']
            else:
                output['error'] = fnlqrycachejson['error']
            return output
        except ValueError:
            output['error'] = 'QUERY_CACHE_ERROR'
            return output          
    else:
        output['error'] = 'MISSING_SCENARIO'
        return output

def findqueryusingcondition(tune_input,query):
    #condition tuning logic
    if query != '':
        try:
            #initiate the parse query process
            parseqryobj = ParseQuery(query)
            fnlparseqryjson = parseqryobj.filterquery()

            fltsqlstmnt = fnlparseqryjson['fltsqlstmnt']
            iswhereposexist = fnlparseqryjson['iswhereposexist']
            fltordgrpstmt = fnlparseqryjson['fltordgrpstmt']

            output_query = ''
            #check where position exist:
            #if exist, then apply mutation logic
            #otherwise, use the same input query as output
            if (iswhereposexist):
                #initiate the tuning query process
                qrytuningobj = TuneQuery(fltsqlstmnt,fltordgrpstmt)
                fnlqrytuningjson = qrytuningobj.processquerytuning()
                
                if (fnlqrytuningjson['message'] == 'SUCCESS' and fnlqrytuningjson['query'] != ''):
                    output_query = fnlqrytuningjson['query']
                    return constructresp(200, 42, 'SUCCESS', tune_input, output_query)
                else:
                    return constructresp(400, 42, 'TUNE_ERROR', tune_input, fnlqrytuningjson['query'])
            else:
                output_query = query
                return constructresp(200, 42, 'SUCCESS', tune_input, output_query)

        except ValueError:
            return constructresp(400, 42, 'PARSE_ERROR', tune_input, 'Application unable to process the input')
    else:
        return constructresp(400, 42, 'MISSING_QUERY_PARAMETER', tune_input, 'Empty \'query\' paramater')

@app.route("/mutation/", methods=['GET', 'POST'])
def mutation_logic():
    query = request.args.get('query')
    scenario = request.args.get('scenario')
    #get the input    
    print ('Query: ',query)
    #to print the input
    print ('Scenario: ',scenario)

    tune_input = {'scenario': scenario, 'query': query}
    
    #to identify the query from query cache using scenario
    scenario_output = findqueryusingscenario(scenario)
    #print (scenario_output)
    if (scenario_output['data'] != ''):
        return constructresp(200, 42, 'SUCCESS', tune_input, scenario_output['data'])
    else:
        #condition tuning logic
        return findqueryusingcondition(tune_input,query)
    
@app.route("/querycache/", methods=['GET', 'POST'])
def stored_cache():
    scenario = request.args.get('scenario')
    #get the input
    print('Scenario: ',scenario)
    if scenario != '':
        try:
            #initiate the query cache process
            querycacheobj = QueryCache(scenario)
            fnlqrycachejson = querycacheobj.searchquery()

            if (fnlqrycachejson['data'] != ''):
                return constructresp(200, 42, 'SUCCESS', scenario, fnlqrycachejson['data'])
            else:
                return constructresp(400, 42, 'QUERY_CACHE_ERROR', scenario, fnlqrycachejson['error'])
        except ValueError:
            return constructresp(400, 42, 'QUERY_CACHE_ERROR', scenario, 'Application unable to process the input')
    else:
        return constructresp(400, 42, 'MISSING_SCENARIO', scenario, 'Empty \'scenario\' paramater')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=PORT)
