import json
import re
import pandas as pd

#using request method to connect the db and get the data
#removed to use the db connection directly instead send the request to ADAT App to get the data
from dbinfo import ConnectDatabase

PRE_CNT_STMT = 'SELECT COUNT(*) AS CNT FROM'

#using packages to connect the db and get the data directly from this app itself
#from dbconn import ConnectDatabase

#to get the schema name from string
def getschemaname(phrase):        
    word_list = phrase.split('.')[0].split('(')
    schema_nm = word_list[len(word_list)-1]
    return schema_nm

#to parse the condition from string after removing the brackets which is not required
def parsecondition(org_cond_str):
    der_cond_str = org_cond_str
    #remove the prefix brackets 
    if (org_cond_str[0] == '('):
        der_cond_str = der_cond_str[1:]

    #remove the suffix brackets
    if (org_cond_str.count(" IN ") > 0 and org_cond_str[-2:] == '))'):
        der_cond_str = der_cond_str[0:len(der_cond_str)-1]

    #if there is no 'IN' condition and last character is bracket, then remove the bracket
    if (org_cond_str.count(" IN ") <= 0 and org_cond_str[-1] == ')'):
        der_cond_str = der_cond_str[0:len(der_cond_str)-1]

    return der_cond_str

#to get the last condition
def loadlastcondition(dfconditions,sqlstmntsplt,prevpointer,isprevwherekey):
    #to process the last condition
    if (prevpointer > 0): 
        if (isprevwherekey == True):
            vschemaname = getschemaname(sqlstmntsplt[prevpointer+1])
            #removed to get the proper schema name
            #vschemaname = sqlstmntsplt[prevpointer+1].split('.')[0].replace('(','')
            vtablename = sqlstmntsplt[prevpointer+1].split('.')[1]
            vprestring = str(" ".join(sqlstmntsplt[:prevpointer+1]))            
            dercondition = str(" ".join(sqlstmntsplt[prevpointer+1:]))
            vcondition = parsecondition(dercondition)
            vdatareturned = "N/A"
            vjoinreturned = "N/A"

            dfconditions = dfconditions.append(
                {"SchemaName": vschemaname, "TableName": vtablename, "PreString": vprestring, "Condition": vcondition,
                "DataReturned": vdatareturned, "JoinReturned": vjoinreturned}, ignore_index=True)
        else:
            vschemaname = getschemaname(sqlstmntsplt[prevpointer+1])
            #removed to get the proper schema name
            #vschemaname = sqlstmntsplt[prevpointer+1].split('.')[0].replace('(','')
            vtablename = sqlstmntsplt[prevpointer+1].split('.')[1]
            vprestring = str(" ".join(sqlstmntsplt[prevpointer:prevpointer+1]))
            dercondition = str(" ".join(sqlstmntsplt[prevpointer+1:]))
            vcondition = parsecondition(dercondition)
            vdatareturned = "N/A"
            vjoinreturned = "N/A"

            dfconditions = dfconditions.append(
                {"SchemaName": vschemaname, "TableName": vtablename, "PreString": vprestring, "Condition": vcondition,
                "DataReturned": vdatareturned, "JoinReturned": vjoinreturned}, ignore_index=True)
    return dfconditions

#check the availability of data
def checkdataavailablity(dfconditions):
    for index,row in dfconditions.iterrows():
        #to check the join condition or not
        joincondjson = checkisjoincondition(dfconditions.iloc[index].Condition)

        if (joincondjson['isjoincondition'] == True):
            vdatareturnedquery = PRE_CNT_STMT + " " + dfconditions.iloc[index].SchemaName + "." + joincondjson['table1'] + ',' + dfconditions.iloc[index].SchemaName + "." + joincondjson['table2'] + " WHERE " + dfconditions.iloc[index].Condition + " WITH UR"
        else:
            vdatareturnedquery = PRE_CNT_STMT + " " + dfconditions.iloc[index].SchemaName + "." + dfconditions.iloc[index].TableName + " WHERE " + dfconditions.iloc[index].Condition + " WITH UR"

        #removed as it is work for normal case, suppose if query like 'SELECT * FROM CSADM.V_SAD,CSADM.FMOS_TKT_PEND WHERE (CSADM.V_SAD.KY_PREM_NO=CSADM.FMOS_TKT_PEND.KY_PREM_NO)'
        #In this case pre string contain two table and condition is joining the table keys
        #But our below query construct, checking the count (*) in first table alone and condition contains two table. So query error will raise.
        #vdatareturnedquery = PRE_CNT_STMT + " " + dfconditions.iloc[index].SchemaName + "." + dfconditions.iloc[index].TableName + " WHERE " + dfconditions.iloc[index].Condition
        
        #print ('vdatareturnedquery: ',vdatareturnedquery)
        vschemaname = dfconditions.iloc[index].SchemaName
        #print('vschemaname: ',vschemaname)
        #removed for endpoint connection
        #vdatareturnedqueryresult = pd.read_sql(vdatareturnedquery,connectionString)
        #call the db2 using request method

        try:
            vdatareturnedqryobj = ConnectDatabase(vdatareturnedquery,vschemaname)

            #using request method to connect the db and get the data
            vdatareturnedqryrslt = vdatareturnedqryobj.executequery()

            #using packages to connect the db and get the data
            #qryrslt = vdatareturnedqryobj.executequery()
            #vdatareturnedqryrslt = checkqueryresult(qryrslt)
            
            #print('vdatareturnedqryrslt: ',vdatareturnedqryrslt)
            vdatareturned = 'N'
            if (vdatareturnedqryrslt == 'VALID_QUERY'):
                vdatareturned = 'Y'
            dfconditions.iloc[index].DataReturned = vdatareturned
        except:
            #print('###########Execute Query Failed############')
            dfconditions.iloc[index].DataReturned = 'N'
            continue
         
    return dfconditions

#to check whether it is a join condition or not
def checkisjoincondition(condition):
    condsplit = condition.split('.')
    isjoincondition = False
    if (len(condsplit) > 4):
        tbl1 = condsplit[1]
        tbl2 = condsplit[3]
        isjoincondition = True
    else:
        tbl1 = condsplit[1]
        tbl2 = ''
    return ({'table1': tbl1,'table2': tbl2,'isjoincondition': isjoincondition})

#to check the query result
def checkqueryresult(qryrslt):
    analysis = ''
    if (len(qryrslt) > 0):
        #to get the count
        #print("CNT: ", qryrslt[0]['CNT'])
        if (qryrslt[0]['CNT'] > 0):
            analysis = 'VALID_QUERY'
        else:
            analysis = 'NO_DATA'
    else:
        analysis = 'NO_DATA'
    return analysis

#check and construct the pre string
def constructprestring(dfconditions):
    if dfconditions.iloc[0].DataReturned == "N":
        #print('E n t e r e d . . . . . 1')
        prestringfirst = dfconditions.iloc[0].PreString
        for index, row in dfconditions.iterrows():
            #print('E n t e r e d . . . . . Loop')
            if dfconditions.iloc[index].DataReturned == "Y":
                #print('E n t e r e d . . . . . 2')
                dfconditions.iloc[index].PreString = prestringfirst
                break
    return  dfconditions

#to construct the tuned query
def constructtunedquery(dfconditions):
    adatquery = ''
    #Create ADAT Query with the conditions individualy returning values
    for index,row in dfconditions.iterrows():
        if dfconditions.iloc[index].DataReturned == "Y":
            adatquery = adatquery + " " + dfconditions.iloc[index].PreString + " " + dfconditions.iloc[index].Condition
            #print(adatquery)
    return adatquery

#to process the tuned query
def processtunedquery(adatquery,fltordgrpstmt):
    fnl_out_json = {'message': '', 'query': ''}
    if (adatquery != ''):
        adatquery = adatquery.strip() + ' ' + fltordgrpstmt
        fnl_out_json['query'] = adatquery.strip()
        fnl_out_json['message'] = 'SUCCESS'
    else:
        fnl_out_json['query'] = ''
        fnl_out_json['message'] = 'FAILED'
    return fnl_out_json

##########################TuneQuery Class - Starts here###########################
class TuneQuery(object):

    def __init__(self,fltsqlstmnt,fltordgrpstmt):
        self.fltsqlstmnt = fltsqlstmnt
        self.fltordgrpstmt = fltordgrpstmt     

    #filter out the order/group by clause from the query
    def processquerytuning(self):
        #to find the order or group clause exist in the sql statement.
        fltsqlstmnt = self.fltsqlstmnt
        sqlstmntsplt =  fltsqlstmnt.upper().split(" ")

        ##Variable Declaration
        dfconditions = pd.DataFrame(columns=["SchemaName", "TableName", "PreString", "Condition", "DataReturned", "JoinReturned"])
        prevpointer = 0
        first = 0
        isprevwherekey = False
        lstqrystr = ''

        ##Main logic for condition identification & storing in temporary DataFrame
        for i in range(len(sqlstmntsplt)):
            if sqlstmntsplt[i] == 'WHERE':
                prevpointer = i
                isprevwherekey = True
            elif first == 0 and prevpointer > 0 and (sqlstmntsplt[i] == 'AND' or sqlstmntsplt[i] == 'OR'):
                vschemaname = getschemaname(sqlstmntsplt[prevpointer + 1])
                #removed to get the proper schema name
                #vschemaname =  sqlstmntsplt[prevpointer+1].split('.')[0].replace('(','')
                vtablename = sqlstmntsplt[prevpointer+1].split('.')[1]
                #removed to get the proper string
                #vprestring = str(" ".join(sqlstmntsplt[:i-prevpointer+1]))
                vprestring = str(" ".join(sqlstmntsplt[:prevpointer+1]))                
                #removed to get the proper condition
                #vcondition = str(" ".join(sqlstmntsplt[(prevpointer+1):i]))
                dercondition = str(" ".join(sqlstmntsplt[(prevpointer+1):i]))
                vcondition = parsecondition(dercondition)
                vdatareturned = "N/A"
                vjoinreturned = "N/A"

                dfconditions = dfconditions.append(
                    {"SchemaName":vschemaname, "TableName":vtablename, "PreString":vprestring, "Condition":vcondition,
                    "DataReturned":vdatareturned, "JoinReturned":vjoinreturned}, ignore_index=True)
                first = 1
                prevpointer = i
                isprevwherekey = False
                lstqrystr = ''
                dfconditions = pd.DataFrame({"SchemaName":[vschemaname], "TableName":[vtablename], "PreString":[vprestring], "Condition":[vcondition], "DataReturned":[vdatareturned], "JoinReturned":[vjoinreturned]})
            elif first == 1 and prevpointer > 0 and (sqlstmntsplt[i] == 'AND' or sqlstmntsplt[i] == 'OR'):
                vschemaname = getschemaname(sqlstmntsplt[prevpointer+1])
                #removed to get the proper schema name
                #vschemaname = sqlstmntsplt[prevpointer + 1].split('.')[0].replace('(','')
                vtablename = sqlstmntsplt[prevpointer+1].split('.')[1]
                vprestring = str(" ".join(sqlstmntsplt[prevpointer:prevpointer+1]))
                dercondition = str(" ".join(sqlstmntsplt[prevpointer+1:i]))
                vcondition = parsecondition(dercondition)
                vdatareturned = "N/A"
                vjoinreturned = "N/A"

                dfconditions = dfconditions.append(
                    {"SchemaName": vschemaname, "TableName": vtablename, "PreString": vprestring, "Condition": vcondition,
                    "DataReturned": vdatareturned, "JoinReturned":vjoinreturned}, ignore_index=True)
                first = 1
                prevpointer = i
                isprevwherekey = False
                lstqrystr = ''
            #to handle last condition 
            elif prevpointer > 0:
                if lstqrystr != '':
                    lstqrystr =  lstqrystr + ' ' + sqlstmntsplt[i]
                else:
                    lstqrystr =  sqlstmntsplt[i]

        #to process the last condition
        dfconditions = loadlastcondition(dfconditions,sqlstmntsplt,prevpointer,isprevwherekey)

        #print the initial df conditions
        #print ('###################Initial DF Conditions#################')
        #print ('lstqrystr: ',lstqrystr)
        #print (dfconditions)

        #Data availability validation for each individual conditions & sorting Flag value in temporary DataFrame
        dfconditions = checkdataavailablity(dfconditions)

        #print the df conditions after testing
        #print ('###################After Testing Conditions#################')
        #print(dfconditions)

        #Construction of ADAT Query
        #If the first condition is not returning values
        dfconditions = constructprestring(dfconditions)

        #print the check the pre string in data frame
        #print ('####################After PreString################')
        #print (dfconditions)

        #Check the condition by joining each condition one by one
        #Check whether any condition is not returning any data, ignore the condition
        fnl_out_json = checkdatabyjoinoperation(dfconditions)
        if (fnl_out_json['query'] != ''):
            adatquery = fnl_out_json['query'].strip() + ' ' + self.fltordgrpstmt
            fnl_out_json['query'] = adatquery.strip()
            fnl_out_json['message'] = 'SUCCESS'

        #Create ADAT Query with the conditions individually returning values
        #adatquery = constructtunedquery(dfconditions)

        #removed for join condition logic on 03/25/2020
        #print ('####################ADAT Tune Query###################')
        #fnl_out_json = processtunedquery(adatquery,self.fltordgrpstmt)

        #to print the final output
        #print (fnl_out_json)

        return(fnl_out_json)
##########################TuneQuery Class - Ends here###########################

##########################Condition Logic - Starts here###########################
#to check whether any condition is not returning any data, ignore the condition
def checkdatabyjoinoperation(dfconditions):
    fnl_out_json = {'message': '', 'query': ''}
    
    #to check the data condition exist and create the 'dfjoincondition' frame
    joinrsltjson = checkdatareturnedexist(dfconditions)

    isdatareturnedexist = joinrsltjson['isdatareturnedexist']
    dfjoincond = joinrsltjson['dfjoincond']

    if (isdatareturnedexist):
        #to check the join condition and execute the query
        cons_tune_qry = checkjoincondition(dfjoincond)

        cons_tune_qry = cons_tune_qry.replace('COUNT(*) AS CNT','*')
        fnl_out_json['query'] = cons_tune_qry    
    else:
        #no data returned using the count(*) of each condition individually
        fnl_out_json['message'] = "INVALID_CONDITIONS"
   
    return fnl_out_json

#to check the data condition exist for joining condition logic
def checkdatareturnedexist(dfconditions):
    #Variable Declaration
    dfjoincond = pd.DataFrame(columns=["SchemaName", "TableName", "PreString", "Condition", "DataReturned", "JoinReturned"])

    isdatareturnedexist = False
    #Create ADAT Query with the conditions individualy returning values
    for index,row in dfconditions.iterrows():
        if dfconditions.iloc[index].DataReturned == "Y":
            isdatareturnedexist = True
            dfjoincond = dfjoincond.append(
                {"SchemaName":dfconditions.iloc[index].SchemaName,
                "TableName":dfconditions.iloc[index].TableName,
                "PreString":dfconditions.iloc[index].PreString,
                "Condition":dfconditions.iloc[index].Condition,
                "DataReturned":dfconditions.iloc[index].DataReturned,
                "JoinReturned":dfconditions.iloc[index].JoinReturned},
                ignore_index=True)
    return ({'dfjoincond': dfjoincond, 'isdatareturnedexist': isdatareturnedexist})

#to check the join condition and execute the query
def checkjoincondition(dfjoincond):
    #join condition logic
    #print("##########Join Condition Started#########")
    vtuneprestmt = ''
    tbl_list = []
    cond_list = []
    cons_tune_qry = ''
    for index,row in dfjoincond.iterrows():
        if (index == 0):
            #print("First Index: ",index)
            dfjoincond.iloc[index].JoinReturned = "Y"

            #to process the first join condition in the query
            fistrsltjson = processfirstjoincondition(dfjoincond,index,tbl_list,cond_list)
            vtuneprestmt = fistrsltjson['vtuneprestmt']
            tbl_list = fistrsltjson['tbl_list']
            cond_list = fistrsltjson['cond_list']

            #re-initialize the final tune query
            cons_tune_qry = vtuneprestmt + dfjoincond.iloc[index].Condition
            #print("#######First Index Completed######")
        else:
            #nothing to add
            #print("Next Index: ",index)
            #tmp_tuneprestmt = vtuneprestmt
            #re-initalize, it will change if new table exist
            der_tuneprestmt = vtuneprestmt
            exec_tuneprestmt = ''
            #removed for code split
            #tmp_tbl_list = []
            #der_tbl_list = []

            #to get the derived table list for next join condition
            der_tbl_list = getderivedtablelist(dfjoincond,index,tbl_list)

            #if new table exist in the condition, so construct the new pre string using 'tbl_list' and 'der_tbl_list'
            if (len(der_tbl_list) > 0):
                #print("#######New Table Loop#######")
                #to get the identified query statement for new table loop
                fnlexecstmtjson = processnewtableloop(dfjoincond,index,tbl_list,cond_list,der_tbl_list)
                #removed as it is not required
                #tmp_tuneprestmt = fnlexecstmtjson['tmp_tuneprestmt']
                der_tuneprestmt = fnlexecstmtjson['der_tuneprestmt']
                exec_tuneprestmt = fnlexecstmtjson['exec_tuneprestmt']
            else:
                #print("######No New Table Loop######")
                fnlexecstmtjson = processnewtableloop(dfjoincond,index,tbl_list,cond_list,der_tbl_list)
                #removed as it is not required
                #tmp_tuneprestmt = fnlexecstmtjson['tmp_tuneprestmt']
                exec_tuneprestmt = fnlexecstmtjson['exec_tuneprestmt']

            #load the captured data in single json to pass the paramater in the function easily
            gbltunejson = {
                'exec_tuneprestmt': exec_tuneprestmt,
                'der_tuneprestmt': der_tuneprestmt,
                'vtuneprestmt': vtuneprestmt,
                'cond_list': cond_list,
                'tbl_list': tbl_list,
                'der_tbl_list': der_tbl_list,
                'cons_tune_qry': cons_tune_qry
            }

            #to check and execute the query and update the join condition index
            fnlqueryrsltjson = executetunequery(dfjoincond,index,gbltunejson)
            dfjoincond = fnlqueryrsltjson['dfjoincond']
            vtuneprestmt = fnlqueryrsltjson['vtuneprestmt']
            cond_list = fnlqueryrsltjson['cond_list']
            tbl_list = fnlqueryrsltjson['tbl_list']
            cons_tune_qry = fnlqueryrsltjson['cons_tune_qry']

    #print the output
    #print('final - vtuneprestmt: ',vtuneprestmt)
    #print('final - cond_list: ',cond_list)
    #print('final - tune_qry: ',cons_tune_qry)
    return cons_tune_qry

#to process the first join condition in the query
def processfirstjoincondition(dfjoincond,index,tbl_list,cond_list):
    joincondjson = checkisjoincondition(dfjoincond.iloc[index].Condition)
    if (joincondjson['isjoincondition'] == True):
        vtuneprestmt = PRE_CNT_STMT + " " + dfjoincond.iloc[index].SchemaName + "." + joincondjson['table1'] + ',' + dfjoincond.iloc[index].SchemaName + "." + joincondjson['table2'] + " WHERE "
        tbl_list.append(joincondjson['table1'])
        tbl_list.append(joincondjson['table2'])                    
    else:
        vtuneprestmt = PRE_CNT_STMT + " " + dfjoincond.iloc[index].SchemaName + "." + dfjoincond.iloc[index].TableName + " WHERE "
        tbl_list.append(dfjoincond.iloc[index].TableName)

    cond_list.append(dfjoincond.iloc[index].Condition)
    #print ('vtuneprestmt: ',vtuneprestmt)
    return ({'vtuneprestmt': vtuneprestmt,'tbl_list': tbl_list, 'cond_list': cond_list})

#to get the derived table list for next join condition in the query
def getderivedtablelist(dfjoincond,index,tbl_list):
    tmp_tbl_list = []
    der_tbl_list = []

    joincondjson = checkisjoincondition(dfjoincond.iloc[index].Condition)
    if (joincondjson['isjoincondition'] == True):
        tmp_tbl_list.append(joincondjson['table1'])
        tmp_tbl_list.append(joincondjson['table2'])                    
    else:
        tmp_tbl_list.append(joincondjson['table1'])

    #print ('tmp_tbl_list: ',tmp_tbl_list)
    for val in tmp_tbl_list:
        tmp_tbl = val
        istmptblexist = False
        for mst_tbl in tbl_list:
            if (tmp_tbl == mst_tbl):
                istmptblexist = True
                break

        if (istmptblexist == False):
            der_tbl_list.append(tmp_tbl)

    #print('der_tbl_list: ',der_tbl_list)
    return der_tbl_list

#to get the identified query statement for new table loop
def processnewtableloop(dfjoincond,index,tbl_list,cond_list,der_tbl_list):
    #to construct the consolidated table list using table list and derived table list
    cons_tbl_list = getconsolidatedtablelist(tbl_list,der_tbl_list)

    #construct the new pre string using 'cons_tbl_list' (i.e., select clause and table clause)
    tmp_tuneprestmt = gettempprestmtloop1(dfjoincond,index,cons_tbl_list)

    #print the constructed tune previous string using table list
    #print('tmp_tuneprestmt string: ',tmp_tuneprestmt)

    #set the derived previous string alone till "WHERE" word
    #re-initalize
    der_tuneprestmt = tmp_tuneprestmt

    #add condition to previous string (i..e, where condition clause)
    #to construct the previous statement to add conditions
    tmp_tuneprestmt = gettempprestmtloop2(dfjoincond,index,cond_list,tmp_tuneprestmt)

    #add the new condition (i..e, current processing condition)
    tmp_tuneprestmt = tmp_tuneprestmt + " " + dfjoincond.iloc[index].PreString + " " + dfjoincond.iloc[index].Condition

    exec_tuneprestmt = tmp_tuneprestmt
    #print('exec_tuneprestmt: ',exec_tuneprestmt)

    return ({'tmp_tuneprestmt': tmp_tuneprestmt,'der_tuneprestmt': der_tuneprestmt,'exec_tuneprestmt': exec_tuneprestmt})

#to construct the consolidated table list using table list and derived table list
def getconsolidatedtablelist(tbl_list,der_tbl_list):
    cons_tbl_list = []
    for mst_tbl in tbl_list:
        cons_tbl_list.append(mst_tbl)

    for der_tbl in der_tbl_list:
        cons_tbl_list.append(der_tbl)
    
    return cons_tbl_list

#to construct the previous statement to add select and table statement
def gettempprestmtloop1(dfjoincond,index,cons_tbl_list):
    #construct the new pre string using 'cons_tbl_list' (i.e., select clause and table clause)
    tmp_tuneprestmt = ''
    for tbl_ind, tbl_val in enumerate(cons_tbl_list):
        if (tbl_ind == 0):
            tmp_tuneprestmt = PRE_CNT_STMT + " " + dfjoincond.iloc[index].SchemaName + "." + tbl_val
        else:
            tmp_tuneprestmt = tmp_tuneprestmt + dfjoincond.iloc[index].SchemaName + "." + tbl_val

        if ((tbl_ind+1) < len(cons_tbl_list)):
            tmp_tuneprestmt = tmp_tuneprestmt + ','
        elif ((tbl_ind+1) == len(cons_tbl_list)):
            tmp_tuneprestmt = tmp_tuneprestmt + " WHERE "
    
    return tmp_tuneprestmt

#to construct the previous statement to add conditions
def gettempprestmtloop2(dfjoincond,index,cond_list,tmp_tuneprestmt):
    #add condition to previous string (i..e, where condition clause)
    for cond_ind, cond_val in enumerate(cond_list):
        #for first condition add condition alone, don't add junction operator
        if (cond_ind == 0):
            tmp_tuneprestmt = tmp_tuneprestmt + cond_val
        else:
            #to construct the previous statement next conditions
            tmp_tuneprestmt = gettempprestmtloop3(dfjoincond,cond_val,tmp_tuneprestmt)
    return tmp_tuneprestmt

#to construct the previous statement next conditions
def gettempprestmtloop3(dfjoincond,cond_val,tmp_tuneprestmt):
    for jn_index,row in dfjoincond.iterrows():
        if (dfjoincond.iloc[jn_index].Condition == cond_val):
            tmp_tuneprestmt = tmp_tuneprestmt + " " + dfjoincond.iloc[jn_index].PreString + " " + cond_val
            break
    return tmp_tuneprestmt

#to get the identified query statement for neoldw table loop
def processoldtableloop(dfjoincond,index,vtuneprestmt,cond_list):
    #if no new table exist, then use the existing 'vtuneprestmt' itself
    #set the derived previous string alone till "WHERE" word
    tmp_tuneprestmt = vtuneprestmt
    #add condition to previous string (i..e, where condition clause)
    tmp_tuneprestmt = gettempprestmtloop2(dfjoincond,index,cond_list,tmp_tuneprestmt)

    #add the new condition (i..e, current processing condition)
    tmp_tuneprestmt = tmp_tuneprestmt + " " + dfjoincond.iloc[index].PreString + " " + dfjoincond.iloc[index].Condition

    exec_tuneprestmt = tmp_tuneprestmt
    #print('exec_tuneprestmt: ',exec_tuneprestmt)
    return ({'tmp_tuneprestmt': tmp_tuneprestmt,'exec_tuneprestmt': exec_tuneprestmt})

#to check and execute the query and update the join condition index
def executetunequery(dfjoincond,index,gbltunejson):
    #get and initialize the values
    exec_tuneprestmt = gbltunejson['exec_tuneprestmt']
    der_tuneprestmt = gbltunejson['der_tuneprestmt']
    vtuneprestmt = gbltunejson['vtuneprestmt']
    cond_list = gbltunejson['cond_list']
    tbl_list = gbltunejson['tbl_list']
    der_tbl_list = gbltunejson['der_tbl_list']
    cons_tune_qry = gbltunejson['cons_tune_qry']

    if (exec_tuneprestmt != ''):
        vschemaname = dfjoincond.iloc[index].SchemaName                    
        try:
            fnl_exec_tuneprestmt = exec_tuneprestmt + " WITH UR"
            vdatareturnedqryobj = ConnectDatabase(fnl_exec_tuneprestmt,vschemaname)

            #using request method to connect the db and get the data
            vdatareturnedqryrslt = vdatareturnedqryobj.executequery()

            #using packages to connect the db and get the data
            #qryrslt = vdatareturnedqryobj.executequery()
            #vdatareturnedqryrslt = checkqueryresult(qryrslt)

            vjoinreturned = 'N'
            if (vdatareturnedqryrslt == 'VALID_QUERY'):
                vjoinreturned = 'Y'

            dfjoincond.iloc[index].JoinReturned = vjoinreturned

            #check and load the current table list into master table list
            #load the current condition into master condition list
            #append the previous string properly
            if (vjoinreturned == 'Y'):
                #append the previous string
                vtuneprestmt = der_tuneprestmt
                
                #add current condition into condition list
                cond_list.append(dfjoincond.iloc[index].Condition)

                #add current table to table list
                for der_tbl in der_tbl_list:
                    tbl_list.append(der_tbl)

                #re-initialize the final tune query
                cons_tune_qry = exec_tuneprestmt
        except:
            #print('###########Execute Query Failed############')
            dfjoincond.iloc[index].JoinReturned = 'N'
            #continue
    else:
        dfjoincond.iloc[index].JoinReturned = "N"
    return ({'dfjoincond': dfjoincond,'vtuneprestmt': vtuneprestmt,'cond_list': cond_list,'tbl_list': tbl_list,'cons_tune_qry': cons_tune_qry})
##########################Condition Logic - Ends here###########################
