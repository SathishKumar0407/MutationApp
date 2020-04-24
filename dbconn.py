import json
import re
import ibm_db

from constants import DB2_CS_CONN, DB2_CR_CONN

class ConnectDatabase:
    def __init__(self,query,schemaname):
        self.query = query
        self.schemaname = schemaname

    def executequery(self):
        output_json = {}
        output_json['data'] = ""
        output_json['error'] = ""

        conn_str = ''
        if (self.schemaname == 'CSADM'):
            conn_str = DB2_CS_CONN
        else:
            conn_str = DB2_CR_CONN

        #by default
        #conn_str='database=FPLDB2T;hostname=sc01.fpl.com;port=1022;protocol=TCPIP;uid=ZZZ0ADE;pwd=fams9786'

        ibm_db_conn = ibm_db.connect(conn_str,'','')

        # Fetch data using ibm_db_dbi
        select = self.query
        qryrslt = []
        try:
            stmt = ibm_db.exec_immediate(ibm_db_conn,select)
            #print ('stmt:',stmt)
            #onerow = ibm_db.fetch_assoc(stmt)
            #print (onerow)
            #for element in onerow:
                #print(element)

            row = ibm_db.fetch_assoc(stmt)

            while row != False :
                #for each row
                #print ("The value returned : ", row)
                qryrslt.append(row)
                row = ibm_db.fetch_assoc(stmt)
        except:
            #for connection issue
            #print("INVALID-QUERY")
            qryrslt = []

        return qryrslt
