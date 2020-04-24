import json
import re

#to get the order and group position from SQL statement
def get_ord_grp_pos(sqlstmntsplt):
    iswhereposexist = False
    orderpos = 0
    grouppos = 0

    for i in range(len(sqlstmntsplt)):
        #print(str(i) + " " + sqlstmntsplt[i])
        if sqlstmntsplt[i] == 'WHERE':
            iswhereposexist = True

        if iswhereposexist:
            if (sqlstmntsplt[i] == 'ORDER'):
                orderpos = i
            if (sqlstmntsplt[i] == 'GROUP'):
                grouppos = i
    return ({'orderpos': orderpos,'grouppos': grouppos,'iswhereposexist': iswhereposexist})

#process and get the order/group position which is lesser value from starting point of sql statement
def process_ord_grp_pos(orderpos,grouppos):
    ord_grp_pos = 0
    if (orderpos > 0 and grouppos > 0):
        if (orderpos < grouppos):
            ord_grp_pos = orderpos
        else:
            ord_grp_pos = grouppos
    elif (orderpos > 0):
        ord_grp_pos = orderpos
    else:
        ord_grp_pos = grouppos
    return ord_grp_pos

class ParseQuery(object):

    def __init__(self,query):
        self.query = query

    #filter out the order/group by clause from the query
    def filterquery(self):
        #to find the order or group clause exist in the sql statement.
        #sqlStatement = self.query

        #remove the left/right side - double/single quotes
        sqlstatement = self.query.lstrip('"')
        sqlstatement = sqlstatement.rstrip('"')
        sqlstatement = sqlstatement.rstrip('\'')
        sqlstatement = sqlstatement.rstrip('\'')

        iswhereposexist = False
        orderpos = 0
        grouppos = 0
        fltordgrpstmt = ''
        fltsqlstmnt = sqlstatement
        ##Split Query
        sqlstmntsplt =  sqlstatement.upper().split(" ")

        #get the order group position
        ord_grp_json = get_ord_grp_pos(sqlstmntsplt)
        orderpos = ord_grp_json['orderpos']
        grouppos = ord_grp_json['grouppos']
        iswhereposexist = ord_grp_json['iswhereposexist']

        #print the orderpos and grouppos
        #print ('orderpos: ',orderpos)
        #print ('grouppos: ',grouppos)

        if (orderpos > 0 or grouppos > 0):
            #process the order group position
            ord_grp_pos = process_ord_grp_pos(orderpos,grouppos)
            #print ('ord_grp_pos: ',ord_grp_pos)

            ord_grp_words = []
            if (ord_grp_pos > 0):
                for i in range(len(sqlstmntsplt)):
                    #print(str(i) + " " + sqlstmntsplt[i])
                    if i >= ord_grp_pos:
                        ord_grp_words.append(sqlstmntsplt[i])

            #print ('ord_grp_words: ',ord_grp_words)
            fltordgrpstmt = str(" ".join(ord_grp_words))
            #print ('fltordgrpstmt: ',fltordgrpstmt)
            fltsqlstmnt = sqlstatement.upper().replace(fltordgrpstmt, "").strip(' ')

        #print the fltsqlstmnt and sqlstmntsplt
        #print ('fltsqlstmnt: ',fltsqlstmnt)
        #print ('sqlstmntsplt: ',sqlstmntsplt)

        output_json = {}
        output_json['fltsqlstmnt'] = str(fltsqlstmnt)
        output_json['fltordgrpstmt'] = str(fltordgrpstmt)
        output_json['iswhereposexist'] = iswhereposexist

        return(output_json)
