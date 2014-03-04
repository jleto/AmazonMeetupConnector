from datetime import datetime
from novalabs import log
from novalabs import db
import urllib2
import pycurl
import json
import csv
from io import BytesIO as StringIO
from pprint import pprint

class squareup:

    _personalAccessToken = None

    _payments = None

    def __init__(self, personalAccessToken):
        self._personalAccessToken = personalAccessToken

    def getPersonalAccessToken(self):
        return self._personalAccessToken

    def sqlLoadPayments(self, jobId, strCSVFilePath):
        return "do $$ begin perform etl.fn_squareup_payment_importcsv("+str(jobId)+", '"+strCSVFilePath+"'); end$$;"

    def getPaymentData(self, startDate):
	    try:
                result = StringIO()
                c = pycurl.Curl()
                URL = 'https://connect.squareup.com/v1/me/payments?begin_time='+startDate+'T00:00:00Z&end_time='+startDate+'T23:59:59Z'
                c.setopt(pycurl.URL, URL)
                c.setopt(pycurl.HTTPHEADER, ['Authorization: Bearer '+self.getPersonalAccessToken()+''])
                c.setopt(pycurl.WRITEFUNCTION, result.write)
                c.perform()                       
                resultDict = {}
                resultDict = result.getvalue() 
                self._payments = json.loads(resultDict)
	    except Exception, e:
                log.writeError(e)

    def getPayments(self):
        return self._payments

    def getPaymentCount(self):
        if self._payments == None:
            return 0
        else:
            return len(self._payments)


    def writePayments(self, strCSVFilePath):       
        if self.getPayments() == None:
           return None
        try:
            writer = csv.writer(open(strCSVFilePath, 'w'), quoting=csv.QUOTE_ALL)
            writer.writerow(["key", "datetime", "description", "amount", "fee"])
            payments = self.getPayments()
            for transaction in payments:
                row = list()
                try:
                    row.append(str(transaction['id']))
                except:
                    row.append("")
                try:
                    row.append(str(transaction['created_at']))
                except:
                    row.append("")
                try:
                    row.append(str(transaction['description']))
                except:
                    row.append("")
                try:
                    row.append(str(transaction['net_total_money']['amount']))
                except:
                    row.append("")
                try:
                    row.append(str(transaction['processing_fee_money']['amount']).replace('-', ''))
                except:
                    row.append("")
                writer.writerow(row)
        except Exception, e:
            log.writeError(e)
 
    def loadPayments(self, jobId, strCSVFilePath):
        try:
            result = db.execute(self.sqlLoadPayments(jobId, strCSVFilePath))
        except Exception, e:
            log.writeError(e)
 
