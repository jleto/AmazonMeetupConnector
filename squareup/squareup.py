from datetime import datetime
from novalabs import log
from novalabs import db
import urllib2
import pycurl
import json
import csv

class squareup:

    _personalAccessToken = None

    _payments = None

    def __init__(self, personalAccessToken):
        self._personalAccessToken = personalAccessToken

    def getPersonalAccessToken(self):
        return self._personalAccessToken

    def sqlLoadPayments(self, jobId, strCSVFilePath):
        return "do $$ begin perform etl.fn_squareup_payment_importcsv("+str(jobId)+", '"+strCSVFilePath+"'); end$$;"

    @staticmethod
    def getTimeStamp(Date):
        try:
            if type(Date) is datetime:
                return str(int((datetime(Date.year, Date.month, Date.day) - \
                                           datetime(1970, 1,1)).total_seconds()))+'000' 
            else:
                return '0'

        except Exception, e:            
            log.writeError(e)
 
    @staticmethod
    def getDate(Date):
        try:
            if type(Date) is datetime:
                return Date
            else:
                return datetime.strptime(str(Date), '%Y-%m-%d')
        except Exception, e:
            log.writeError(e)

    def getPaymentData(self, startDate, endDate):
	    try:
			c = pycurl.Curl()
			c.setopt(pycurl.URL, 'https://connect.squareup.com/v1/me/payments')
			c.setopt(pycurl.HTTPHEADER, ['Authorization: Bearer '+self.getPersonalAccessToken()]
			c.setopt(pycurl.VERBOSE, 0)
			print c.getinfo(pycurl.HTTP_CODE)
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
            writer.writerow(["key", "datetime", "event_id", "event_name", "member_id", "member_name", "amount"])
            payments = self.getPayments()
            for transaction in payments:
                row = list()
                row.append(str(transaction)+str(payments[transaction]['event_id'])+str(payments[transaction]['member_id']))
                row.append(str(payments[transaction]['date']))
                row.append(str(payments[transaction]['event_id']))
                row.append(str(payments[transaction]['event_name']))
                row.append(str(payments[transaction]['member_id']))
                row.append(str(payments[transaction]['member_name']))
                row.append(str(payments[transaction]['amount']))
                writer.writerow(row)
        except Exception, e:
            log.writeError(e)
 
    def loadPayments(self, jobId, strCSVFilePath):
        try:
            result = db.execute(self.sqlLoadPayments(jobId, strCSVFilePath))
        except Exception, e:
            log.writeError(e)
 
