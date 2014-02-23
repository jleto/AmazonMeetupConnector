from datetime import datetime
from novalabs import log
from novalabs import db
import urllib2
import json
import csv

class meetup:

    _apiKey = None
    _groupName = None

    _payments = None
    _events = None
    _rsvps = None

    def __init__(self, apiKey, groupName):
        self._apiKey = apiKey
        self._groupName = groupName

    def getApiKey(self):
        return self._apiKey

    def getGroupName(self):
        return self._groupName

    def sqlLoadPayments(self, jobId, strCSVFilePath):
        return "do $$ begin perform etl.fn_meetup_payment_importcsv("+str(jobId)+", '"+strCSVFilePath+"'); end$$;"

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

    def getEventData(self, startDate, endDate):
        try:
            eventURL = 'http://api.meetup.com/2/events?key='+self.getApiKey()+'&time='+self.getTimeStamp(startDate)+','+self.getTimeStamp(endDate)+'&group_urlname='+self.getGroupName()+'&fields=fee&status=upcoming,past'

            self._events = json.load(urllib2.urlopen(eventURL))['results']
        except Exception, e:
            log.writeError(e)

    def getEvents(self):
       return self._events

    def getRsvpData(self, strEventId):
        try:
            rsvpURL='http://api.meetup.com/2/rsvps?key='+self.getApiKey()+'&event_id='+strEventId+'&fields=pay_status'
            self._rsvps = json.load(urllib2.urlopen(rsvpURL))['results']
        except Exception, e:
            log.writeError(e)

    def getRsvps(self):
        return self._rsvps

    def getPaymentData(self, startDate, endDate):
        try:
            payments = {}
            self.getEventData(startDate, endDate)
            for event in self.getEvents():
                if 'fee' in event:
                    self.getRsvpData(str(event['id']))
                    rsvps = self.getRsvps()
                    for rsvp in rsvps:
                        if rsvp['pay_status'] in ('paid','refunded'):
                            payments[str(datetime.fromtimestamp(int(rsvp["mtime"])/1000).strftime('%Y%m%d%H%M%S'))] = {'member_id': rsvp["member"]["member_id"], 'member_name':rsvp["member"]["name"], 'event_id':rsvp["event"]["id"], 'event_name':rsvp["event"]["name"], 'date': datetime.fromtimestamp(int(rsvp["mtime"])/1000).strftime('%Y-%m-%d %H:%M:%S'), 'pay_status': rsvp["pay_status"], 'amount':event["fee"]["amount"]}

            self._payments = payments

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
 
