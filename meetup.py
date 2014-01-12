import datetime
import urllib2
import json
from pprint import pprint

class meetup:

    __apiKey = None
    __groupName = None

    def __init__(self, groupName, apiKey):

        self.__groupName = groupName
        self.__apiKey = apiKey

    def getApiKey(self):
        return self.__apiKey

    def getGroupName(self):
        return self.__groupName

    def getPayments(self,  startDate, endDate):

        try:
            __startDate = str(int((datetime.datetime(startDate.year, startDate.month, startDate.day) - \
		                       datetime.datetime(1970, 1,1)).total_seconds()))+'000'
        except:
            __endDate = None
            pass

        try:
            __endDate = str(int((datetime.datetime(endDate.year, endDate.month, endDate.day) - \
		                     datetime.datetime(1970,1,1)).total_seconds()))+'000'
        except:
            __endDate = None
            pass

        #   Construct url
        events_url = 'http://api.meetup.com/2/events?key='+self.getApiKey()
        
        if __startDate is not None and __endDate is not None:
            events_url = events_url+'&time='+__startDate+','+__endDate
        elif __startDate is None and __endDate is not None:
            events_url = events_url+'&time=,'+__endDate
        elif __startDate is not None and __endDate is None:
            events_url = events_url+'&time='+__startDate+','  
			
        events_url = events_url+'&group_urlname='+self.getGroupName()+'&fields=fee&status=upcoming,past'

        payments = {}
        try:
            events_result = urllib2.urlopen(events_url)
            events_data = json.load(events_result)
            for event in events_data['results']:
                if 'fee' in event:
                    
                    rsvps_url='http://api.meetup.com/2/rsvps?key='+self.getApiKey()+'&event_id='+str(event['id'])+'&fields=pay_status'
                    rsvps_result = urllib2.urlopen(rsvps_url)
                    rsvps_data = json.load(rsvps_result)
                    for rsvp in rsvps_data['results']:
                        if rsvp['pay_status'] in ('paid','refunded'):
                           payments[str(datetime.datetime.fromtimestamp(int(rsvp["mtime"])/1000).strftime('%Y-%m-%d %H:%M:%S'))] = {'member_id': rsvp["member"]["member_id"], 'member_name':rsvp["member"]["name"], 'event_id':rsvp["event"]["id"], 'event_name':rsvp["event"]["name"], 'date': datetime.datetime.fromtimestamp(int(rsvp["mtime"])/1000).strftime('%Y-%m-%d %H:%M:%S'), 'pay_status': rsvp["pay_status"], 'amount':event["fee"]["amount"]}

            return payments

        except urllib2.HTTPError, e:
            try:
		        dom = parse(e)
            finally:
		        e.close()
            err = dom['Response']['Errors']['Error']
            raise Exception(err['Code'], err['Message'])

