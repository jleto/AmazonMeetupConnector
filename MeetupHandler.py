import csv
from datetime import timedelta
import datetime
from novalabs import properties
from novalabs.etl import jobProcessor
from novalabs import log
from meetup import meetup

def dtDate(batch_key):
    return datetime.datetime.strptime(batch_key, '%Y-%m-%d')

def strDate(batch_key):
    return str(datetime.datetime.strptime(batch_key, '%Y-%m-%d')).split(" ")[0]

def skip(job):
    jobProcessor.complete(job['job_id']);
    log.writeInfo('[MEETUP] No transactions for JobId: ('+str(job['job_id'])+') | BatchId: ('+str(job['batch_id'])+') | Date: ('+strDate(job['batch_key'])+'). Skipping and marking job complete.')

def process(job, meetupProperties):
    
    try:
        meetupProvider = meetup.meetup(meetupProperties['apiKey'], meetupProperties['groupName'])
        meetupProvider.getPaymentData(dtDate(job['batch_key']), dtDate(job['batch_key']))
        if meetupProvider.getPaymentCount() > 0:            
            strCSVFilePath = meetupProperties['datafile_path'] + '/meetup-payment-' + strDate(job['batch_key']) + '.csv'        
            log.writeInfo(' [MEETUP] Writing '+strCSVFilePath+' to disk.')
            meetupProvider.writePayments(strCSVFilePath)
            meetupProvider.loadPayments(job['job_id'], strCSVFilePath)
            log.writeInfo(' [MEETUP] Transactions processed: ('+str(meetupProvider.getPaymentCount())+') | JobId: ('+job['job_id']+') | BatchId: ('+job['batch_id']+') | Date: ('+strDate(job['batch_key'])+').')
        else:
            skip(job)

    except Exception, e:
        log.writeError(e)
