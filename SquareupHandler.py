import csv
from datetime import timedelta
import datetime
from novalabs import properties
from novalabs.etl import jobProcessor
from squareup import squareup
from novalabs import log

def dtDate(batch_key):
    return datetime.datetime.strptime(batch_key, '%Y-%m-%d')

def strDate(batch_key):
    return str(datetime.datetime.strptime(batch_key, '%Y-%m-%d')).split(" ")[0]

def skip(job):
    jobProcessor.complete(job['job_id']);
    log.writeInfo('[SQUARE UP] No transactions for JobId: ('+str(job['job_id'])+') | BatchId: ('+str(job['batch_id'])+') | Date: ('+strDate(job['batch_key'])+'). Skipping and marking job complete.')

def process(job, squareupProperties):
    squareupProvider = squareup.squareup(squareupProperties['PERSONAL_ACCESS_TOKEN'])
    squareupProvider.getPayments(dtDate(job['batch_key'])-timedelta(days=1), dtDate(job['batch_key']))
    if squareupProvider.getTransactionCount() > 0:
        try:
            path = squareupProperties['datafile_path']
            strCSVFilePath = path + "/squareup-payment-" + strDate(job['batch_key']) + ".csv"
            log.writeInfo(' [SQUARE UP] Writing '+strCSVFilePath+' to disk.')
            payment.write(squareupProvider, strCSVFilePath)
            payment.load(str(job['job_id']), strCSVFilePath)
            #logging.info('['+getTimeStamp()+'] [AMAZON] Transactions processed: ('+str(amazonProvider.getTransactionCount())+') | JobId: ('+job['job_id']+') | BatchId: ('+job['batch_id']+') | Date: ('+strDate+').')

        except Exception, e:
            log.writeError(e)

    else:
        skip(job)
