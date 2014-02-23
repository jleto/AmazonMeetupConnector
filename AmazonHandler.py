import csv
from datetime import timedelta
import datetime
from novalabs import properties
from novalabs.etl import jobProcessor
from amazon import amazon
from amazon import payment
from novalabs import log

def dtDate(batch_key):
    return datetime.datetime.strptime(batch_key, '%Y-%m-%d')

def strDate(batch_key):
    return str(datetime.datetime.strptime(batch_key, '%Y-%m-%d')).split(" ")[0]

def skip(job):
    jobProcessor.complete(job['job_id']);
    log.writeInfo('[AMAZON] No transactions for JobId: ('+str(job['job_id'])+') | BatchId: ('+str(job['batch_id'])+') | Date: ('+strDate(job['batch_key'])+'). Skipping and marking job complete.')

def process(job, amazonProperties):
    amazonProvider = amazon.amazon(amazonProperties['AWS_ACCESS_KEY'], amazonProperties['AWS_SECRET_KEY'])
    amazonProvider.getPayments(dtDate(job['batch_key'])-timedelta(days=1), dtDate(job['batch_key']))
    if amazonProvider.getTransactionCount() > 0:
        try:
            path = amazonProperties['datafile_path']
            strCSVFilePath = path + "/amazon-payment-" + strDate(job['batch_key']) + ".csv"
            log.writeInfo(' [AMAZON] Writing '+strCSVFilePath+' to disk.')
            payment.write(amazonProvider, strCSVFilePath)
            payment.load(str(job['job_id']), strCSVFilePath)
            #logging.info('['+getTimeStamp()+'] [AMAZON] Transactions processed: ('+str(amazonProvider.getTransactionCount())+') | JobId: ('+job['job_id']+') | BatchId: ('+job['batch_id']+') | Date: ('+strDate+').')

        except Exception, e:
            log.writeError(e)

    else:
        skip(job)
