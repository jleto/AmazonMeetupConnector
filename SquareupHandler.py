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
    log.writeInfo('[SQUARE UP] No transactions for JobId: ('+str(job['job_id'])+') | BatchId: ('+str(job['batch_id'])+') | Date: ('+strDate(job['batch_key'])+'). Skipping.')

def process(job, squareupProperties):
    squareupProvider = squareup.squareup(squareupProperties['PERSONAL_ACCESS_TOKEN'])
    squareupProvider.getPaymentData(strDate(job['batch_key']))
    if squareupProvider.getPaymentCount() > 0:
        try:
            path = squareupProperties['datafile_path']
            strCSVFilePath = path + "/squareup-payment-" + strDate(job['batch_key']) + ".csv"
            log.writeInfo('[SQUARE UP] Writing '+strCSVFilePath+' to disk.')
            squareupProvider.writePayments(strCSVFilePath)
            squareupProvider.loadPayments(str(job['job_id']), strCSVFilePath)
            log.writeInfo('[SQUARE UP] Transactions processed: ('+str(squareupProvider.getPaymentCount())+') | JobId: ('+str(job['job_id'])+') | BatchId: ('+str(job['batch_id'])+') | Date: ('+strDate(job['batch_key'])+').')

        except Exception, e:
            log.writeError(e)

    else:
        skip(job)
