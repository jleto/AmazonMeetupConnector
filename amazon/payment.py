import csv
from datetime import datetime
from datetime import timedelta
from novalabs import properties
from novalabs import log
from novalabs import db
from boto.fps.connection import FPSConnection

def _sqlLoadPayments(jobId, strCSVFilePath):
    return "do $$ begin perform etl.fn_amazon_payment_importcsv("+str(jobId)+", '"+strCSVFilePath+"'); end$$;"

def get(amazonProvider):

    try:
        options = {}
        options['host'] = 'fps.amazonaws.com'
        boto_aws = FPSConnection(amazonProvider.getAccessKey(), amazonProvider.getSecretKey(), **options)

        options = {}
        options['StartDate'] = amazonProvider.getStartDate().strftime("%Y-%m-%d")
        options['EndDate'] = amazonProvider.getEndDate().strftime("%Y-%m-%d")

        obj = boto_aws.get_account_activity(**options)
        result = obj.GetAccountActivityResult
        amazonProvider._payments = result.Transaction

        return result.Transaction

    except Exception, e:
        log.writeError(e)
        return None

def write(amazonProvider, strCSVFileName):
 
    if amazonProvider.getPaymentResults() == None:
        log.writeInfo('[AMAZON] There are no payments.')        
        return False

    try:
        writer = csv.writer(open(strCSVFileName, 'w'), quoting=csv.QUOTE_ALL)
        writer.writerow(["key","datetime","fps_operation", "sender_key", "sender_name", "description", "fees", "status", "amount"])

        for transaction in amazonProvider.getPaymentResults():
            row = list()
            row.append(str(transaction.TransactionId))
            try:
                row.append(str(transaction.DateCompleted))
            except:
                row.append("")
            try:
                row.append(str(transaction.FPSOperation))
            except:
                row.append("")
            try:
                row.append(str(transaction.SenderTokenId))
            except:
                row.append("")
            try:
                row.append(transaction.SenderName.encode("ascii", "ignore"))
            except:
                row.append("")
            if "Description:" in str(transaction.TransactionPart[0]).split('(')[1].split(',')[0]:
                row.append(str(transaction.TransactionPart[0]).split('(')[1].split(',')[0].split("'")[1])
            else:
                row.append("")

            try:
                row.append(str(transaction.FPSFees))
            except:
                row.append("")
            try:
                row.append(str(transaction.TransactionStatus))
            except:
                row.append("")
            try:
                row.append(str(transaction.TransactionAmount))
            except:
                row.append("")

            writer.writerow(row)
            return True
        
    except Exception, e:
        log.writeError(e)
        return False

def load(jobId, strCSVFilePath):
    try:
        db.execute(_sqlLoadPayments(jobId, strCSVFilePath))
        return True
    except Exception, e:
        log.writeError(e)
        return False
