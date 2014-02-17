import sys, traceback
import os
from datetime import datetime
import logging
import json
import csv
from boto.fps.connection import FPSConnection

class amazon:

    __AWSAccessKey = None
    __AWSSecretKey = None

    __payments = None
    __startDate = None
    __endDate = None

    def __init__(self, accessKey, secretKey):
	    
        self.__AWSAccessKey = accessKey
        self.__AWSSecretKey = secretKey

    def getAccessKey(self):
        return self.__AWSAccessKey

    def getSecretKey(self):
        return self.__AWSSecretKey

    def getStartDate(self):
        return self.__startDate

    def getEndDate(self):
        return self.__endDate

    def setStartDate(self, startDate):
        self.__startDate = startDate

    def setEndDate(self, endDate):
        self.__endDate = endDate

    def getTransactionCount(self):
        return len(self.__payments)

    def getPayments(self, startDate, endDate):

        #   Instantiate amazon object using boto library
  
        try:
            options = {}
            options['host'] = 'fps.amazonaws.com'
            boto_aws = FPSConnection(self.getAccessKey(), self.getSecretKey(), **options)

            options = {}
            if endDate != None:
                endDate.strftime("%Y-%m-%d")
            else:
                endDate = startDate
 
            options['StartDate'] = startDate.strftime("%Y-%m-%d")
            options['EndDate'] = endDate.strftime("%Y-%m-%d")

            self.setStartDate(startDate)
            self.setEndDate(endDate)

            obj = boto_aws.get_account_activity(**options)
            result = obj.GetAccountActivityResult
            self.__payments = result.Transaction

        except Exception, e:
            e = sys.exc_info()[0]
            logging.info('['+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] [ERROR] Boto FPS Amazon Activity failed with Error: %s', e)


    def write(self, path):

        if self.__payments == None:
            logging.info('['+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] [AMAZON] There are no payments.')
            return None
        
        try:
            CSVFileName = path+"/amazon-"+self.getEndDate().strftime('%Y-%m-%d').split(" ")[0].replace('/','-')+".csv"
            writer = csv.writer(open(CSVFileName, 'w'), quoting=csv.QUOTE_ALL)
            writer.writerow(["key","datetime","fps_operation", "sender_key", "sender_name", "description", "fees", "status", "amount"])
        
            for transaction in self.__payments:
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

        except Exception, e:
            e = sys.exc_info()[0]
            logging.info('['+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] [ERROR] %s.', e)
