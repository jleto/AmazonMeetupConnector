import sys, traceback
import os
from datetime import datetime
import logging
import json
import csv
from boto.fps.connection import FPSConnection
from pprint import pprint

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
            options['StartDate'] = startDate.strftime("%Y-%m-%d")
            if endDate != None:
                endDate.strftime("%Y-%m-%d")

            self.setStartDate(startDate)
            self.setEndDate(endDate)

            obj = boto_aws.get_account_activity(**options)
            result = obj.GetAccountActivityResult
            self.__payments = result.Transaction

        except Exception, Argument:
            print "Boto FPS Amazon Activity failed with Error: %s", Argument


    def write(self, path):

        if self.__payments == None:
            print "There are no payments."
            return None
        
        writer = csv.writer(open(path+"/amazon-"+str(self.getEndDate()).split(" ")[0].replace('/','-')+".csv", 'w'), quoting=csv.QUOTE_ALL)
        writer.writerow(["key","datetime","sender_key", "sender_name", "description", "fees", "status", "amount"])
        
        for transaction in self.__payments:
            row = list()
            row.append(transaction.TransactionId)
            row.append(transaction.DateCompleted)
            row.append(transaction.SenderTokenId)
            row.append(transaction.SenderName)
            if "Description:" in str(transaction.TransactionPart[0]).split('(')[1].split(',')[0]:
                row.append(str(transaction.TransactionPart[0]).split('(')[1].split(',')[0].split("'")[1])
            else:
                row.append("")
            row.append(transaction.FPSFees)
            row.append(transaction.TransactionStatus)
            row.append(transaction.TransactionAmount)
            writer.writerow(row)
