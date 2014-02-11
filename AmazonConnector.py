import sys, traceback
import os
from datetime import datetime
import logging
import json
import csv
from boto.fps.connection import FPSConnection
from pprint import pprint

amazonProperties = {}
with open('amazon.properties', 'r') as f:
    for line in f:

        #removes trailing whitespace and '\n' chars
        line = line.rstrip()

        #skips blanks and comments w/ '='
        if "=" not in line:
            continue

        #skips comments that contain '#'
        if line.startswith("#"):
            continue

        k, v = line.split("=", 1)
        amazonProperties[k] = v

#   If only utility script is called
if len(sys.argv) <= 1:
    sys.exit("Usage: python %s [OPTION] [PARAMETERS]\n"
             "Where possible options include:\n"
             "   --payments                        Collect amazon payment information.\n"
             "   --help                         Help for using this tool.\n"
             "Where possible optional parameters include:\n"
             "   --start-date=STARTDATE         Start date of the search. If omitted, search will start at the first transaction.\n"
             "   --end-date=ENDDATE             End end of the search. If ommitted, search will span to the most current transaction.\n"
			 "   --amazon-accesskey=ACCESSKEY   Amazon Access Key.\n"
             "   --amazon-secretkey=SECRETKEY   Amazon Secret Key.\n"
             "   --output-format=OUTPUTFORMAT   Format to Output Data.\n"
			 )

#   If help is requested
elif (sys.argv[1] == '--help'):
    sys.exit("Help for %s not yet implemented." % sys.argv[0])

#   Test for valid options, instantiate provider objects
if sys.argv[1] == '--payments':

    amazonAccessKey = amazonProperties['AWS_ACCESS_KEY']
    amazonSecretKey = amazonProperties['AWS_SECRET_KEY']

    startDate = None
    endDate = None
    
    #   If user passes parameters in as arguments, override properties file
    for arg in sys.argv[2:]:
        if arg.split("=",1)[0] == "--start-date":
            startDate = datetime.strptime(arg.split("=",1)[1], '%m/%d/%Y')
        elif arg.split("=",1)[0] == "--end-date":
            endDate = datetime.strptime(arg.split("=",1)[1], '%m/%d/%Y')
        elif arg.split("=",1)[0] == "--amazon-accesskey":
            amazonApiKey = arg.split("=",1)[1]
        elif arg.split("=",1)[0] == "--amazon-secretkey":
             amazonSecretKey = arg.split("=",1)[1]
        elif arg.split("=",1)[0] == "--output-format":
            outputFormat = arg.split("=",1)[1]
        elif arg.split("=",1)[0] == "--output-filename":
            outputFileName = arg.split("=",1)[1]

    if startDate == None:
        raise "StartDate was not specified.", Exception
    if endDate == None:
        endDate = datetime.strptime(Now(), '%m-%d-%Y')

    #   Instantiate amazon object using boto library
  
    try:
        options = {}
        options['host'] = 'fps.amazonaws.com'
        boto_aws = FPSConnection(amazonAccessKey, amazonSecretKey, **options)

        options = {}
        options['StartDate'] = startDate.strftime("%Y-%m-%d")
        if endDate != None:
            endDate.strftime("%Y-%m-%d")

        obj = boto_aws.get_account_activity(**options)
        result = obj.GetAccountActivityResult
        payments = result.Transaction

        if outputFormat == "CSV":
            writer = csv.writer(open("amazon-"+str(endDate).split(" ")[0].replace('/','-')+".csv", 'w'), quoting=csv.QUOTE_ALL)
            writer.writerow(["key","datetime","sender_key", "sender_name", "description", "fees", "status", "amount"])
            for transaction in payments:
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

    except Exception, Argument:
        print "Boto FPS Amazon Activity failed with Error: %s", Argument

