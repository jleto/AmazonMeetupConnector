import sys, traceback
import os
from datetime import datetime
import logging
import json
import amazon
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

    try:

        amazonProvider = amazon.amazon(amazonAccessKey, amazonSecretKey)
        amazonProvider.getPayments(startDate, endDate)
        amazonProvider.write()

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(str(e).split("'")[1])
