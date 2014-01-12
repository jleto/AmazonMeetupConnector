import sys, traceback
import os
from datetime import datetime
import logging
import meetup
from pprint import pprint

meetupProperties = {}
with open('meetup.properties', 'r') as f:
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
        meetupProperties[k] = v

#   If only utility script is called
if len(sys.argv) <= 1:
    sys.exit("Usage: python %s [OPTION] [PARAMETERS]\n"
             "Where possible options include:\n"
             "   --merge                        Merge amazon and meetup payment information.\n"
             "   --help                         Help for using this tool.\n"
             "Where possible optional parameters include:\n"
             "   --start-date=STARTDATE         Start date of the search. If omitted, search will start at the first transaction.\n"
             "   --end-date=ENDDATE             End end of the search. If ommitted, search will span to the most current transaction.\n"
			 "   --meetup-groupname=GROUPNAME   Name of the Meetup Group to query.\n"
			 "   --meetup-apikey=APIKEY         Meetup Group Api Key.\n"
			 )

#   If help is requested
elif (sys.argv[1] == '--help'):
    sys.exit("Help for %s not yet implemented." % sys.argv[0])

#   Test for valid options, instantiate provider objects
if sys.argv[1] == '--merge':

    meetupGroupName = meetupProperties['groupName']
    meetupApiKey = meetupProperties['apiKey']
    startDate = None
    endDate = None
    
    #   If user passes parameters in as arguments, override properties file
    for arg in sys.argv[2:]:
        if arg.split("=",1)[0] == "--start-date":
            startDate = datetime.strptime(arg.split("=",1)[1], '%m/%d/%Y')
        elif arg.split("=",1)[0] == "--end-date":
            endDate = datetime.strptime(arg.split("=",1)[1], '%m/%d/%Y')
        elif arg.split("=",1)[0] == "--meetup-groupname":
            meetupGroupName = arg.split("=",1)[1]
        elif arg.split("=",1)[0] == "--meetup-apikey":
            meetupApiKey = arg.split("=",1)[1]

    #   Instantiate meetup object with group name and api key
    meetupProvider = meetup.meetup(meetupGroupName, meetupApiKey)

	#   Get payment transactions from meetup for the defined time period
    payments = meetupProvider.getPayments(startDate, endDate)

    pprint (payments) 

