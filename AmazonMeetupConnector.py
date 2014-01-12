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
    sys.exit("Usage: python %s <options> <csvfiles>\n"
             "Where possible options include:\n"
             "   --merge            Merge amazon and meetup payment information.\n"
	         "   --help             Help for using this tool." % sys.argv[0]
			 )

#   If help is requested
elif (sys.argv[1] == '--help'):
    sys.exit("Help for %s not yet implemented." % sys.argv[0])

#   Test for valid options, instantiate provider objects
if sys.argv[1] == '--merge':
    meetupProvider = meetup.meetup(meetupProperties['groupName'], meetupProperties['apiKey'])
    startDate = datetime.strptime(sys.argv[2], '%m/%d/%Y')
    endDate = datetime.strptime(sys.argv[3], '%m/%d/%Y')
    payments = meetupProvider.getPayments(startDate, endDate)
    pprint (payments) 

