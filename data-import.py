#!/usr/bin/python
import time
from novalabs.etl import jobProcessor
from novalabs import log
from novalabs import properties
import datetime
import logging
import AmazonHandler
import MeetupHandler

def main():
   
    logging.basicConfig(filename='/home/jleto/AmazonMeetupConnector/log/novalabs-data-import.log', level=logging.INFO)
    projectProperties = properties.get('data-import.properties')
    meetupProperties = properties.get('meetup.properties')
    amazonProperties = properties.get('amazon.properties')
    try:
       log.writeInfo(' [GENERAL] Generating batches.')
       result = jobProcessor.generate()
    except Exception, e:
       log.writeError(e)

    job_cursor = jobProcessor.getReadyJobs()
    log.writeInfo(' [GENERAL] Jobs to process: ('+str(job_cursor.rowcount)+').')
    
    for job in job_cursor:
        time.sleep(1)
        jobDict = {}
        jobDict['job_id'] = str(job[0])
        jobDict['batch_id'] = str(job[1])
        jobDict['batch_key'] = str(job[2])
        jobDict['product_key'] = str(job[3])

        if jobDict['product_key'] == 'meetup_payments':
            MeetupHandler.process(jobDict, meetupProperties)
        elif jobDict['product_key'] == 'amazon_payments':
            AmazonHandler.process(jobDict, amazonProperties)
        elif jobDict['product_key'] == 'squareup_payments':
		    SquareupHander.process(jobDict, squareupProperties)

   # logging.info('['+getTimeStamp()+'] [FINISH] Data import processing completed.')
    log.writeInfo('[FINISH] Data import processing completed.')

if __name__ == "__main__":
    main()
