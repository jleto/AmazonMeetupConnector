#!/usr/bin/python
import psycopg2
import sys
import datetime
from datetime import timedelta
import meetup
import amazon
import logging
import time

def main():
   
    logging.basicConfig(filename='/home/jleto/AmazonMeetupConnector/log/novalabs-data-import.log', level=logging.INFO)
 
    def getTimeStamp():
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
 
    # Read property files
    def getProperties(strPropertiesFilePath):
        
        with open(strPropertiesFilePath, 'r') as f:
            propertiesDict = {}
            for line in f:
                #remove whitespace and \n chars
                line = line.rstrip()

                #skips blands and comments w/ '='
                if "=" not in line:
                   continue

                if line.startswith("#"):
                    continue

                k, v = line.split("=", 1)
                propertiesDict[k] = v
        return propertiesDict

    def skipRow(row):
        skip_conn = psycopg2.connect(conn_string)
        skip_cursor = skip_conn.cursor()
        skip_cursor.execute("do $$ " \
                            "begin " \
                            "   update etl.job " \
                            "   set status = 'completed' " \
                            "   where id = " + str(row[0]) + "; " \
                            "end $$; ")
        skip_conn.commit()
        skip_conn = None
        if row[3] == 'meetup_payments':
            logging.info('['+getTimeStamp()+'] [MEETUP] No transactions for JobId: ('+str(row[0])+') | BatchId: ('+str(row[1])+') | Date: ('+strBatchDate+'). Skipping and marking job complete.')
        elif row[3] == 'amazon_payments':
            logging.info('['+getTimeStamp()+'] [AMAZON] No transactions for JobId: ('+str(row[0])+') | BatchId: ('+str(row[1])+') | Date: ('+strBatchDate+'). Skipping and marking job complete.')
            

    def reportError(e, row):
           e = sys.exc_info()[0]
           logging.info('['+getTimeStamp()+'] [ERROR] %s. Line ('+str(sys.exc_traceback.tb_lineno)+').', e)
           error_conn = psycopg2.connect(conn_string)
           error_cursor = error_conn.cursor()
           error_cursor.execute("update etl.job set status='error' where id = " + str(row[0]) + ";")
           error_conn.commit()
           error_conn = None


    def amazonRow(row):
        amazonProvider = amazon.amazon(amazonProperties['AWS_ACCESS_KEY'], amazonProperties['AWS_SECRET_KEY'])
        amazonProvider.getPayments(dtBatchDate-timedelta(days=1), dtBatchDate)
        if amazonProvider.getTransactionCount() > 0:
            try:
                path = amazonProperties['datafile_path']
                strCSVFilePath = path + "/amazon-" + strBatchDate + ".csv"
                logging.info('['+getTimeStamp()+'] [AMAZON] Writing '+strCSVFilePath+' to disk.') 
                amazonProvider.write(path)
                import_conn = psycopg2.connect(conn_string)
                import_cursor = import_conn.cursor()
                import_cursor.execute("do $$ begin perform etl.fn_amazon_payment_importcsv("+str(row[0])+", '"+strCSVFilePath+"'); end$$;")
                import_conn.commit()
                import_conn = None
                logging.info('['+getTimeStamp()+'] [AMAZON] Transactions processed: ('+str(amazonProvider.getTransactionCount())+') | JobId: ('+str(row[0])+') | BatchId: ('+str(row[1])+') | Date: ('+strBatchDate+').')


            except Exception, e:
                reportError(e, row)
        else:
            skipRow(row)

    def meetupRow(row):
        meetupProvider = meetup.meetup(meetupProperties['groupName'], meetupProperties['apiKey'])
        meetupProvider.getPayments(dtBatchDate, dtBatchDate)
        if meetupProvider.getTransactionCount() > 0:
            try:
                path = meetupProperties['datafile_path']
                strCSVFilePath = path + "/meetup-" + strBatchDate + ".csv"
                logging.info('['+getTimeStamp()+'] [MEETUP] Writing '+strCSVFilePath+' to disk.') 
                meetupProvider.write(path)
                import_conn = psycopg2.connect(conn_string)
                import_cursor = import_conn.cursor()
                import_cursor.execute("do $$ begin perform etl.fn_meetup_payment_importcsv("+str(row[0])+", '"+strCSVFilePath+"'); end $$;")
                import_conn.commit()
                import_conn = None
                logging.info('['+getTimeStamp()+'] [MEETUP] Transactions processed: ('+str(meetupProvider.getTransactionCount())+') | JobId: ('+str(row[0])+') | BatchId: ('+str(row[1])+') | Date: ('+strBatchDate+').')

            except Exception, e:
                reportError(e, row)

        else:
            skipRow(row)

    logging.info('['+getTimeStamp()+'] [START] Data import process started.')
    projectProperties = getProperties('data-import.properties')
    logging.debug('['+getTimeStamp()+'] [GENERAL] Reading in project properties file ('+str(projectProperties)+')')
    meetupProperties = getProperties('meetup.properties')
    logging.debug('['+getTimeStamp()+'] [MEETUP] Reading in Meetup properties file ('+str(meetupProperties)+')')
    amazonProperties = getProperties('amazon.properties')
    logging.debug('['+getTimeStamp()+'] [AMAZON]  Reading in Amazon properties file ('+str(amazonProperties)+')')

    conn_string = "host='"+projectProperties['conn_host']+"' dbname='"+projectProperties['conn_dbname']+"' user='"+projectProperties['conn_user']+"'"

    logging.info('['+getTimeStamp()+'] [GENERAL] Generating batches.')
    gen_conn = psycopg2.connect(conn_string)
    gen_cursor = gen_conn.cursor()
    gen_cursor.execute("do $$ begin perform etl.fn_generate_batch(); end$$;")
    gen_conn.commit()
    gen_conn = None

    job_conn = psycopg2.connect(conn_string)
    job_cursor = job_conn.cursor()
    job_cursor.execute("select job.id, batch_id, batch.key as batch_key, product.key as product_key \
                    from etl.job \
                    inner join etl.batch \
                    on job.batch_id = batch.id \
                    inner join etl.product \
                    on batch.product_id = product.id \
                    where job.status in ('pending', 'ready') \
                    order by product_key asc, batch.key asc;")
    
    logging.info('['+getTimeStamp()+'] [GENERAL] Jobs to process: ('+str(job_cursor.rowcount)+').')
    
    for row in job_cursor:
        time.sleep(1)
        dtBatchDate = datetime.datetime.strptime(row[2], '%Y-%m-%d')
        strBatchDate = str(datetime.datetime.strptime(row[2], '%Y-%m-%d')).split(" ")[0]
        if row[3] == 'meetup_payments':
            meetupRow(row)
        elif row[3] == 'amazon_payments':
            amazonRow(row)

    logging.info('['+getTimeStamp()+'] [FINISH] Data import processing completed.')

if __name__ == "__main__":
    main()
