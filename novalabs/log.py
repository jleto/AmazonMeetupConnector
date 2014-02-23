import sys
import logging
from datetime import datetime

def getTimeStamp():
    return '[' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ']'

def writeError(e):
    e = sys.exc_info()[0]
    logging.info(getTimeStamp()+' [ERROR] %s. File: '+sys.exc_traceback.tb_frame.f_code.co_filename+'. Line ('+str(sys.exc_traceback.tb_lineno)+').', e)

def writeInfo(strMsg):
    logging.info(getTimeStamp()+' ' + strMsg)

