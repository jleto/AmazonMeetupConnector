import os
import psycopg2
from novalabs import properties

def connString():
    dir = os.path.dirname(__file__)
    filename = os.path.join(dir, 'db.properties')
    propertyDict = properties.get(filename)
    return "host='"+propertyDict['conn_host']+"' dbname='"+propertyDict['conn_dbname']+"' user='"+propertyDict['conn_user']+"'"

def execute(sqlCmd):
    conn = psycopg2.connect(connString())
    cursor = conn.cursor()
    cursor.execute(sqlCmd)
    conn.commit()
    conn = None
    return cursor
