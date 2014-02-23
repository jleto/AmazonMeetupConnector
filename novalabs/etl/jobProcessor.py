from novalabs import db

def _sqlUpdateJobStatus(jobId, jobStatus):
   return "update etl.job set status = '" + jobStatus + "' where id = " + jobId + ";"

def _sqlGenerateBatches():
   return "do $$ begin perform etl.fn_generate_batch(); end$$;"

def _sqlReadyJobs():
    return "select job.id, batch_id, batch.key as batch_key, product.key as product_key \
           from etl.job \
           inner join etl.batch \
           on job.batch_id = batch.id \
           inner join etl.product \
           on batch.product_id = product.id \
           where job.status in ('pending', 'ready') \
           order by product_key asc, batch.key asc;"

def complete(jobId):
    return db.execute(_sqlUpdateJobStatus(jobId, 'completed'))

def error(jobId):
    return db.execute(_sqlUpdateJobStatus(jobId, 'error'))

def reset(jobId):
    return db.execute(_sqlUpdateJobStatus(jobId, 'pending'))

def generate():
    return db.execute(_sqlGenerateBatches())

def getReadyJobs():
    return db.execute(_sqlReadyJobs())

