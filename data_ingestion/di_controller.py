import logging
import queue
import threading
import json
import pandas as pd
from data_ingestion import di_parameters as param
from data_ingestion import di_processor

logger = logging.getLogger('DI.Controller')

def run(job_file):
    logger.info('Inside Controller')
    df_jobs = pd.read_csv(job_file)
    
    q = queue.Queue()
    workers = []
    
    for s in range(param.thread_count):
        w = threading.Thread(name='Worker-'+str(s+1), target=worker, args=(q, s+1))
        workers.append(w)
        w.start()
        
    for row_index, row in df_jobs.iterrows():
        q.put(row)
        
    q.join()
    
    for _ in range(param.thread_count):
        q.put(None)
        
    for w in workers:
        w.join()
        
    #return 'success'
    json_df_jobs = json.loads(df_jobs.to_json(orient='records'))
    
    return json_df_jobs
    
        
def worker(job_queue, thread_no):
    while True:
        row = job_queue.get()
        if row is None:
            break
        
        di_processor.run_ingestion(row, thread_no)
        job_queue.task_done()
        
      
    #return str(df['firstname'][0]) + '--' +str(df['firstname'][1]) + '--' + str(param.thread_count)