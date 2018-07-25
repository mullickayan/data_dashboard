import logging
import time

logger = logging.getLogger('DI.Processor')

def run_ingestion(row, thread_no):
    logger.info('Inside Processor')
    time.sleep(1)