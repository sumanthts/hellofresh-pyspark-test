import argparse
import os
from pyspark.sql import SparkSession
import importlib
import sys
import json
import time

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')


def get_config(path, job):
    """
    load the configuration of the job in json to a config object
    :param path: str
        relative path of the job resources
    :param job: str
        job name
    :return: config
    """
    file_path = path + '/' + job + '/resources/args.json'
    with open(file_path, encoding='utf-8') as json_file:
        config = json.loads(json_file.read())
    config['relative_path'] = path
    return config


if __name__ == '__main__':
    # parsing and extracting the arguments
    parser = argparse.ArgumentParser(description='pyspark job arguments')
    parser.add_argument('--job', type=str, required=True, dest='job_name',
                        help='The name of the spark job you want to run')
    parser.add_argument('--res-path', type=str, required=True, dest='res_path',
                        help='relative path to the jobs resources')

    args = parser.parse_args()
    print("Called with arguments: %s" % args)

    # create the entry point SparkSession to pyspark
    spark = SparkSession\
        .builder\
        .appName(args.job_name)\
        .getOrCreate()

    # invoke the corresponding job based on the job name
    job_module = importlib.import_module('jobs.%s' % args.job_name)

    # initialize logger for yarn cluster logs
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.info("pyspark script logger initialized")

    start = time.time()
    job_module.run(spark, get_config(args.res_path, args.job_name), logger)
    end = time.time()

    logger.info("Execution of job %s took %s seconds" % (args.job_name, end - start))