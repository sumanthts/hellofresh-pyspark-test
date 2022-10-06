from pyspark.sql.types import FloatType
from py4j.protocol import Py4JJavaError
import pyspark.sql.functions as F
import isodate


def convert_isodate_to_minutes(t1, t2):
    """
    Function to convert iso date in PT to minutes
    :param t1: str
        a iso date string in PT format
    :param t2: str
        other iso date string in PT format
    :return: int
        total minutes
    :raises ValueError, Overflow
        if the provided strings are not properly aligned to iso date PT format
    """
    try:
        if t1 != '' and t2 != '':
            return (isodate.parse_duration(t1).total_seconds() + isodate.parse_duration(t2).total_seconds()) / 60
        elif t1 != '':
            return isodate.parse_duration(t1).total_seconds() / 60
        else:
            return isodate.parse_duration(t2).total_seconds() / 60
    except(ValueError, OverflowError) as err:
        msg = "Cannot deserialize duration object"
        raise_with_traceback(DeserializationError, msg, err)


# registering the custom conversion function as udf to be used in pyspark DataFrame DSL api
isoDateToSecs = F.udf(lambda t1, t2: convert_isodate_to_minutes(t1, t2), FloatType())


def run(spark, config, logger):
    """
    Reads the recipe data in json format, applies pre-processing and
    write the output to parquet format
    :param spark: SparkSession
        Entry point to the Spark application as Spark Session
    :param config: dict
        configuration object with job config properties
    :param logger: log4jLogger
        logger object
    :return: None
    """
    try:
        # read the recipes json archive
        df = spark.read.json(config['relative_path'] + config['input_file_path'])
        logger.debug("Count of source records: %s" % df.count())
        # get only the valid recipes
        # for any recipe, there should be at least cooking time or preparation time
        valid_df = df.where((df['cookTime'] != '') | (df['prepTime'] != ''))
        # compute total cooking time for each recipe
        total_time_df = valid_df.withColumn("total_cooktime_mins", isoDateToSecs('cookTime','prepTime'))
        # get the recipes with total cook time > 0 to exclude those with cookTime as 'PT' and prepTime as 'PT'
        final_df = total_time_df.where(total_time_df['total_cooktime_mins'] > 0)
        logger.debug("Count of processed records: %s" % final_df.count())

        # persist the output in parquet
        final_df.write.mode("overwrite")\
            .parquet(config['relative_path'] + config['output_file_path'])
    except Py4JJavaError as err:
        raise err
