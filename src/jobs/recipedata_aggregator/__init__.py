from py4j.protocol import Py4JJavaError
import pyspark.sql.functions as F


def run(spark, config, logger):
    """
    Reads the processed recipe data in parquet format and
    generates a csv report with average cooking time per
    difficulty level for the supplied ingredient
    :param spark: SparkSession
        Entry point to the Spark application as Spark Session
    :param config: dict
        configuration object with job config properties
    :param logger: log4jLogger
        logger object
    :return: None
    """
    try:
        # read the valid recipes parquet data
        df = spark.read.parquet(config['relative_path'] + config['input_file_path'])
        # get the ingredient to analyze on from the config
        ingredient = config['ingredient']
        # get the recipes only with provided ingredient and compute the difficulty level
        ingredient_df = df.where(F.lower(df.ingredients).contains(ingredient)).withColumn('difficulty',
                                                                                          F.when(
                                                                                              df.total_cooktime_mins < 30,
                                                                                              'easy')
                                                                                          .when(
                                                                                              (df.total_cooktime_mins > 60),
                                                                                              'hard')
                                                                                          .otherwise('medium'))
        logger.debug("Count of specific ingredient processed records: %s" % ingredient_df.count())

        # compute the average cooking time duration per difficulty level
        final_df = ingredient_df.groupBy('difficulty').agg(F.round(F.avg('total_cooktime_mins'), 6)
                                                           .alias('avg_total_cooking_time'))

        # save the aggregated output as a csv report
        final_df.coalesce(1).write.mode("overwrite").option("header", True)\
            .csv(config['relative_path'] + config['output_file_path'])
    except Py4JJavaError as err:
        raise err
