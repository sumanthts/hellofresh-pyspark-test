import pytest
from src.jobs.recipedata_aggregator import run
from test.jobs.recipedata_preprocessor import test_recipedata_preprocessor
from pyspark.sql import Row


@pytest.mark.run(order=3)
def test_recipedata_aggregator_run(spark_session):
    # set up config
    conf = {
        "relative_path": "",
        "input_file_path": "test/jobs/recipedata_preprocessor/resources/processed_output",
        "output_file_path": "test/jobs/recipedata_aggregator/resources/agg_output",
        "ingredient": "beef"
    }
    # initialize logger
    log4jLogger = spark_session.sparkContext._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)

    run(spark_session, conf, logger)
    df = spark_session.read.option("header", True).csv("test/jobs/recipedata_aggregator/resources/agg_output")
    df.cache()
    # expected_results = [Row(difficulty='hard', avg_total_cooking_time=),
    # Row(difficulty='medium', avg_total_cooking_time=32.5)]
    assert 2 == df.count()
    assert '222.5' == df.where(df.difficulty == 'hard').select("avg_total_cooking_time").collect()[0][0]
    assert '32.5' == df.where(df.difficulty == 'medium').select("avg_total_cooking_time").collect()[0][0]