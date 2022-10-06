import pytest
from src.jobs.recipedata_preprocessor import convert_isodate_to_minutes, run


@pytest.mark.run(order=1)
def test_convert_isodate_to_minutes():
    assert 10.0 == convert_isodate_to_minutes('PT','PT10M')
    assert 20.0 == convert_isodate_to_minutes('', 'PT20M')
    assert 40.0 == convert_isodate_to_minutes('PT40M', '')
    assert 160.0 == convert_isodate_to_minutes('PT40M', 'PT2H')


@pytest.mark.run(order=2)
def test_recipedata_preprocessor_run(spark_session):
    # set up config
    conf = {
        "relative_path": "",
        "input_file_path": "test/jobs/recipedata_preprocessor/resources/recipes_sample.json",
        "output_file_path": "test/jobs/recipedata_preprocessor/resources/processed_output"
    }
    # initialize logger
    log4jLogger = spark_session.sparkContext._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)

    run(spark_session, conf, logger)
    parquet_df = spark_session.read.parquet("test/jobs/recipedata_preprocessor/resources/processed_output")
    parquet_df.cache()
    assert 10 == len(parquet_df.columns)
    assert 9 == parquet_df.count()
    assert 0 == parquet_df.where((parquet_df.cookTime == '') & (parquet_df.prepTime == '')).count()
    assert 0 == parquet_df.where(parquet_df.total_cooktime_mins == 0).count()

