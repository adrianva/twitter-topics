from pyspark.sql import SparkSession


def get_spark_session_instance(spark_conf):
    if 'sparkSessionSingletonInstance' not in globals():
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=spark_conf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']
