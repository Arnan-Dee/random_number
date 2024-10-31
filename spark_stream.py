import logging

from cassandra.cluster import Cluster, Session
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructField, StructType


def create_cassandra_connection() -> Session | None:

    try:
        cluster = Cluster(["localhost"])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:

        logging.error(f"Could not create cassandra connection due to {e}")

        return None


def create_spark_connection() -> SparkSession | None:
    s_sess = None

    try:
        s_sess = (
            SparkSession.builder.appName("SparkDataStreaming")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1",
            )
            .config("spark.cassandra.connection.host", "localhost")
            .getOrCreate()
        )
        s_sess.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")

    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_sess


def connect_to_kafka(spark_sess: SparkSession) -> DataFrame | None:
    spark_df = None
    try:
        spark_df = (
            spark_sess.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "users_created")
            .option("startingOffsets", "earliest")
            .load()
        )

        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_df_from_kafka(spark_df: DataFrame) -> DataFrame | None:

    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("gender", StringType(), False),
            StructField("address", StringType(), False),
            StructField("post_code", StringType(), False),
            StructField("email", StringType(), False),
            StructField("username", StringType(), False),
            StructField("registered_date", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("picture", StringType(), False),
        ]
    )

    sel = (
        spark_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    print(sel)
    return sel


def create_keyspace(cas_sess: Session):

    cas_sess.execute(
        """
		CREATE KEYSPACE IF NOT EXISTS spark_stream
		WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
		"""
    )

    print("Keyspace created successfully!")


def create_table(cas_sess: Session):

    cas_sess.execute(
        """
	CREATE TABLE IF NOT EXISTS spark_streams.created_users (
		id UUID PRIMARY KEY,
		first_name TEXT,
		last_name TEXT,
		gender TEXT,
		address TEXT,
		post_code TEXT,
		email TEXT,
		username TEXT,
		registered_date TEXT,
		phone TEXT,
		picture TEXT);
	"""
    )


if __name__ == "__main__":
    spark_sess = create_spark_connection()

    if spark_sess is not None:
        spark_stream_df = connect_to_kafka(spark_sess)
        selection_df = create_df_from_kafka(spark_stream_df)
        cas_sess = create_cassandra_connection()

        if cas_sess is not None:
            create_keyspace(cas_sess)
            create_table(cas_sess)

            logging.info("Streaming is being started...")

            streaming_query = (
                selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                .option("keyspace", "spark_streams")
                .option("table", "created_users")
                .start()
            )

            streaming_query.awaitTermination()
