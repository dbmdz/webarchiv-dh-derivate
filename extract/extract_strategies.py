from abc import abstractmethod, ABC

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import sum, col, lit, when, regexp_replace

from aut import (
    extract_domain,
    WebArchive,
    extract_boilerplate,
    remove_html,
    remove_http_header,
)


class ExtractStrategy(ABC):
    @property
    @abstractmethod
    def schema(self) -> StructType:
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def load(self, spark: SparkSession, path: str) -> DataFrame:
        pass

    @abstractmethod
    def extract(self, spark: SparkSession, path: str) -> DataFrame:
        pass

    @abstractmethod
    def merge(self, spark: SparkSession, path: str) -> DataFrame:
        pass


class LinkgraphStrategy(ExtractStrategy):
    def __init__(self):
        self.schema = StructType(
            [
                StructField("crawl_date", StringType(), True),
                StructField("src", StringType(), True),
                StructField("dest", StringType(), True),
                StructField("weights", LongType(), True),
            ]
        )
        self.name = "linkgraph"

    def schema(self):
        return self.schema

    def name(self):
        return self.name

    def load(self, spark: SparkSession, path: str):
        """
        Load link graph from CSV file into dataframe
        :param spark:
        :param path: path to CSV file
        :return: Dataframe listing links
        """
        return (
            spark.read.format("csv")
            .schema(self.schema)
            .option("multiline", "true")
            .option("path", path)
            .option("escape", '"')
            .load()
        )

    def merge(self, spark: SparkSession, path: str):
        """
        Merge link graphs whose file name matches the provided glob pattern.
        :param spark:
        :param path: glob pattern to specify link graphs
        :return: Dataframe listing unique links with crawl date, source domain, destination URL and total frequency
        """
        extracts = self.load(spark, path)
        return extracts.groupBy("crawl_date", "src", "dest").agg(
            sum("weights").alias("weights")
        )

    def extract(self, spark: SparkSession, path: str):
        """
        Extract extrinsic links from records in WARC file.
        :param spark:
        :param warc_path: glob pattern to specify WARC files
        :return: Dataframe listing crawl date, source domain, destination URL and frequency for each link
        """
        return (
            WebArchive(spark.sparkContext, spark, path)
            .webgraph()
            .groupBy(
                "crawl_date",
                extract_domain("src").alias("src"),
                extract_domain("dest").alias("dest"),
            )
            .count()
            .select(
                "crawl_date",
                "src",
                "dest",
                col("count").alias("weights"),
            )
            .filter((col("dest").isNotNull()) & (col("dest") != ""))
            .filter((col("src").isNotNull()) & (col("src") != ""))
            .filter(col("src") != col("dest"))
            .withColumn(
                "weights",
                when(col("weights").isNotNull(), col("weights")).otherwise(lit(None)),
            )
        )  # ugly hack to enforce nullability of weights-column


class PlaintextStrategy(ExtractStrategy):
    def __init__(self):
        self.schema = StructType(
            [
                StructField("crawl_date", StringType(), True),
                StructField("url", StringType(), True),
                StructField("language", StringType(), True),
                StructField("content", StringType(), True),
            ]
        )
        self.name = "plaintext"

    def schema(self) -> StructType:
        return self.schema

    def name(self) -> str:
        return self.name

    def load(self, spark: SparkSession, path: str) -> DataFrame:
        return (
            spark.read.format("csv")
            .schema(self.schema)
            .option("multiline", "true")
            .option("path", path)
            .option("escape", '"')
            .load()
        )

    def merge(self, spark: SparkSession, path: str) -> DataFrame:
        extracts = self.load(spark, path)
        return extracts.distinct()

    def extract(self, spark: SparkSession, path: str) -> DataFrame:
        return (
            WebArchive(spark.sparkContext, spark, path)
            .webpages()
            .select(
                "crawl_date",
                "url",
                "language",
                remove_html(remove_http_header(("content"))).alias("content"),
            )
            .withColumn("content", regexp_replace(col("content"), r"(\xa0)+", " "))
            .withColumn("content", regexp_replace(col("content"), r"\s+", " "))
        )


class NoBoilerplateStrategy(PlaintextStrategy):
    def __init__(self):
        super().__init__()
        self.name = "noboilerplate"

    def extract(self, spark: SparkSession, path: str) -> DataFrame:
        return (
            WebArchive(spark.sparkContext, spark, path)
            .webpages()
            .select(
                "crawl_date",
                "url",
                "language",
                extract_boilerplate("content").alias("content"),
            )
            .withColumn("content", regexp_replace(col("content"), r"(\xa0)+", " "))
            .withColumn("content", regexp_replace(col("content"), r"\s+", " "))
        )
