from abc import abstractmethod, ABC

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    ArrayType,
    MapType,
    IntegerType,
)
from pyspark.sql.functions import (
    sum,
    col,
    lit,
    when,
    regexp_replace,
    explode,
    collect_list,
    desc,
)

from extract.warc_record_util import decompress_udf, decode_udf
from extract.social_media_util import extract_tweets_udf, aggregate_action_stats_udf

from aut import (
    extract_domain,
    WebArchive,
    extract_boilerplate,
    remove_html,
    remove_http_header,
)


def keep_valid_pages(df: DataFrame) -> DataFrame:
    """
    Keep only HTML documents with HTTP Status Code 200 OK
    :param df:
    :return: filtered dataframe
    """
    return (
        df.filter(df.crawl_date.isNotNull())
        .filter(
            ~(df.url.rlike(".*robots\\.txt$"))
            & (
                df.mime_type_web_server.rlike("text/html")
                | df.mime_type_web_server.rlike("application/xhtml\\+xml")
                | df.url.rlike("(?i).*htm$")
                | df.url.rlike("(?i).*html$")
            )
        )
        .filter(df.http_status_code.rlike("200"))
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

    def load(self, spark: SparkSession, path: str) -> DataFrame:
        return (
            spark.read.format("csv")
            .schema(self.schema())
            .option("multiline", "true")
            .option("path", path)
            .option("escape", '"')
            .load()
        )

    @abstractmethod
    def extract(self, spark: SparkSession, path: str) -> DataFrame:
        pass

    @abstractmethod
    def merge(self, spark: SparkSession, path: str) -> DataFrame:
        pass

    def save(self, df: DataFrame, path: str):
        df.coalesce(1).write.format("csv").option("escape", '"').option(
            "path", path
        ).save()


class LinkgraphStrategy(ExtractStrategy):
    def schema(self):
        return StructType(
            [
                StructField("crawl_date", StringType(), True),
                StructField("src", StringType(), True),
                StructField("dest", StringType(), True),
                StructField("weights", LongType(), True),
            ]
        )

    def name(self):
        return "linkgraph"

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
    def schema(self):
        return StructType(
            [
                StructField("crawl_date", StringType(), True),
                StructField("url", StringType(), True),
                StructField("language", StringType(), True),
                StructField("content", StringType(), True),
            ]
        )

    def name(self):
        return "plaintext"

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
    def name(self):
        return "noboilerplate"

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


class TweetStrategy(ExtractStrategy):
    def schema(self):
        return StructType(
            [
                StructField("tweet_time", StringType()),
                StructField("tweet_author", StringType()),
                StructField("retweeter", StringType()),
                StructField("tweet_text", StringType()),
                StructField("hashtags", ArrayType(StringType())),
                StructField("mentioned_users", ArrayType(StringType())),
                StructField("link_destinations", ArrayType(StringType())),
                StructField("action_stats", MapType(StringType(), IntegerType())),
            ]
        )

    def name(self):
        return "tweet"

    def load(self, spark: SparkSession, path: str) -> DataFrame:
        return spark.read.format("json").schema(self.schema).option("path", path).load()

    def extract(self, spark: SparkSession, path: str) -> DataFrame:
        """
        Extract individual tweets from webrecorder WARCs, including metadata.
        :param spark:
        :param path: glob pattern to specify WARC files
        :return: Dataframe containing the time the tweet was published, its author, retweeter, text content,
            hashtags, users mentioned, link destinations and number of replies, retweets and favorites
        """
        return (
            WebArchive(spark.sparkContext, spark, path)
            .all()
            .filter(
                col("mime_type_web_server").rlike("text/html")
                | col("url").rlike(
                    "https?://twitter.com/i/profiles/show/[^/]+/timeline/tweets(\\?.+)?"
                )
            )
            .withColumn("content", decode_udf(decompress_udf("content", "bytes")))
            .select(
                explode(extract_tweets_udf("mime_type_web_server", "content")).alias(
                    "tweet"
                )
            )
            .select(col("tweet.*"))
            .sort(desc("tweet_time"))
        )

    def merge(self, spark: SparkSession, path: str) -> DataFrame:
        """
        Merge tweet extracts whose file name matches the provided glob pattern.
        :param spark:
        :param path: glob pattern to specify HTML extracts
        :return: Dataframe listing unique extracts with the time the tweet was published, its author, retweeter,
        text content, hashtags, users mentioned, link destinations and number of replies, retweets and favorites
        """
        extracts = self.load(spark, path)
        return (
            extracts.groupBy(
                "tweet_time",
                "tweet_author",
                "retweeter",
                "tweet_text",
                "hashtags",
                "mentioned_users",
                "link_destinations",
            )
            .agg(
                aggregate_action_stats_udf(collect_list("action_stats")).alias(
                    "action_stats"
                )
            )
            .sort(desc("tweet_time"))
        )

    def save(self, df: DataFrame, path: str):
        df.coalesce(1).write.format("json").option("path", path).save()
