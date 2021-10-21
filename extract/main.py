import re
from pathlib import Path
from shutil import rmtree

import click
from pyspark.sql import SparkSession, DataFrame

from extract.extract_strategies import (
    LinkgraphStrategy,
    PlaintextStrategy,
    NoBoilerplateStrategy,
    TweetStrategy,
)


@click.command()
@click.option(
    "--derivative",
    type=click.Choice(["linkgraph", "plaintext", "noboilerplate", "tweet"]),
    required=True,
    help="The kind of derivative to be produced.",
)
@click.option("--warc", "input_type", flag_value="warc", default=True)
@click.option(
    "--target-instance", "input_type", flag_value="target_instance", default=True
)
@click.argument("file", type=click.File())
def main(derivative, input_type, file):
    input = [line.strip() for line in file]

    input_path = Path("/in")
    output_path = Path("/out")

    spark = SparkSession.builder.appName("Extract").getOrCreate()

    if derivative == "linkgraph":
        strategy = LinkgraphStrategy()
    elif derivative == "plaintext":
        strategy = PlaintextStrategy()
    elif derivative == "noboilerplate":
        strategy = NoBoilerplateStrategy()
    elif derivative == "tweet":
        strategy = TweetStrategy()

    suffix = "_" + strategy.name()

    if input_type == "warc":
        for file in input_path.iterdir():
            if not file.is_dir() and file.name in input:
                warc_path = file
                extract_file = re.sub(".warc.gz", suffix, file.name)
                extract_path = output_path / extract_file
                # skip successful extracts, remove data from failed extraction attempts
                if Path(extract_path / "_SUCCESS").exists():
                    continue
                else:
                    if extract_path.exists():
                        rmtree(str(extract_path))
                extract = strategy.extract(spark, str(warc_path))
                strategy.save(extract, str(extract_path))

    elif input_type == "target_instance":
        for file in input_path.iterdir():
            if file.is_dir() and file.name in input:
                warc_pattern = "*.warc.gz"
                warc_path = file / "arcs" / warc_pattern
                extract_path = output_path / (file.name + suffix)

                # skip successful extracts, remove data from failed extraction attempts
                if Path(extract_path / "_SUCCESS").exists():
                    continue
                else:
                    if extract_path.exists():
                        rmtree(str(extract_path))

                # process WARCs individually if total size exceeds limit
                target_instance_size = sum(
                    warc.stat().st_size
                    for warc in Path(file / "arcs").glob(warc_pattern)
                )
                if target_instance_size < 6000000000:
                    extract = strategy.extract(spark, str(warc_path))
                    strategy.save(extract, str(extract_path))
                else:
                    for warc_path in Path(file / "arcs").iterdir():
                        tmp_output_path = output_path / (file.name + "_tmp")
                        extract_path = tmp_output_path / (warc_path.stem + suffix)
                        extract = strategy.extract(spark, str(warc_path))
                        strategy.save(extract, str(extract_path))
                    extracts_path = Path(tmp_output_path / ("*" + suffix))
                    merge_path = Path(output_path / file.name)
                    merged_extracts = strategy.merge(spark, str(extracts_path))
                    strategy.save(merged_extracts, str(merge_path))
                    rmtree(str(tmp_output_path))


if __name__ == "__main__":
    main()
