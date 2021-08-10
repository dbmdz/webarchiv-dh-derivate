from pathlib import Path
from shutil import rmtree

import click
from pyspark.sql import SparkSession, DataFrame

from extract.extract_strategies import (
    LinkgraphStrategy,
    PlaintextStrategy,
    NoBoilerplateStrategy,
)


def save_csv(df: DataFrame, path: str):
    df.coalesce(1).write.format("csv").option("escape", '"').option("path", path).save()


@click.command()
@click.option(
    "--target-instance-list",
    type=click.File(),
    required=True,
    help="A file containing a list of Target Instances to be processed.",
)
@click.option(
    "--type",
    type=click.Choice(["linkgraph", "plaintext", "noboilerplate"]),
    required=True,
    help="The kind of derivative to be produced.",
)
def main(target_instance_list, type):
    target_instances = [line.strip() for line in target_instance_list]

    input_path = Path("/in")
    output_path = Path("/out")

    spark = SparkSession.builder.appName("Extract").getOrCreate()

    if type == "linkgraph":
        strategy = LinkgraphStrategy()
    elif type == "plaintext":
        strategy = PlaintextStrategy()
    elif type == "noboilerplate":
        strategy = NoBoilerplateStrategy()

    for file in input_path.iterdir():
        if file.is_dir() and file.name in target_instances:
            warc_pattern = "*.warc.gz"
            warc_path = file / "arcs" / warc_pattern
            extract_path = output_path / file.name

            # skip successful extracts, remove data from failed extraction attempts
            if Path(extract_path / "_SUCCESS").exists():
                continue
            else:
                if extract_path.exists():
                    rmtree(str(extract_path))

            # process WARCs individually if total size exceeds limit
            target_instance_size = sum(
                warc.stat().st_size for warc in Path(file / "arcs").glob(warc_pattern)
            )
            if target_instance_size < 6000000000:
                extract = strategy.extract(spark, str(warc_path))
                save_csv(extract, str(extract_path))
            else:
                suffix = "_" + strategy.name
                for warc_path in Path(file / "arcs").iterdir():
                    tmp_output_path = output_path / (file.name + "_tmp")
                    extract_path = tmp_output_path / (warc_path.stem + suffix)
                    extract = strategy.extract(spark, str(warc_path))
                    save_csv(extract, str(extract_path))
                extracts_path = Path(tmp_output_path / ("*" + suffix))
                merge_path = Path(output_path / file.name)
                merged_extracts = strategy.merge(spark, str(extracts_path))
                save_csv(merged_extracts, str(merge_path))
                rmtree(str(tmp_output_path))


if __name__ == "__main__":
    main()
