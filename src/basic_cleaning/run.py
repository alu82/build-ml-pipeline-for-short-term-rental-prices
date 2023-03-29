#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    logger.info("Start to perform basic cleaning with argument: %s", args)
    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    logger.info("Downloading artifact %s.", args.input_artifact)
    input_artifact = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(input_artifact)

    logger.info("Cleaning data...")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    df['last_review'] = pd.to_datetime(df['last_review'])

    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    logger.info("Cleaning data complete. Creating and uploading artifact")
    df.to_csv("clean_data.csv", index=False)

    output_artifact = wandb.Artifact(
        name = args.output_artifact, 
        type = args.output_type, 
        description = args.output_description,
    )    

    output_artifact.add_file("clean_data.csv")
    run.log_artifact(output_artifact)
    logger.info("Artifact added successfully.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning.")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Name of the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Output artifact type (wandb)",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Output artifact description (wandb)",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum price to be considered valid",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="maximum price to be considered valid",
        required=True
    )


    args = parser.parse_args()

    go(args)
