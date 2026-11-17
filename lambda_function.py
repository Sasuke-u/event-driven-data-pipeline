import io
import json
import os
import urllib.parse

import boto3
import pandas as pd

s3 = boto3.client("s3")
sns = boto3.client("sns")
TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()
    for col in df.select_dtypes(include="number").columns:
        df[col] = df[col].fillna(df[col].median())
    for col in ("Fare", "Salary"):
        if col in df.columns:
            df = df[df[col] >= 0]
    return df


def process(bucket: str, key: str) -> str:
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj["Body"])
    rows_in = len(df)

    df = clean(df)
    if df.empty:
        raise ValueError("No rows left after cleaning")

    out_key = key.replace("incoming/", "clean/", 1).rsplit(".", 1)[0] + ".parquet"
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    s3.put_object(Bucket=bucket, Key=out_key, Body=buf.getvalue())

    return f"{key}: {rows_in} rows in, {len(df)} rows out -> {out_key}"


def lambda_handler(event, context):
    for record in event["Records"]:
        body = json.loads(record["body"])
        for r in body.get("Records", []):  # S3 test events have no Records
            bucket = r["s3"]["bucket"]["name"]
            key = urllib.parse.unquote_plus(r["s3"]["object"]["key"])
            try:
                summary = process(bucket, key)
                sns.publish(TopicArn=TOPIC_ARN, Subject="Pipeline SUCCESS", Message=summary)
            except Exception as e:
                sns.publish(TopicArn=TOPIC_ARN, Subject="Pipeline FAILED",
                            Message=f"{key} failed: {e}")
                raise
