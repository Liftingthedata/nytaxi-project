from dataclasses import dataclass
from typing import Union

import gcsfs
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage


def files_list(bucket, folder, file_prefix) -> list[str]:
    """
    returns a list of items in a specified GCS bucket.
    """
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket, prefix=f"{folder}/{file_prefix}")
    return list(blobs)


def curated_list(blobs: list[bytes]) -> list[str]:
    """
    takes a list made of bytes and extracts object name.
    :param list[bytes] blobs
    :return list[str]
    """
    all_blobs = []
    for blob in blobs:
        row = (str(blob).strip().split(",")[-2]).split("/")[-1]
        all_blobs.append(row)
    return all_blobs


def get_df(bucket, folder, blobs: list[str]) -> pd.DataFrame:
    """
    reads the schema of items in a list by reading the metadata.
    """
    fs = gcsfs.GCSFileSystem()
    output = pd.DataFrame()
    for item in blobs:
        link = f"gs://{bucket}/{folder}/{item}"
        with fs.open(link) as f:
            schema = pq.read_schema(f, memory_map=True)
            data = {k: v for (k, v) in zip(schema.names, schema.types)}
            data["link"] = link
            df_dictionary = pd.DataFrame([data])
            output = pd.concat([output, df_dictionary], ignore_index=True)
    return output.astype(str)


def df_groups_as_list(df: pd.DataFrame):
    """creates a list holding lists of links of files that share the same schema."""
    columns = [value for value in list(df.columns) if value != "link"]
    df_groups = df.groupby(columns)["link"]
    g = df_groups.groups.keys()
    lists = [[] for _ in range(len(g))]
    index = 0
    for key in g:
        subset = df_groups.get_group(key)
        subset = list(subset)
        lists[index].extend(subset)
        index += 1
    return lists


def validator(bucket, folder, file_prefix) -> list[list[str]]:
    blobs = files_list(bucket, folder, file_prefix)
    blobs = curated_list(blobs)  # type: ignore
    df = get_df(blobs)  # type: ignore
    lists = df_groups_as_list(df)
    return lists
