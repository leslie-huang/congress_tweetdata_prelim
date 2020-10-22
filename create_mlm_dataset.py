# Create a db with just tweet text and id for language model training

import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

parser = argparse.ArgumentParser()
parser.add_argument(
    "--db_path", type=str, default="tweets.db", help="Path to SQL database"
)
parser.add_argument(
    "--data_dir",
    type=str,
    default="../congresstweets/data/",
    help="JSON data path",
)

args = parser.parse_args()

# Setup sql
engine = create_engine(f"sqlite:///{args.db_path}")

session = sessionmaker()
session.configure(bind=engine)
s = session()

engine.execute(
    "CREATE TABLE IF NOT EXISTS tweets(id TEXT PRIMARY KEY, text TEXT)"
)

# we only need tweet text and don't need to filter users!

for fname in [f for f in os.listdir(args.data_dir) if f.endswith("json")]:
    temp_df = (
        pd.read_json(os.path.join(args.data_dir, fname))
        .dropna()
        .replace("\n", " ", regex=True)
    )[["id", "text"]]

    temp_df.to_sql(
        "temp_table",
        con=engine,
        index=False,
        if_exists="replace",
        chunksize=1000,
    )

    del temp_df

    insert_into_sql = "INSERT OR IGNORE INTO tweets SELECT * FROM temp_table"
    engine.execute(insert_into_sql)
    s.commit()
