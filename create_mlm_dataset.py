# Create a db with just tweet text and id for language model training

import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Setup sql
engine = create_engine("sqlite:///hydrated_tweets.db")

session = sessionmaker()
session.configure(bind=engine)
s = session()

engine.execute(
    "CREATE TABLE IF NOT EXISTS tweets(id TEXT PRIMARY KEY, text TEXT)"
)
dirname = "../congresstweets/data"

# we only need tweet text and don't need to filter users!

for fname in [f for f in os.listdir(dirname) if f.endswith("json")]:
    temp_df = (
        pd.read_json(os.path.join(dirname, fname))
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
    data_for_df = []

    insert_into_sql = "INSERT OR IGNORE INTO tweets SELECT * FROM temp_table"
    engine.execute(insert_into_sql)
    s.commit()
