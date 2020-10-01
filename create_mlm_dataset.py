# Create a db with just tweet text and id for language model training

import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Setup sql
engine = create_engine("sqlite:///lm_db.db")

session = sessionmaker()
session.configure(bind=engine)
s = session()

dirname = "../congresstweets/data"

# we only need tweet text and don't need to filter users!

for fname in [f for f in os.listdir(dirname) if f.endswith("json")]:
    temp_df = (
        pd.read_json(os.path.join(dirname, fname))
        .dropna()
        .replace("\n", " ", regex=True)
    )[["text", "id"]]

    temp_df.to_sql(
        "lm_tweets",
        con=engine,
        index=False,
        if_exists="append",
        chunksize=1000,
    )

    del temp_df

s.commit()
