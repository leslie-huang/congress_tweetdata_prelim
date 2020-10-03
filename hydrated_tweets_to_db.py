import json
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--tweet_ids_fn", type=str)
parser.add_argument("--tweet_json_fn", type=str)
args = parser.parse_args()

# Setup sql
engine = create_engine("sqlite:///hydrated_tweets.db")

session = sessionmaker()
session.configure(bind=engine)
s = session()

# make table with primary key
engine.execute(
    "CREATE TABLE IF NOT EXISTS tweets(id TEXT PRIMARY KEY, text TEXT)"
)

with open(args.tweet_ids_fn, "r") as f:
    requested_ids = f.readlines()

requested_ids = [i.strip() for i in requested_ids]
print(f"We asked for: {len(requested_ids)}")
requested_set = set(requested_ids)
print(f"Checking for duplicates... set is {len(requested_set)}")

with open(args.tweet_json_fn, "r") as f:
    data_for_df = []

    for line_idx, line in enumerate(f):
        if line_idx % 50000 == 0:
            print(f"Processing line {line_idx}")

        try:
            result = json.loads(line)
            data_for_df.append(result)

            if line_idx % 10000 == 0 and len(data_for_df) > 0:
                temp_df = pd.DataFrame.from_dict(
                    data_for_df, orient="columns"
                )[["id_str", "full_text"]]

                temp_df.dropna(inplace=True)
                temp_df = temp_df.replace("\n", " ", regex=True)
                temp_df.columns = ["text", "id"]

                print(f"Making temp table... from df size {temp_df.shape}")
                temp_df.to_sql(
                    "temp_table",
                    con=engine,
                    index=False,
                    if_exists="replace",
                    chunksize=1000,
                )

                del temp_df
                data_for_df = []

                insert_into_sql = (
                    "INSERT OR IGNORE INTO tweets SELECT * FROM temp_table"
                )
                engine.execute(insert_into_sql)

                s.commit()

        except Exception as e:
            print(e)
            print(result["id_str"])
            print(result)
            break

    # # check if there's anything left to add to the table
    if len(data_for_df) > 0:
        temp_df = pd.DataFrame.from_dict(data_for_df, orient="columns")[
            ["id_str", "full_text"]
        ]

        temp_df.dropna(inplace=True)
        temp_df = temp_df.replace("\n", " ", regex=True)
        temp_df.columns = ["text", "id"]

        print(f"Making temp table... from df size {temp_df.shape}")
        temp_df.to_sql(
            "temp_table",
            con=engine,
            index=False,
            if_exists="replace",
            chunksize=1000,
        )

        del temp_df
        data_for_df = []

        insert_into_sql = (
            "INSERT OR IGNORE INTO tweets SELECT * FROM temp_table"
        )
        engine.execute(insert_into_sql)

        s.commit()


# now check set sizes etc
result = engine.execute("SELECT id FROM tweets")
collected_tweets_ids = [str(r[0]) for r in result]

print(f"We got: {len(collected_tweets_ids)}")
collected_set = set(collected_tweets_ids)
print(f"Checking for duplicates... set is {len(collected_set)}")

print("Set difference between requested and received")
print(len(requested_set.difference(collected_set)))

print("Set difference between received and requested")
print(len(collected_set.difference(requested_set)))
