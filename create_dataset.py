import json
import os
import pandas as pd
from pandas.io.json import json_normalize
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Setup sql
engine = create_engine("sqlite:///test.db")

session = sessionmaker()
session.configure(bind=engine)
s = session()

dirname = "../congresstweets/data"


###################################################
# get metadata first
###################################################

# get social media handle - legislator mapping
with open("legislators-social-media.json", "r") as f:
    sm_data = json.load(f)

legislator_sm_df = json_normalize(sm_data)[
    ["id.govtrack", "social.twitter_id", "social.twitter"]
]

# need to lowercase for matching
legislator_sm_df["social.twitter"] = legislator_sm_df[
    "social.twitter"
].str.lower()


# get legislator - party mapping
keep_cols = ["id.govtrack", "type", "state", "party"]


def extract_legis_metadata(fn, keep_cols):
    with open(fn, "r") as f:
        dat = json.load(f)
    df = json_normalize(dat)

    # fix ridiculous nested dict/list/idct
    terms = pd.DataFrame(df.terms.tolist())[[0]]
    terms.columns = ["col"]
    terms = terms["col"].apply(pd.Series)[["type", "state", "party"]]

    df = pd.concat([df, terms], axis=1)[keep_cols]

    return df


current_legis = extract_legis_metadata("legislators-current.json", keep_cols)
historical_legis = extract_legis_metadata(
    "legislators-historical.json", keep_cols
)

# combine legislator metadata
all_legislators_metadata_df = pd.concat([current_legis, historical_legis])

# join with social media metadata

combined_metadata = legislator_sm_df.merge(
    all_legislators_metadata_df,
    how="left",
    left_on="id.govtrack",
    right_on="id.govtrack",
)

combined_metadata.dropna(inplace=True)  # drop anyone with incomplete metadata

###################################################
# Tweets to sql, filtering by users that metadata exists for
###################################################

# tweets to sql
for fname in [f for f in os.listdir(dirname) if f.endswith("json")]:
    temp_df = (
        pd.read_json(os.path.join(dirname, fname))
        .dropna()
        .replace("\n", " ", regex=True)
    )

    if "source" in temp_df.columns:
        temp_df.drop(["source", "link", "time"], axis=1, inplace=True)
    else:
        print(fname)

    # need to lowercase so that the merge keys match
    temp_df["screen_name"] = temp_df["screen_name"].str.lower()

    # combined_metadata only includes users with complete metadata
    temp_df = temp_df.merge(
        right=combined_metadata,
        how="inner",
        left_on="screen_name",
        right_on="social.twitter",
    )

    temp_df.to_sql(
        "tweetsample",
        con=engine,
        index=False,
        index_label="id",
        if_exists="append",
        chunksize=1000,
    )

    del temp_df

s.commit()

###################################################
# now save the filtered metadata as a separate table
###################################################

# create table of user metadata for users in the tweet table
unique_users = pd.read_sql_query(
    "SELECT DISTINCT(screen_name) FROM tweetsample", engine
)
filtered_combined_metadata = unique_users.merge(
    combined_metadata,
    how="left",
    left_on="screen_name",
    right_on="social.twitter",
)

# to sql
filtered_combined_metadata.to_sql(
    "userdata",
    con=engine,
    index=False,
    index_label="screen_name",
    if_exists="append",
    chunksize=1000,
)

s.commit()
