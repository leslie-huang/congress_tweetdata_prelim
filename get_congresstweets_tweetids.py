import os
import pickle
from itertools import chain
import pandas as pd

dirname = "../congresstweets/data"

tweet_ids = []

for fname in [f for f in os.listdir(dirname) if f.endswith("json")]:
    try:
        ids = (
            pd.read_json(os.path.join(dirname, fname))
            .dropna()
            .replace("\n", " ", regex=True)
        ).id.tolist()

        tweet_ids.append(ids)
    except AttributeError as e:
        print(e)
        print(fname)

tweet_ids = list(chain.from_iterable(tweet_ids))

print(len(tweet_ids))

with open("tweet_ids.pkl", "wb") as f:
    pickle.dump(tweet_ids, f)
