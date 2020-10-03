import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

collected_json_fn = "hydrated_115th.jsonl"
tweets_requested_fn = "tweet_ids_to_hydrate_115th.txt"

# Setup sql
engine = create_engine("sqlite:///hydrated_tweets_115.db")

session = sessionmaker()
session.configure(bind=engine)
s = session()


with open(tweets_requested_fn, "r") as f:
    requested_ids = f.readlines()

requested_ids = [i.strip() for i in requested_ids]

print(f"We asked for: {len(requested_ids)}")

requested_set = set(requested_ids)

print(f"Checking for duplicates... set is {len(requested_set)}")

collected_tweets_ids = []

with open(collected_json_fn, "r") as f:
    data_for_df = []
    for line_idx, line in enumerate(f):
        try:
            result = json.loads(line)
            collected_tweets_ids.append(result["id_str"])

        except Exception as e:
            print(e)
            print(result["id_str"])
            print(result)

print(f"We got: {len(collected_tweets_ids)}")

collected_set = set(collected_tweets_ids)

print(f"Checking for duplicates... set is {len(collected_set)}")

print("Set difference between requested and received")

print(len(requested_set.difference(collected_set)))

with open("115th_missed_tweets.txt", "w") as f:
    f.write(
        "\n".join(
            str(i) for i in list(requested_set.difference(collected_set))
        )
    )


print("Set difference between received and requested")

print(len(collected_set.difference(requested_set)))
