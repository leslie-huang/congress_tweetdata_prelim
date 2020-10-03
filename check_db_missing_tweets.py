from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

tweets_requested_fns = [
    "tweet_hydrating_old/tweet_ids_to_hydrate_115th.txt",
    "tweet_hydrating_old/tweet_ids_to_hydrate_116th.txt",
]

# Setup sql
engine = create_engine("sqlite:///hydrated_tweets.db")

session = sessionmaker()
session.configure(bind=engine)
s = session()

# now check set sizes etc
result = engine.execute("SELECT id FROM tweets")
collected_tweets_ids = set([str(r[0]) for r in result])
print(f"Database contains: {len(collected_tweets_ids)}")

missing_tweet_ids = []

for fn in tweets_requested_fns:
    print(f"--------------\nFile: {fn}")
    with open(fn, "r") as f:
        requested_ids = f.readlines()

    requested_ids = [i.strip() for i in requested_ids]

    print(f"We asked for: {len(requested_ids)}")

    requested_set = set(requested_ids)

    print(f"Checking for duplicates... set is {len(requested_set)}")

    print("Set difference between requested and received")
    print(len(requested_set.difference(collected_tweets_ids)))
    missing_tweet_ids = missing_tweet_ids + list(
        requested_set.difference(collected_tweets_ids)
    )

print(f"Total in missing_tweet_ids: {len(missing_tweet_ids)}")
missing_tweet_ids = set(missing_tweet_ids)
print(f"Total in set missing_tweet_ids: {len(missing_tweet_ids)}")


with open("missed_tweets.txt", "w") as f:
    f.write("\n".join(str(i) for i in missing_tweet_ids))

