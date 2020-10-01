import pickle

# Load 115th Congress tweets and congresstweets tweets and check
# for new tweets from the 116h Congress
# tweet ids from https://doi.org/10.7910/DVN/MBOJNS


with open("tweet_ids.pkl", "rb") as f:
    already_have_tweet_ids = pickle.load(f)

with open("tweet_ids_to_hydrate.txt", "r") as f:
    hydrate_115th_ids = f.readlines()

already_have_tweet_ids += hydrate_115th_ids

# load 116th
with open(
    "/Users/lesliehuang/Desktop/dataverse_files_116th/congress116-house-ids.txt",
    "r",
) as f:
    tweet_ids_reps = f.readlines()

with open(
    "/Users/lesliehuang/Desktop/dataverse_files_116th/congress116-senate-ids.txt",
    "r",
) as f:
    tweet_ids_sens = f.readlines()

tweet_ids_reps = [int(i.strip()) for i in tweet_ids_reps]
tweet_ids_sens = [int(i.strip()) for i in tweet_ids_sens]

all_116th_tweets = tweet_ids_reps + tweet_ids_sens

print(f"already have: {len(already_have_tweet_ids)}")

print(already_have_tweet_ids[:10])

print(f"116th reps and sens: {len(all_116th_tweets)}")

print(all_116th_tweets[:10])

set_diff = set(all_116th_tweets).difference(set(already_have_tweet_ids))

print(f"set diff: {len(set_diff)}")

with open("tweet_ids_to_hydrate_116th.txt", "w") as f:
    f.write("\n".join(str(i) for i in list(set_diff)))
