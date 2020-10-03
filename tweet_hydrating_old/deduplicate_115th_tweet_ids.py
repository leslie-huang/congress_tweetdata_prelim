import pickle

# Load 115th Congress tweets and check if we already have them
# tweet ids from https://doi.org/10.7910/DVN/UIVHQR


with open("tweet_ids.pkl", "rb") as f:
    already_have_tweet_ids = pickle.load(f)

with open(
    "/Users/lesliehuang/Desktop/dataverse_files/representatives-1.txt", "r"
) as f:
    tweet_ids_115th_reps = f.readlines()

with open(
    "/Users/lesliehuang/Desktop/dataverse_files/senators-1.txt", "r"
) as f:
    tweet_ids_115th_sens = f.readlines()

tweet_ids_115th_reps = [int(i.strip()) for i in tweet_ids_115th_reps]
tweet_ids_115th_sens = [int(i.strip()) for i in tweet_ids_115th_sens]

all_115th_tweets = tweet_ids_115th_reps + tweet_ids_115th_sens

print(f"already have: {len(already_have_tweet_ids)}")

print(already_have_tweet_ids[:10])

print(f"115th reps and sens: {len(all_115th_tweets)}")

print(all_115th_tweets[:10])

set_diff = set(all_115th_tweets).difference(set(already_have_tweet_ids))

print(f"set diff: {len(set_diff)}")

with open("tweet_ids_to_hydrate.txt", "w") as f:
    f.write("\n".join(str(i) for i in list(set_diff)))
