# DATASET CONSTRUCTION AND SOURCES

`legislators-social-media.json`, `legislators-current.json`, and `legislators-historical.json` from https://github.com/unitedstates/congress-legislators


## Language model pretraining dataset

### Combines the following sources:
- tweets from https://github.com/alexlitel/congresstweets/tree/master/data (through 9/26/2020)
- tweets from the 115th and 116th Congresses, which were hydrated using https://github.com/DocNow/hydrator/
  - https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/UIVHQR
  - https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/MBOJNS


First run

```
python3 create_mlm_dataset.py --db_path db.db --data_dir ../congresstweets/data/
```

Then add each dataset of hydrated tweets, checking for duplicates

```
python3 hydrated_tweets_to_db.py --tweet_ids_fn tweet_ids_to_hydrate_115th.txt --tweet_json_fn hydrated_115th.jsonl
```

Then check for missing tweets again (deleted tweets will continue to be in this list); this generates a txt file of the IDs of tweets we wanted but are not in the database
```
python3 check_db_missing_tweets.py
```

## Heldout data for validation/test

`heldout.db` was created using
```
python3 create_mlm_dataset.py --db_path heldout.db --data_dir ../congresstweets/data/heldout_data
```

where the data directory contains tweet jsons from 9/1/2020 through 9/26/2020
