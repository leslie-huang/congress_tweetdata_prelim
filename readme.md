# DATASET CONSTRUCTION AND SOURCES

`legislators-social-media.json`, `legislators-current.json`, and `legislators-historical.json` are from https://github.com/unitedstates/congress-legislators


## Language model pretraining dataset

`lm_train.db` is a (de-duplicated) union of the following sources:
- tweets from https://github.com/alexlitel/congresstweets/tree/master/data (through 8/1/2020)
- tweets from the 115th and 116th Congresses, which were hydrated using https://github.com/DocNow/hydrator/
  - https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/UIVHQR
  - https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/MBOJNS

The primary key is the tweet id (aka id_str). The total number of records is 5186977

This includes both tweets from members of the House/Senate, as well as political parties, campaign accounts, etc and tweets that may be well outside of the timeframe of the other datasets

### Generating this dataset requires the following steps:

First run

```
python3 create_mlm_dataset.py --db_path db.db --data_dir ../congresstweets/data/
```

Then add each dataset of hydrated tweets, checking for duplicates

```
python3 hydrated_tweets_to_db.py --tweet_ids_fn tweet_ids_to_hydrate_115th.txt --tweet_json_fn hydrated_115th.jsonl
```

Then check for missing tweets again (deleted tweets will continue to be in this list); this generates a txt file (`missing_tweets.txt`) of the IDs of tweets we wanted but are not in the database
```
python3 check_db_missing_tweets.py
```


## Heldout data for validation/test of language models

`lm_heldout.db` was created using
```
python3 create_mlm_dataset.py --db_path heldout.db --data_dir ../congresstweets/data/heldout_data
```

where the data directory contains tweet jsons from 8/1/2020 through 9/26/2020 from https://github.com/alexlitel/congresstweets/tree/master/data

This includes ONLY members of the House/Senate

The primary key is the tweet id (aka id_str). The total number of records is 161282


## Dataset for classification

For the `tweetbert` classifiers, we use the large version of tweets from 2017 through 10/17/2020 from https://github.com/alexlitel/congresstweets/tree/master/data

`classification_db.db` was created in several steps:

- `classification_dataset_inspect_metadata.ipynb` generates `classification_unfiltered.db` and makes a list of the unique twitter screennames included in it. Then metadata files from `unitedstates/congress-legislators` are merged together to get a mapping of twitter screen names to political party. The set difference of these lists of screen names is written to `missing_metadata.csv`
- the missing metadata is filled in manually in `missing_metadata_filled.csv` -- this includes campaign and non-official accounts for politicians as well as caucus/committee/organizational twitter accounts that are clearly partisan
- `create_classification_dataset.ipynb` generates `classification_final.db`. this ONLY keeps tweets which (a) match the official metadata or (b) match the manually coded accounts

The primary key is the tweet id (aka id_str). The total number of records in `classification_final.db` is 2271407, of which 1519295 are from official accounts

*NOTE* Subsequently, in `tweetbert/example_nbs/08_example_polarity_per_legislator.ipynb` we identify case-sensitive variations in screen_name for the following people:
  - RepTimmons
  - RepGolden
  - RepLucyMcBath
  - RepDavidKustoff
  - RepDavidTrone
  - SenBooker

  These were manually resolved in the `tweets` table of `classification_final.db`. Note that the multiple variations in screen_name do appear as separate entries in the `users` table
