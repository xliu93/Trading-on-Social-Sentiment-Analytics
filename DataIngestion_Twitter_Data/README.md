
## Data Ingestion for Twitter Dataset.

Due to the change of policy of Twitter, we no longer have free access to all historical tweets. The standard search on Twitter has rate limit and only allows search a user timeline for the last 7-9 days. 

With the work of Jefferson-Henrique, we collected tweets from selected users and save them on Hadoop File System. 

### Acknowledgement: 

Using package: [**GetOldTweets-python**] (https://github.com/Jefferson-Henrique/GetOldTweets-python) 
by Jefferson Henrique - 
A project written in Python to get old tweets, it bypass some limitations of Twitter Official API.


### Prerequisites
This package assumes using Python 2.x. The Python3 "got3" folder is maintained as experimental and is not officially supported.

Expected package dependencies are listed in the "requirements.txt" file for PIP, you need to run the following command to get dependencies:
```
pip install -r requirements.txt
```

### Dump the Tweets

1. Dump tweets for a single user:
	- `python Exporter.py --username <user> --since <yyyy-mm-dd> --until <yyyy-mm-dd>`
2. Dump tweets for list of users:
	- `./BATCH_RUN <txt file contains usernames>`

3. To use the same list of usernames, use usernames/distinct_usernames.txt.

*`sbatch_task` is a sbatch script I use on NYU HPC cluster. 
