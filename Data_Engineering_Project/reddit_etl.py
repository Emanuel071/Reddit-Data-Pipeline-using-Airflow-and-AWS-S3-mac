from os import access
import praw
import pandas as pd 
import json
from datetime import datetime
import s3fs

#can be used to scrape different elements   (just implement under submission loop)        
# print(submission.title)
# print(submission.id)
# print(submission.author)
# print(submission.created_utc)
# print(submission.score)
# print(submission.upvote_ratio)
# print(submission.url)

def read_creds_from_mac_folder():
    with open('/Users/eacalder/Documents/reddit/creds/reddit_api_creds.csv', 'r') as f: 
        creds = f.readlines()
        client_id = creds[0].split(',')[1].strip()
        client_secret = creds[1].split(',')[1].strip()
        return client_id, client_secret

def run_reddit_etl(client_id : str, client_secret : str):

    user_agent = "Scrapper 1.0 by /u/emanuel071"
    reddit = praw.Reddit(
        client_id= client_id,
        client_secret = client_secret,
        user_agent= user_agent
        )
    

    reddit_list = []
    for submission in reddit.subreddit('politics').hot(limit=20):
        submission_data = {"title": submission.title,
                       "id": submission.id,
                       "author": submission.author.name,
                       "created_utc": submission.created_utc,
                       "score": submission.score,
                       "upvote_ratio": submission.upvote_ratio,
                       "url": submission.url
                      }
        reddit_list.append(submission_data)
    return reddit_list

############################################################################################################################
#### MAIN CODE
############################################################################################################################

client_id, client_secret = read_creds_from_mac_folder()

print(client_id)
print(client_secret)



reddit_list = run_reddit_etl(client_id=client_id, client_secret=client_secret)
df = pd.DataFrame(reddit_list)
print(df.info())
df.to_csv('/Users/eacalder/Documents/Github/Reddit-Data-Pipeline-using-Airflow-and-AWS-S3-mac/Extracted_Data/reddit_data.csv') # local file test run with notebook or cmd to see the result
    
# #df.to_csv('YOUR-S3-BUCKET/reddit_data.csv')