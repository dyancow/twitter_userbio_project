import csv, tweepy
import pandas as pd
import time
from os import path

#contains: CONSUMER_TOKEN, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET
KEYS= {}

IDS = set([])

def create_api():
    keys={}
    keyfile = open("secrets.txt", "r")
    for key in keyfile:
        name, value = key.split("=")
        keys[name.split(" ")[0]] = value.split(" ")[1]
    KEYS = keys

    #OAuthHandler Instance with consumer ID keys
    auth = tweepy.OAuthHandler(KEYS['CONSUMER_TOKEN'], KEYS['CONSUMER_SECRET'])
    auth.set_access_token(KEYS['ACCESS_TOKEN'], KEYS['ACCESS_SECRET'])
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    return api

#output all scraped into spreadsheet
def open_backup_document(filename):
    #if the backup document doesn't exist (scrape_users)
    header = ["User ID", "Handle", "Profile Link", "Messages Link", "Election Status", "Election Response", "Permalink", "Search Term", "Bio"]
    if path.exists(filename):
        print("File Exists!")
        #APPEND LIST OF IDS AND HANDLES TO CONSTS.
        return
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
    f.close()
    print("Created file.")
    return

#processes one user while reading output from API call
def process_user(user, index, term):
    user_id = user.id
    handle = user.screen_name
    url = user.url
    bio = user.description

    row = [" "]*9
    row[0] = user_id
    row[1] = handle
    row[2] = url
    row[-1] = bio
    row[-2] = term

    if user_id not in IDS:
        IDS.update([user_id])
        return row
    else:
        return

#writes output from API call to specified file
def write_to_backup(data, query, filename):
    with open(filename, "a") as f:
        writer = csv.writer(f)
        for i, row in enumerate(data):
            user_as_row = process_user(row, i, query)
            if user_as_row:
                writer.writerow(user_as_row)
    f.close()
    print("Finished write to backup.")
    return

#initialize ID const to user IDs we already have
def set_ids():
    df = pd.read_csv("./dms_tracker.csv", usecols = ["User ID"])
    ids = set(df["User ID"].unique())
    return ids

def update_ids():
    df = pd.read_csv("./scraped_users.csv", usecols = ["User ID"])
    ids = set(df["User ID"].unique())
    return ids

def one_api_batch(api, query):
    users = []
    count = 20
    if "'" not in query:
        count = 5
    for i in range(0,count): 
        users.extend(api.search_users(query, page = i, count = 20))
        time.sleep(0.7)
    return users

def record_queries(terms):
    if path.exists("past_api_queries.csv"):
        print("File Exists!")
        return
    with open("past_api_queries.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["term"])
        for term in terms:
            writer.writerow(term)
    f.close()
    print("Created file.")
    return

def set_queries():
    qs = []
    for i in range(21):
        string = str(2000-i)
        qs.append("harvard '"+string[2:])
        if (i<10):
            qs.append("harvard '0"+str(i))
        else:
            qs.append("harvard '"+str(i))
    
    qs.extend(["harvard alum", "harvard alumnae", "harvard alumnus", "harvard alumna"])

    return qs

#code to actually run
def main():
    backupfile = "scraped_users.csv"
    IDS = set_ids()
    api = create_api()
    open_backup_document(backupfile)

    queries = set_queries()

    for query in queries:
        print("Making query call for keyword %s: %d unique users atm." %(query, len(IDS)))
        users_batch = one_api_batch(api, query)

        if(len(users_batch) != 0):
            print("Bath retrieval successful! Got %d users." %(len(users_batch)))
        
        print("Writing users to backup file.")
        #write the users to backup file
        write_to_backup(users_batch, query, backupfile)
        
        #now update IDS
        IDS.update(update_ids())
        print("Length of IDs after call: %d" %(len(IDS)))
        time.sleep(2)

    record_queries(queries)






if __name__ == "__main__":
    main()
