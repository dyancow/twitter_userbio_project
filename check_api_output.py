#checking that scraped users list is 1) unique and 2) non clashing with info from other sheet

import pandas as pd
import csv

df = pd.read_csv("./dms_tracker.csv", usecols = ["User ID"])
original_ids = set(df["User ID"].unique())

dfa = pd.read_csv("./scraped_users.csv", usecols=["User ID"])
new_ids = set(dfa["User ID"].unique())

numlines = 0
file = open("./scraped_users.csv")
reader = csv.reader(file)
numlines= len(list(reader))

print("Number of clashing IDS is %d. Number of duplicated IDS is %d" %(len(original_ids.intersection(new_ids)), numlines-len(new_ids)))
print(numlines)