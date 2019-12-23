#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import re
import json
import datetime
import numpy as np
import pandas as pd
import urllib.request
from bs4 import BeautifulSoup
from tqdm import tqdm_notebook
from pymongo import MongoClient

def load_to_mongoDB(database, collection, dataframe): 
    
    client = MongoClient()
    col = client[database][collection]
    records = json.loads(dataframe.T.to_json()).values()
    col.insert_many(records)

def read_from_mongoDB(database, collection): 
    
    client = MongoClient('localhost', 27017)
    db = client[database]
    
    price_col = db[collection]
    price_df = pd.DataFrame(list(price_col.find({})))
    
#     rem = db[collection].remove()
    price_df.drop("_id", axis = 1, inplace = True)
    
    return price_df

def myjob_scraper(): 
    
    old = read_from_mongoDB("labor_market", 'myjob')
    old_IDs = list(old.UniqueID.values)
    
    links = {}
    now = datetime.datetime.now()
    for page in tqdm_notebook(np.arange(1, 1000)):

        link = "https://www.myjob.am/Default.aspx?pg=" + str(page)
        raw = urllib.request.urlopen(link)
        clean = BeautifulSoup(raw, 'html.parser')
        container = clean.find("div", class_ = "jobPageContainer").find_all("a")

        for job in container:

            link = "https://www.myjob.am/" + job["href"]
            publish_date = job.find("div", class_ = "shortJobOpening").text.split(" ")[-1]
            unique_id = link + publish_date
            if unique_id not in old_IDs:
                links[link] = [publish_date, unique_id]

        if len(clean.find_all("a", rel = "next")) == 0:
                break
    
    print("Number of new links found", len(links))

    if len(links) > 0:
        
        job_descriptions = []
        for link in tqdm_notebook(list(links.keys())):

            raw = urllib.request.urlopen(link)
            clean = BeautifulSoup(raw, 'html.parser')

            publish_date = links[link][0]
            unique_identifier = links[link][1]
            scrape_date = str(now.year) + "/" + str(now.month) + "/" + str(now.day)

            try:
                company = clean.find("div", id = "MainContentPlaceHolder_jobContainer").find("div", "fullJobCompany").text
            except:
                company = "None"

            try:
                long_position = clean.find("div", id = "MainContentPlaceHolder_jobContainer").find("div", "fullJobTextLong").text
            except:
                long_position = "None"

            try:
                job_category = [elm.text for elm in clean.find("div", id = "MainContentPlaceHolder_jobContainer").find("div", "fullJobTextsShort").find_all("div")][0]
            except:
                job_category = "None"

            try:
                long_address = [elm.text for elm in clean.find("div", id = "MainContentPlaceHolder_jobContainer").find("div", "fullJobTextsShort").find_all("div")][1]
            except:
                long_address = "None"

            try: 
                close_date = [elm.text for elm in clean.find("div", id = "MainContentPlaceHolder_jobContainer").find("div", "fullJobTextsShort").find_all("div")][2]
            except:
                close_date = "None"

            try:
                long_responsibilities = clean.find("div", id = "MainContentPlaceHolder_jobContainer").find_all("div", "fullJobTextLong")[1].text 
            except:
                long_responsibilities = "None"

            try:
                long_qualifications = clean.find("div", id = "MainContentPlaceHolder_jobContainer").find_all("div", "fullJobTextLong")[2].text    
            except:
                long_qualifications = "None"

            try:
                additional_qualifications = clean.find("div", id = "MainContentPlaceHolder_jobContainer").find_all("div", "fullJobTextLong")[3].text
            except:
                additional_qualifications = "None"

            job_description = [link,scrape_date, unique_identifier, publish_date, company, long_position, job_category, long_address, close_date, long_responsibilities, long_qualifications, additional_qualifications]
            job_description_df = pd.DataFrame(job_description).T
            job_description_df.columns = ["Link","Scrape Date", "UniqueID", "Open Date", "Organization", "Position", "Category", "Address", "Close Date", "Responsibilities", "Qualifications", "Additional"]

            job_descriptions.append(job_description_df)


        new = pd.concat(job_descriptions).reset_index(drop = True)
        load_to_mongoDB("labor_market", 'myjob', new)

if __name__ == '__main__':
    myjob_scraper()

