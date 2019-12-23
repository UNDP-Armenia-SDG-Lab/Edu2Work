#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import time
import datetime
import numpy as np
import pandas as pd
from tqdm import tqdm_notebook

import re
import urllib.request
from bs4 import BeautifulSoup

import json 
from pymongo import MongoClient

from selenium import webdriver
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# In[ ]:


now = datetime.datetime.now()

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
        
    return price_df

def ijob_scraper(): 
    
    driver = webdriver.Chrome()

    # Open page link 
    first_link = "http://ijob.am/job-list/"
    driver.get(first_link)

    # Wait for page to load 
    time.sleep(10)

    old = read_from_mongoDB("labor_market", "ijob_am")
    old_links = list(old.Link)
    
    stop = False
    new_jobs_raw = []
    while driver.find_element_by_class_name("load_more_jobs").is_displayed():

        if stop:
            break

        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, "load_more_jobs"))).click()

        clean = BeautifulSoup(driver.page_source, 'html.parser')
        job_listings = clean.find('ul', class_ = 'job_listings')
        jobs_raw = [element for element in list(job_listings) if str(type(element)) != "<class 'bs4.element.NavigableString'>"]

        
        for job in jobs_raw:

                try:
                    link = job["data-href"]
                except:
                    link = None

                if link in old_links:
                    stop = True

                    break

                else:
                    if job not in new_jobs_raw:
                        new_jobs_raw.append(job)

        time.sleep(2)
    print(len(new_jobs_raw))
    short_job_descriptions = []
    new_links=[]#Srbuhi
    for job in tqdm_notebook(new_jobs_raw): 

        scrape_date = now.strftime("%Y-%m-%d %H:%M")

        try:
            link = job["data-href"]
        except:
            link = None
        new_links.append(link)
    
    print("Number of new links", len(new_links))#Srbuhi
    for new_link in new_links:#Srbuhi
        print(new_links)#Srbuhi

        short_job_description = [scrape_date, link]
        short_job_description_df = pd.DataFrame(short_job_description).T
        short_job_description_df.columns = ['ScrapeDate', 'Link']
        short_job_descriptions.append(short_job_description_df)

    short_job_descriptions_df = pd.concat(short_job_descriptions).reset_index(drop = True)
    
    long_job_descriptions = []
    for link in tqdm_notebook(list(short_job_descriptions_df.Link)):

        driver.get(link)
        clean = BeautifulSoup(driver.page_source, 'html.parser')

        try:
            position = clean.find("h1", class_ = "page-title").text.strip()
        except:
            position = None

        try:
            term_time = clean.find("ul", class_ = "job-listing-meta meta").find_all("li")[0].text
        except:
            term_time = None

        try:
            location = clean.find("ul", class_ = "job-listing-meta meta").find_all("li")[1].text
        except:
            location = None

        try:
            when_posted = clean.find("ul", class_ = "job-listing-meta meta").find_all("li")[2].text
        except:
            when_posted = None

        try:
            close_date = clean.find("ul", class_ = "job-listing-meta meta").find_all("li")[3].text.split(": ")[1]
        except:
            close_date = None

        try:
            company = clean.find("ul", class_ = "job-listing-meta meta").find_all("li")[4].text.strip()
        except:
            company = None

        try:
            categories = clean.find("div", class_ = "job_listing-categories").text
        except:
            categories = None

        try:
            raw_text = clean.find("div", class_ = "job_listing-description job-overview col-md-12 col-sm-12")
            text_list = []
            for elm in raw_text.find_all("p"):
                try:
                    b = elm.text.strip()
                    b_list = b.split("\n")
                    text_list.extend(b_list)
                except:
                    pass
            description = "::::".join(text_list)
        except:
            description = None

        long_job_description = [position, term_time, location, when_posted, close_date, company, categories, description]
        long_job_description_df = pd.DataFrame(long_job_description).T
        long_job_description_df.columns = ['Position', 'Term Time', "Location", "Date Posted", "Close Date", "Organization", 'Categories', 'Description']
        long_job_descriptions.append(long_job_description_df)

    long_job_descriptions_df = pd.concat(long_job_descriptions).reset_index(drop = True)
    total_df = pd.concat([short_job_descriptions_df, long_job_descriptions_df], axis = 1)
    load_to_mongoDB("labor_market", "ijob_am", total_df)

if __name__ == '__main__':
	ijob_scraper()

