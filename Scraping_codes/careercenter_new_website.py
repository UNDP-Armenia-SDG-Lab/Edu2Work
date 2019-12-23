#!/usr/bin/env python
# coding: utf-8

# In[7]:


import numpy as np
import pandas as pd
import time
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
import datetime
from tqdm import tqdm_notebook,tqdm_pandas
from pymongo import MongoClient
import json

# In[57]:


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


# In[ ]:


now = datetime.datetime.now()
website_name="careercenter.am"
try:
    hrefs_old = set(read_from_mongoDB("labor_market", "careercenter")["Job Link"])
except:
    hrefs_old = []


# In[2]:


#Scraping careercenter annoucement urls using timing
def careercenter_link_scraper(): 
    '''Scraper returning links of job announcements in a list'''
    timeout = 60   # [seconds]
    timeout_start = time.time()
    browser = webdriver.Chrome(r'C:\Users\Srbuhi\Desktop\Task\chromedriver.exe')
    lnk="https://careercenter.am/en/jobs"
    browser.get(lnk)
    browser.implicitly_wait(4)
    rows = browser.find_elements_by_xpath("//*[@data-title]")
    while time.time() < timeout_start + timeout:
        rows = browser.find_elements_by_xpath("//*[@data-title]")
        browser.execute_script("arguments[0].scrollIntoView();", rows[-1])
        time.sleep(1)
    links=[i.get_attribute("href") for i in rows]
    browser.quit()
    return links


# In[3]:


def UrlRequest(url):
    '''getting page in beatiful soup format'''
    response=requests.get(url)
    rsc=response.status_code
    if rsc<300:
        page=response.content
        page=BeautifulSoup(page,'html.parser')
    else:
        page=""                                 
    return page


# In[52]:


def get_df(url):
    '''getting job info from the given url page'''
    page = UrlRequest(url)
    job_title=page.find("h1").get_text()
    job_content=page.find("div",class_="tab-content job-tab-content").get_text()
    job_views=int(page.find("span",attrs={"flow":"left"}).get("tooltip").split("retrieved")[1].replace("times","").strip())
    scrape_date=now.strftime("%Y-%m-%d %H:%M")
    website_name="careercenter.am"
    company_name=job_locaion=deadline=None
    try:
        attrs = page.find_all('th')
        for attr in attrs:
                if "Company" in attr.get_text():
                    company_name=attr.find_next("td").get_text().replace("\n","").strip()
                if "Location" in attr.get_text():
                    job_locaion=attr.find_next("td").get_text().replace("\n","").strip()
                if "Deadline" in attr.get_text():
                    deadline=attr.find_next("td").get_text().replace("\n","").strip()
    except:
          pass
    feat_list={"Company":[company_name],"Job Title":[job_title],
           "Deadline":[deadline], "Job Location":[job_locaion],
             "Job Content":[job_content],"Job Views":[job_views],"Job Link":[url],"Scrape_Date":[scrape_date],
               "Website_name":[website_name]}
    feat_list_df=pd.DataFrame.from_dict(feat_list)
    return feat_list_df


# In[54]:



hrefs = careercenter_link_scraper()
print("all links found: ", len(hrefs))
final_hrefs = list(set(hrefs).difference(hrefs_old))
#printing new links
print("new links found: ", len(final_hrefs))

if len(final_hrefs)>1:
    final_df = []
    for url in tqdm_notebook(final_hrefs):
        features=get_df(url)
        final_df.append(features)
    final_concat = pd.concat(final_df).reset_index(drop=True)
    load_to_mongoDB("labor_market", 'careercenter', final_concat)

