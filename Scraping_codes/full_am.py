    #!/usr/bin/env python
    # coding: utf-8

    # In[1]:


import numpy as np
import pandas as pd
from tqdm import tqdm
import datetime

import json

import bs4
from bs4 import BeautifulSoup
import urllib3
import urllib.request
import re

from pymongo import MongoClient


    # In[20]:


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

def converter(x):
    text=None
    try:
        text=x.replace("[","").replace("]","").replace(",","")
    except:
        pass
    return text



def full_scraper(): 
    
    now = datetime.datetime.now()
    old_links = set(read_from_mongoDB("labor_market", "full_am").Link)

    # base_link = "https://full.am/am/job/public/list?keyword=&search_job=&company_name=&job_type_id=&salary_from=&salary_to=&country="
    # base_raw = urllib.request.urlopen(base_link).data.decode('utf-8', 'ignore')


    u_getter = urllib3.PoolManager()
    base_link = "https://full.am/am/job/public/list?keyword=&search_job=&company_name=&job_type_id=&salary_from=&salary_to=&country="
    base_raw = u_getter.request("GET",base_link).data.decode('utf-8','ignore')

    base_clean = BeautifulSoup(base_raw, 'html.parser')

    num_pages = int(base_clean.find("ul", class_ = "pagination").find_all("li")[-2].text)
    new_links = []
    stop = False
    for page in tqdm(list(range(1,num_pages + 1))): 

        if stop == True:
            break

        link = "https://full.am/am/job/public/list?keyword=&search_job=&company_name=&job_type_id=&salary_from=&salary_to=&country=&page=" + str(page)
        u_getter1= urllib3.PoolManager()
        raw = u_getter1.request("GET",link).data.decode('utf-8','ignore')
        clean = BeautifulSoup(raw, 'html.parser')
        boxes = clean.find("div", class_ = "row list-category").find_all("div", class_ = "col-xs-12")

        for box in boxes:

            if box.find("a") != None:
                try:
                    href = box.find("a")["href"]
                except:
                    href = None
                new_links.append(href)
    print("Number of new links that are Not in old links:",len(set(new_links).difference(set(old_links))))#Srbuhi as links are repeated, so there will definitely be ones that Are in old_links           

    
    if len(new_links) != 0:
        long_job_descriptions = []
        for link in tqdm(new_links):
            raw = urllib.request.urlopen(link)
            clean = BeautifulSoup(raw, 'html.parser')

            try:
                container = clean.find("ul", class_ = "timeline resume-timeline").find_all("li")
            except:
                pass

            if len(container) == 9: 
                scrape_date = now.strftime("%Y-%m-%d %H:%M") 

                try:
                    organization = container[0].find("p").text
                except:
                    organization = None
                try:
                    categories = container[1].find('p').text
                except:
                    categories = None

                try:
                    term_time = container[2].find('p').text
                except:
                    term_time = None

                try:
                    education = container[3].find('p').text
                except:
                    education = None

                try:
                    experience = " ".join([elm.strip() for elm in list(container[4].find("p")) if type(elm) != bs4.element.Tag])
                except:
                    experience = None

                try:
                    raw_gender_container = container[5].find('p').find_all("i")

                    if len(raw_gender_container) == 1:
                        preferred_gender = raw_gender_container[0]["class"][1].split("-")[1]

                    elif len(raw_gender_container) == 2:
                        preferred_gender = "Both"

                    elif len(raw_gender_container) == 0:
                        preferred_gender = None

                except:
                    preferred_gender = None
                try:
                    preferred_age = container[6].find('p').text
                except:
                    preferred_age = None

                try:
                    description = " ".join([elm.strip() for elm in list(container[8].find("p")) if type(elm) != bs4.element.Tag])
                except:
                    description = None

                try:
                    post_date = clean.find("span", class_ = 'date help-block').text.strip()
                except:
                    post_date = None

                try:
                    position = clean.find("h1", class_ = "heading-detailed").text.strip()
                except:
                    position = None

                try:
                    location = clean.find("ul", class_ = "row list-inline list-details item-detailed").find("li").text.strip()
                except:
                     location = None

                try:
                    organization_type = clean.find("div", class_ = "col-sm-9 col-xs-12").find("p").text.strip()
                except:
                    organization_type = None

                salary = None 

                long_job_description = [scrape_date, link, post_date, position, salary, location, organization_type, organization,categories, term_time, education, experience, preferred_gender, preferred_age, description]
                long_job_description_df = pd.DataFrame(long_job_description).T
                long_job_description_df.columns = ["Scrape Date", "Link", "Post Date", "Position", "Salary", "Location",
                       "Organization Type", "Organization", "Categories", "Term Time", "Education", 
                       "Experience", "Preferred Gender", "Preferred Age", "Description"]
                long_job_descriptions.append(long_job_description_df)

            elif len(container) == 10:
                scrape_date = now.strftime("%Y-%m-%d %H:%M") 

                try:
                    organization = container[0].find("p").text
                except:
                    organization = None

                try:
                    categories = container[1].find('p').text
                except:
                    categories = None

                try:
                    term_time = container[2].find('p').text
                except:
                    term_time = None

                try:
                    salary = [int(s) for s in container[3].find('p').text.strip().split() if s.isdigit()]
                except:
                    salary = None

                try:
                    education = container[4].find('p').text
                except:
                    education = None

                try:
                    experience = " ".join([elm.strip() for elm in list(container[5].find("p")) if type(elm) != bs4.element.Tag])
                except:
                    experience = None

                try:
                    raw_gender_container = container[6].find('p').find_all("i")

                    if len(raw_gender_container) == 1:
                        preferred_gender = raw_gender_container[0]["class"][1].split("-")[1]
                    elif len(raw_gender_container) == 2:
                        preferred_gender = "Both"

                    elif len(raw_gender_container) == 0:
                        preferred_gender = None    
                except:
                    preferred_gender = None

                try:
                    preferred_age = container[7].find('p').text
                except:
                    preferred_age = None

                try:
                    description = " ".join([elm.strip() for elm in list(container[9].find("p")) if type(elm) != bs4.element.Tag])
                except:
                    description = None

                try:
                    post_date = clean.find("span", class_ = 'date help-block').text.strip()
                except:
                    post_date = None

                try:
                    position = clean.find("h1", class_ = "heading-detailed").text.strip()
                except:
                    position = None

                try:
                    location = clean.find("ul", class_ = "row list-inline list-details item-detailed").find("li").text.strip()
                except:
                    location = None

                try:
                    organization_type = clean.find("div", class_ = "col-sm-9 col-xs-12").find("p").text.strip()
                except:
                    organization_type = None

                long_job_description = [scrape_date, link, post_date, position, salary, location, organization_type, organization,
                                    categories, term_time, education, experience, preferred_gender, preferred_age, description]
                long_job_description_df = pd.DataFrame(long_job_description).T
                long_job_description_df.columns = ["Scrape Date", "Link", "Post Date", "Position", "Salary", "Location", 
                                               "Organization Type", "Organization", "Categories", "Term Time", "Education",
                                               "Experience", "Preferred Gender","Preferred Age", "Description"]
                long_job_descriptions.append(long_job_description_df)

        long_job_descriptions_single = pd.concat(long_job_descriptions).reset_index(drop = True)  


        old_data_full_am=read_from_mongoDB("labor_market", "full_am")#Srbuhi
        concat=pd.concat([old_data_full_am,long_job_descriptions_single], ignore_index=True, sort=False)#Srbuhi
        #concat.Salary=concat.Salary.apply(converter)
        cols=['Categories','Description','Education','Experience','Link','Location','Organization','Organization Type','Position','Post Date','Preferred Age','Preferred Gender','Term Time']
        concat=concat.drop_duplicates(subset=cols)
        load_to_mongoDB("labor_market", "full_am", concat[concat["_id"].isna()].drop("_id", axis=1))
        #load_to_mongoDB("labor_market", "full_am", long_job_descriptions_single)


if __name__ == '__main__':
    full_scraper()