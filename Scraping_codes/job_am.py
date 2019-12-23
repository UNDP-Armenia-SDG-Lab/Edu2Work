# ## Job.am scrapper
import pandas as pd
from bs4 import BeautifulSoup
import time
import re
import datetime
import requests
import os
import pymongo
from pymongo import MongoClient
import numpy as np #for numeric operations
import matplotlib.pyplot as plt #for visualization
import json
import logging

client = MongoClient('localhost', 27017)
db = client['labor_market']
job_am = db.job_am
try:
    hrefs_old = set(pd.DataFrame(list(job_am.find({},{"urls":True})))["urls"])
except:
    hrefs_old = []
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    filename='main.log',
                    filemode='w')
# Function to request url
def UrlRequest(url):

    BaseLink="https://job.am"
        
    if BaseLink not in url:
        url=BaseLink+str(url)

    response=requests.get(url)
    rsc=response.status_code
    if rsc<300:
        page=response.content
        page=BeautifulSoup(page,'html.parser')
    else:
        page=""
        
    # Wait time
#     time.sleep(1)                                                      
    return page

# Function to get info from a job announcement
def get_df(url): #upd #Srbuhi
    page = UrlRequest(url)
    title=page.select_one("h1").get_text(strip=True)
    company_name=page.select_one("h5 a").text
    industry=city=salary=date_posted=deadline = "NA"
    try:
        attrs = [i.text for i in page.find_all("p",class_="mb-2")]
        for attr in attrs:
            if "Ոլորտ:" in attr:
                industry = attr.split(": ")[1]
                continue;
            elif "Քաղաք:" in attr:
                city = attr.split(": ")[1]
                continue;
            elif "Աշխատավարձ" in attr:
                salary = attr.split(": ")[1]
                continue;
            elif "Հրապարակվել" in attr:
                date_posted = attr.split(": ")[1]
                continue;
            if "Վերջնաժամկետ" in attr:
                deadline = attr.split(": ")[1]
    except:
        pass
    try:
        requirements_responsibilities = " ".join([i.get_text(strip=True).replace(u'\xa0', u'') for i in page.select(".job-descr li")])#Srbuhi "".join to avoid having list in final df
    except:
        requirements_responsibilities =None #[] Srbuhi (None instead of list)
    feat_list={"Company":[company_name],"Industry":[industry],"Job Title":[title],"Post date":[date_posted],
           "Deadline":[deadline], "Job Location":[city],
               "Salary":[salary],"Qualification":[requirements_responsibilities]}
    feat_list_df=pd.DataFrame.from_dict(feat_list)
    return feat_list_df




def get_job_links():#Srbuhi the class name has changed
    hrefs  = []
    for page_num in range(1,100):
        page = UrlRequest("https://job.am/hy/jobs?Sort=DATEBOOSTED_DESC&p="+str(page_num))
        if page.find("div", class_="row pt-2 pb-2 topjob") is None:
            print(page_num)
            break
        for i in page.find_all("a",class_="text-dark font-weight-bold wordBreak jttl SpanSize"):
            hrefs.append(i["href"])
    return hrefs





hrefs = get_job_links()
print("all links found: ", len(hrefs))
hrefs = list(set(hrefs).difference(hrefs_old))
logging.debug("new_links_added: "+str(len(hrefs)))
print("new links found: ", len(hrefs))
for new_link in hrefs: #Srbuhi printing new links
    print(new_link)
if len(hrefs)>1:
    final_df = []
    for url in hrefs:
        final_df.append(get_df(url))


    final_df1 = pd.concat(final_df)
    final_df1["urls"] = hrefs
    final_df1 = final_df1.reset_index(drop=True)
    now = datetime.datetime.now()
    final_df1["date_added"] = now.strftime("%Y-%m-%d %H:%M")  #Srbuhi this is a scrape date


    records = json.loads(final_df1.T.to_json()).values()
    job_am.insert_many(records)
    
    logging.debug("number of jobs in the data: " + str(job_am.count_documents(filter={})))
else:
    logging.debug("No new links were found :(")

