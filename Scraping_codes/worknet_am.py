import pandas as pd
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import UnexpectedAlertPresentException
import pickle
import datetime
import requests #get html

from bs4 import BeautifulSoup
import time

import logging
import pymongo
from pymongo import MongoClient
import json

url = "https://www.worknet.am/en/jobs"
driver = webdriver.Chrome()
driver.get(url)


timeout = time.time() + 60*5   # 5 minutes from now
while True:
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    if time.time() > timeout:
        break

announcements = driver.find_elements_by_css_selector(".listview__heading a")
hrefs = []
for ann in announcements:
    hrefs.append(ann.get_attribute("href"))
hrefs2 = [href.replace("https://www.worknet.am/",
	"https://www.worknet.am/en/") for href in hrefs]

client = MongoClient('localhost', 27017)
db = client['labor_market']
worknet_am = db.worknet_am 
old_links = set(pd.DataFrame(list(worknet_am.find({},{"Job Link":True})))["Job Link"])

print("All links found", len(hrefs2))
newlinks = list(set(hrefs2).difference(old_links))
print("New links found", len(newlinks))
for new_link in newlinks:#Srbuhi printing all newly found websites
    print(new_link)

def UrlRequest(url):

    BaseLink="https://www.worknet.am"
        
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
    time.sleep(1)
    
    return page

def get_page_info(url):
    page = UrlRequest(url)
    try:
        title = page.select_one("h1").text
    except:
        title = None #""Srbuhi
    try:
        title_en = page.select("small")[-1].text
    except:
        title_en = None #""Srbuhi
    try:
        company = page.select_one(".profile__info > a").text
    except:
        company = None #""Srbuhi
    position, salary, gender, age, region, employment_term = "","","","","",""
    try:
        for li in page.find("ul",class_="icon-list option-items").find_all("li"):
            if "Position" in li.text or "Պաշտոնը" in li.text:
                position = li.text.replace("Position","").strip()
            elif "Salary" in li.text:
                salary = li.text.replace("\n","").replace("Salary","").strip()
            elif "Work schedule" in li.text:
                employment_term = li.text.replace("Work schedule","").strip()
            elif "Gender" in li.text:
                gender = li.text.replace("Gender","").strip()
            elif "Age" in li.text:
                age = li.text.replace("Age","").strip()
            elif "Region" in li.text:
                region = li.text.replace("\n","").replace("Region","").strip()
    except:
        pass
    try:
        if "Published" in page.find_all("li")[-1].text:
            published = page.find_all("li")[-1].text.replace("Published","").strip()
        else:
            published =None #""Srbuhi
    except:
        published = None #""Srbuhi
    try:
        job_desc = page.find("p").text
    except:
        job_desc = None #""Srbuhi
    try:
        website = page.select(".icon-list a")[-1].text
    except:
        website = None #""Srbuhi
    updated = datetime.datetime.now()#Srbuhi this is the same as scrape date
    
    feat_list={"Company":[company], "Job Title":[title],"Job Title (en)": [title_en], "Post Date":[published],
               "Employment Term": [employment_term],"Job Type":[position],"Job Location":[region],
               "Salary":[salary],"Job Description":[job_desc], "Job Link":[url], "Added":[updated], "website":[website],
              "Gender":[gender]}
    feat_list=pd.DataFrame.from_dict(feat_list)
    return feat_list

job_info = []
for href in newlinks:
    job_info.append(get_page_info(href))
        
if len(job_info) > 1:
    DataFinal=pd.concat(job_info)
    DataFinal = DataFinal.reset_index(drop=True)
    records = json.loads(DataFinal.T.to_json()).values()
    worknet_am.insert_many(records)
    