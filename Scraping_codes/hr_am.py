import pandas as pd
from bs4 import BeautifulSoup
import time
import re
import datetime
import requests
import logging
import pymongo
from pymongo import MongoClient
import json
import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import UnexpectedAlertPresentException

#logging.basicConfig(level=logging.DEBUG,
#                    format='%(asctime)s %(levelname)s %(message)s',
#                    datefmt='%m/%d/%Y %I:%M:%S %p',
#                    filename='C:\\Users\\User\\Desktop\\edu2work\\scraping_codes\\hr_am\\main.log',
#                    filemode='w')
client = MongoClient('localhost', 27017)
db = client['labor_market']
hr_am = db.hr_am
old_links = set(pd.DataFrame(list(hr_am.find({},{"Job Link":True})))["Job Link"])


url = "http://hr.am/"
driver = webdriver.Chrome()
driver.get(url)
ids = []
number_of_pages =len(driver.find_element_by_id("vacancy_pagination").find_elements_by_css_selector("a"))
for page_num in range(1, number_of_pages+1):
    ids.extend([i.get_attribute("data-id") for i in driver.find_elements_by_class_name("vacancy-item")])
    driver.find_element_by_id("vacancy_pagination").find_elements_by_css_selector("a")[page_num-1].click()
    # print(page_num)
    time.sleep(3)
job_links = ["http://hr.am/vacancy/view/vid/" + i + "/t/" for i in ids]
driver.close()

def UrlRequest(url):

    response=requests.get(url)
    rsc=response.status_code
    if rsc<300:
        page=response.content
        page=BeautifulSoup(page,'html.parser')
    else:
        page=""
        
    # Wait time
    
    return page
def get_features(url):
    page = UrlRequest(url)
    try:
        title = page.select_one(".vacancy-top-text").text
    except:
        title =None #"NaN" #Srbuhi
    try:
        company = page.select_one(".vacancy-info").text
    except:
        company = None #"NaN"
    try:
        deadline = page.select_one("i").text.split(": ")[-1]
    except:
        deadline = None #"NaN"
    try:   
        Job_Desc = page.select_one(".text p").text.replace("\n","")
    except:
        Job_Desc = None #"NaN"
    try:
        specializations =[i.text.split(": ")[-1] for i in page.select('.separator ~ .profile-spec')]
        scope_of_activity =[i.text.split() for i in page.select('.separator ~ .profile-data span')]
        exp = [re.findall('\d+', i.text) for i in page.select('.separator ~ .profile-exp')]
        spec_set = list(zip(specializations,scope_of_activity,exp))
    except:
        spec_set = None #[]
    try:
        skills = [i.text.strip() for i in page.select(".skills-block div") if i.text.strip() != ""]
        years_of_experience = [i.text.strip() for i in page.select(".skills-block span") if i.text != ""]
        skill_set = list(zip(skills,years_of_experience))
    except:
        skill_set = None #[]
    try:
        years_of_experience = page.select_one("screw").text
    except:
        years_of_experience = None #"NaN"
    try:
        soft_skills =  [i.text for i in page.select(".soft-block div")]
    except:
        soft_skills = None #[]
    try:
        education = [i.text for i in page.select(".edu-block span")]
    except:
        education = None #[]
    try:
        languages = [i.text for i in page.select(".lang-block span")]
    except:
        languages = None #[]
    try:
        benefits = [i.text for i in page.select(".benefit-block div") if i.text != '']
    except:
        benefits = None #[]
    views = None #"NaN"
    applications = None #"NaN"
    for i in page.select(".vac-bot-center p"):
        if "Views" in i.text:
            views = re.findall('\d+', i.text)[0]
        elif "Applications" in i.text:
            applications = re.findall('\d+', i.text)[0]
    now=datetime.datetime.now()
    scrape_date=now.strftime("%Y-%m-%d %H:%M") #Srbuhi
    feat_list={"Company": [company],"Job Title": [title],"Deadline": [deadline],
               "Job Type":[title],"Job Description":[Job_Desc],"Qualification":[spec_set],
               "Education":[education], "Benefits": [benefits], "Applications": [applications],
               "Page Views":[views], "Languages":[languages],"Prof Skills":[skill_set], 
               "Soft Skills":[soft_skills],"Job Link":[url],"Scrape_Date":[scrape_date]}#Srbuhi
    feat_list=pd.DataFrame(feat_list)
    
    return feat_list


logging.debug("Actual number of links "+str(len(old_links)))
logging.debug("Links count on the website: "+str(len(job_links)))
newlinks = list(set(job_links).difference(old_links))
print("New links found: ",len(newlinks)) #Srbuhi
for i in newlinks: #Srbuhi
    print(i)

logging.debug("New links found: "+str(len(newlinks)))

if len(newlinks) > 1:
    # Getting information from all pages
    df = []
    for url in newlinks:
        features = get_features(url)
        df.append(features)
    final_data = pd.concat(df).reset_index(drop=True) 
    records = json.loads(final_data.T.to_json()).values()
    hr_am.insert_many(records)
    logging.debug("number of jobs in the data: " + str(hr_am.count_documents(filter={})))
else:
    logging.debug("No new links were found :(")