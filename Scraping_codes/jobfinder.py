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
jobfinder_am = db.jobfinder_am
old_links = set(pd.DataFrame(list(jobfinder_am.find({},{"Job Link":True})))["Job Link"])


def UrlRequest(url):

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

def get_attrs(url):
    now=datetime.datetime.now()
    scrape_date=now.strftime("%Y-%m-%d %H:%M") #Srbuhi
    page1 = UrlRequest(url)
    try:
        title = page1.select_one("#ctl00_bdyPlaceHolde_jfpanelViewJob_jfJobPreview_lblJobPostTitle").text
    except:
        title =None #""#Srbuhi
    try:
        company = page1.select_one("#ctl00_bdyPlaceHolde_jfpanelViewJob_jfJobPreview_lblCompany").text
    except:
        company =None #""#Srbuhi
    try:  #'position type' on website  
        term = page1.select_one("#ctl00_bdyPlaceHolde_jfpanelViewJob_jfJJobPreview_lblPositionType").text
    except:
        term =None #""#Srbuhi
    try:    
        category = page1.select_one("#ctl00_bdyPlaceHolde_jfpanelViewJob_jfJobPreview_lblCategory").text
    except:
        category =None #""#Srbuhi
    try:
        experience = page1.select_one("#ctl00_bdyPlaceHolde_jfpanelViewJob_jfJobPreview_lblExperience").text
    except:
        experience =None #""#Srbuhi
    try:
        education = page1.select_one("#ctl00_bdyPlaceHolde_jfpanelViewJob_jfJobPreview_lblEducation").text
    except:
        education =None #""#Srbuhi
    try:
        location = page1.select_one("#ctl00_bdyPlaceHolde_jfpanelViewJob_jfJobPreview_lblLocation").text
    except:
        location =None #""#Srbuhi
    try:
        dates = page1.select_one("#ctl00_bdyPlaceHolde_jfpanelViewJob_jfJobPreview_lblDate").text
    except:    
        dates =None #""#Srbuhi
    try:
        salary = page1.select_one("#ctl00_bdyPlaceHolde_jfpanelViewJob_jfJobPreview_lblSalary").text
    except:    
        salary =None #""#Srbuhi
    try:    
        age = page1.select_one("#ctl00_bdyPlaceHolde_jfpanelViewJob_jfJobPreview_lblAge").text
    except:None    #Srbuhi
    try:    
        gender = page1.select_one("#ctl00_bdyPlaceHolde_jfpanelViewJob_jfJobPreview_lblGender").text
    except:    
        gender =None #""#Srbuhi
    try:    
        description = page1.select_one("#ctl00_bdyPlaceHolde_jfpanelViewJob_jfJobPreview_lblJobDescription").text
    except:    
        description =None #""#Srbuhi
    try:    
        responsibilites = page1.select_one("#ctl00_bdyPlaceHolde_jfpanelViewJob_jfJobPreview_lblJobResponsibilities").text.replace("\r","")
    except:
        responsibilites =None #""#Srbuhi
    try:
        qualification = page1.select_one("#ctl00_bdyPlaceHolde_jfpanelViewJob_jfJobPreview_lblRequiredQualifications").text.replace("\r","")
    except:
        qualification =None #""#Srbuhi
    try:    
        procedure = page1.select_one("#ctl00_bdyPlaceHolde_jfpanelViewJob_jfJobPreview_lblApplicationProcedure").text.replace("\r","")
    except:
        procedure =None #""#Srbuhi
    try:    
        about = page1.select_one("#ctl00_bdyPlaceHolde_jfpanelViewJob_jfJobPreview_lblAboutCompany").text.replace("\r","")
    except:
        about =None #""#Srbuhi
    feat_list={"Company":[company],#"Industry":"","Page Views":"","Active Jobs":"", "Job History":"","Job Views": "","Job Type":"",#Srbuhi
                "Job Title":[title],"Deadline":[dates],
               "Employment Term": [term],"Job Category":[category],"Job Location":[location],
               "Salary":[salary],"Job Description":[description],"Qualification":[qualification],"Procedure":[procedure],
              "About":[about],"Age":[age],"Experience":[experience],"Preferred_gender":[gender],"Job Link":[url], "Scrape Date":[scrape_date]}
    feat_list=pd.DataFrame.from_dict(feat_list)
    return feat_list

page = UrlRequest("http://jobfinder.am/")
urls = ["http://jobfinder.am/" + page["href"] for page in page.find_all("a", {"title" : 'Display in new window' })]
num_of_jobs = int(re.sub("[^0-9]", "", 
                         page.find("span",id="ctl00_bdyPlaceHolde_grdJobs_JFTabContainer_JFJobs_lblSearchCriteria").text))

urls = urls[:num_of_jobs]

JobLinks = list(set(urls).difference(old_links))
finder_list = []
for url in JobLinks:
    finder_list.append(get_attrs(url))

print("Number of new announcements", len(finder_list))#Srbuhi printing new links and number of new links
for new_link in finder_list:        #Srbuhi
    print(new_link)                 #Srbuhi

if len(finder_list) > 0:
    finder = pd.concat(finder_list)
    DataFinal = finder.reset_index(drop=True)
    records = json.loads(DataFinal.T.to_json()).values()
    jobfinder_am.insert_many(records)