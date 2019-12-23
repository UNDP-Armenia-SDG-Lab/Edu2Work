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

now = datetime.datetime.now()#Srbuhi
'''logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    filename='C:\\Users\\User\\Desktop\\edu2work\\scraping_codes\\hr_am\\main.log',
                    filemode='w')'''
client = MongoClient('localhost', 27017)
db = client['labor_market']
staff_am = db.staff_am
old_links = set(pd.DataFrame(list(staff_am.find({},{"Job Link":True})))["Job Link"])

# Function to request url
def UrlRequest(url):

    BaseLink="https://staff.am"
        
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



# Function to get info from a job announcement
def get_job_info(url):
    scrape_date = now.strftime("%Y-%m-%d %H:%M")#Srbuhi
    page = UrlRequest(url)
    try:
        # Company name
        company=page.find_all("h1")[0].get_text()
    except:
        company=None #["Issue"] #Srbuhi
        
    try:
        # Job industry
        industry=page.find_all("p",class_="professional-skills-description")
        indust=[i.get_text() for i in industry][0].split("\n")[2]
    except:
        indust= None #["Issue"] #Srbuhi
    
    # Company job announcements' page views
    # Active jobs
    # Job history
    try:
        chunk=page.find_all("span",class_="margin-r-2")
        chunk=[re.findall("\d+",str(i))[1] for i in chunk]

        page_views=chunk[0]
        active_jobs=chunk[1]  
        job_history=chunk[2]
    except:
        page_views= None #["Issue"] #Srbuhi
        active_jobs= None #["Issue"] #Srbuhi
        job_history= None #["Issue"] #Srbuhi
    
    try:
        # The number of job announcements views
        job_views=page.find("div",class_="statistics").get_text()
        job_views=(re.findall("\d+",job_views))[0]
    except:
        job_views= None #["Issue"] #Srbuhi

    try:
        # Job title
        job_title=page.select_one("div.col-lg-8 h2").get_text()
    except:
        job_title= None #["Issue"] #Srbuhi
    
    try:
        # Deadline
        dead_list=page.find_all("div",class_="col-lg-4 apply-btn-top")[0].find_all("p")[0].get_text().split()[1:]
        dead_list=str(dead_list[0]+" "+dead_list[1]+" "+dead_list[2])
    except:
        dead_list= None #["Issue"] #Srbuhi

    try:
        # Imployment term
        imp_term=page.find_all("div",class_="col-lg-6 job-info")[0].find_all('p')[0].get_text().split()[2]

        #Job type
        job_type=page.find_all("div",class_="col-lg-6 job-info")[0].find_all('p')[1].get_text().split(":")[1].strip()

        # Category
        job_cat=page.find_all("div",class_="col-lg-6 job-info")[1].find_all('p')[0].get_text().split(":")[1]

        # Location 
        job_loc=page.find_all("div",class_="col-lg-6 job-info")[1].find_all('p')[1].get_text().split(":")[1].strip()
    except:
        imp_term= None #["Issue"] #Srbuhi
        job_type= None #["Issue"] #Srbuhi
        job_cat= None #["Issue"] #Srbuhi
        job_loc= None #["Issue"] #Srbuhi
    
    try:
    #Salary
        job_sal=page.find("div",class_="job-list-content-desc").find("span").get_text()

        # Job description
        job_descri=page.find("div",class_="job-list-content-desc").find_all("p")[1].get_text()
    except:
        job_sal= None #["Issue"] #Srbuhi
        job_descri= None #["Issue"] #Srbuhi
    
    try:
    # Job responsibilities and Required qualifications
        q_lists=page.find_all("div",class_="job-list-content-desc")[0].find_all("li")
        q_lists=[i.get_text() for i in q_lists]
    except:
        q_lists= None #["Issue"] #Srbuhi  
    
    try:
    # Job description text in paragraphs
        text_p=page.find("div",class_="job-list-content-desc").find_all("p")
        job_content=[i.get_text() for i in text_p]
        job_content = list(filter(None,job_content))
        job_content =[i for i in job_content if i != '\n']
    except:
        job_content= None #["Issue"] #Srbuhi
    
    try:
        # Professional skills
        prof_skills=page.find_all("div", class_="soft-skills-list clearfix")[0].find_all("span",class_="soft-skills-delete")
        prof_skills=[i.get_text() for i in prof_skills]
    except:
        prof_skills= None #["Issue"] #Srbuhi
    
    try:
        # Soft skills
        soft_skills=page.find_all("div", class_="soft-skills-list clearfix")[1].find_all("span",class_="soft-skills-delete")
        soft_skills=[i.get_text() for i in soft_skills]
    except:
        soft_skills= None #["Issue"] #Srbuhi
    
    website_name="staff_am"
    feat_list={"Company":[company],"Industry":[indust],"Page Views":[page_views],"Active Jobs":[active_jobs], 
               "Job History":[job_history],"Job Views": [job_views], "Job Title":[job_title],"Deadline":[dead_list],
               "Employment Term": [imp_term],"Job Type":[job_type],"Job Category":[job_cat],"Job Location":[job_loc],
               "Salary":[job_sal],"Job Description":[job_descri],"Qualification":[q_lists],"Job Content":[job_content],
               "Prof Skills":[prof_skills],"Soft Skills":[soft_skills],"Job Link":[url], "Scrape_Date":[scrape_date], "Website":''}#Srbuhi
    feat_list=pd.DataFrame.from_dict(feat_list)
    
    return feat_list
url="https://staff.am/en/jobs"

page=UrlRequest(url)

term=re.findall("\d+",page.find_all(attrs={"id":"jobsfilter-job_term"})[0].get_text())
JobCount=sum([int(i) for i in term])


url1="https://staff.am/en/jobs?page="
url2="&per-page=50"


JobLinks=[]

for i in range(1,JobCount//50+2):
    page=UrlRequest(url1+str(i)+url2) 
    links = [i.find("a")["href"] for  i in page.find_all("div",class_="web_item_card")]
    JobLinks.extend(links)

# try:
#     assert(len(JobLinks)==JobCount)
# except:
#     logging.exception("JobLinksCount exception")

logging.debug("Actual number of links "+str(len(JobLinks)))
logging.debug("Links count on the website: "+str(JobCount))
JobLinks = list(set(JobLinks).difference(old_links))

print("Number of new links found in staff_am:,",len(JobLinks),JobLinks)#Srbuhi



logging.debug("New links found in staff_am: "+str(JobCount))
if len(JobLinks) > 1:
    # Getting information from all pages
    job_info=[]
    for url in JobLinks:
        job_info.append(get_job_info(url))
        # Creating a dataframe
    DataFinal=pd.concat(job_info)
    DataFinal = DataFinal.reset_index(drop=True)

    records = json.loads(DataFinal.T.to_json()).values()
    staff_am.insert_many(records)
    logging.debug("number of jobs in the data: " + str(staff_am.count_documents(filter={})))
else:
    logging.debug("No new links were found :(")