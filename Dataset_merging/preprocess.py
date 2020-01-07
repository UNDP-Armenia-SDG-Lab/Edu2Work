import numpy as np
from datetime import datetime
import json
import re
from utils import *
from config import *


#reading all collections
full=read_from_mongoDB("labor_market","full_am")
hr_am=read_from_mongoDB("labor_market","hr_am")
ijob_am=read_from_mongoDB("labor_market","ijob_am")
job_am=read_from_mongoDB("labor_market","job_am")
myjob_am=read_from_mongoDB("labor_market","myjob")
staff_am=read_from_mongoDB("labor_market","staff_am")
jobfinder=read_from_mongoDB("labor_market","jobfinder_am")
career_center2=read_from_mongoDB("labor_market","career_center2")
worknet=read_from_mongoDB("labor_market","worknet_am")
careercenter=read_from_mongoDB("labor_market","careercenter")



#STAFF.am 
#getting rid of exceptions scraped as "['Issue']"
for index, row in staff_am.iterrows():
    for i in staff_am.columns:
        if row[i]=="['Issue']":
            row[i]=""

#Merging Prof Skills and Soft Skills into one
staff_am["Prof Skills"]=staff_am["Prof Skills"].apply(lambda x:str(x).replace("[","").replace("]",""))
staff_am["Soft Skills"]=staff_am["Soft Skills"].apply(lambda x:str(x).replace("[","").replace("]",""))
staff_am["Skills"]=staff_am.loc[:,["Prof Skills","Soft Skills"]].sum(axis=1)

#Renaming features to express the exact meaning presented on the website
staff_am.rename(columns={"Job Category":"Employment Type","Job Type":"Job Industry","Industry":"Company Industry"}, inplace=True)

#making strings, getting rid of square brackets
staff_am["Job Content"]=staff_am["Job Content"].fillna("").apply(lambda x:"".join(x).replace("[","").replace("]",""))
staff_am["Qualification"]=staff_am["Qualification"].fillna("").apply(lambda x:"".join(x).replace("[","").replace("]",""))
staff_am["Salary"]=staff_am["Salary"].fillna("").apply(lambda x:"".join(x).replace("[","").replace("]",""))
#making full link
staff_am["Job Link"]=staff_am["Job Link"].apply(lambda x:"https://staff.am"+x)

#Dropping the noted features
staff_am.drop(["Website","Active Jobs","Job Description","Job History", "Prof Skills","Soft Skills", "Page Views", "Qualification"], axis=1, inplace=True)
#filling empty string with None
staff_am=frame_intitial_clean(staff_am)
#adding website name
staff_am["Website_name"]="staff.am"


#HR.am

#merging Prof and Soft skills into Skills
hr_am["Prof Skills"]=hr_am["Prof Skills"].apply(lambda x:str(x).replace("[","").replace("]",""))
hr_am["Soft Skills"]=hr_am["Soft Skills"].apply(lambda x:str(x).replace("[","").replace("]",""))
hr_am["Skills"]=hr_am.loc[:,["Prof Skills","Soft Skills"]].sum(axis=1)
hr_am.drop(["Applications","Benefits", "Job Type","Languages","Education","Prof Skills","Soft Skills"],axis=1,inplace=True)
#renaming respective columns to match actual meaning
hr_am.rename(columns={ "Job Description":"Job Content","Qualification":"Job Industry","Page Views":"Job Views"},inplace=True)
#making Job Industry column consistent, by keeping only the first job industry
clean=clean_str(hr_am)
hr_am["Job Industry"]=clean
#adding website name
hr_am["Website_name"]="hr.am"
#filling empty string with None
hr_am=frame_intitial_clean(hr_am)


#IJOB.am

#renaming columns, to keep consistency
ijob_am.rename(columns={"Categories":"Job Industry","Close Date":"Deadline","Date Posted":"Post Date","Description":"Job Content",
                    "Link":"Job Link","Location":"Job Location","Organization":"Company","Position":"Job Title","ScrapeDate":"Scrape_Date",
                    "Term Time":"Employment Type"}, inplace=True)

#dropping missing values
ijob_am.dropna(axis=0,subset=["Job Content"],inplace=True)
#resetting index
ijob_am.reset_index(drop=True, inplace=True)
#standardizing job location
ijob_am["Job Location"]=ijob_am["Job Location"].apply(lambda x:x.split(",")[0])
#Changing post date and deadline to have datetime values
ijob_am["Post Date"]=ijob_am["Post Date"].apply(lambda x:x.replace("Posted","").replace("ago",""))
ijob_am["Post Date"]=ijob_am["Post Date"].apply(date_conv)
ijob_am["Post Date"]=ijob_am["Post Date"].apply(lambda x: pd.Timedelta(x,unit='D'))
ijob_am["Post Date"]=ijob_am["Scrape_Date"]-ijob_am["Post Date"]
ijob_am["Deadline"]=ijob_am["Deadline"].apply(timestamp_to_datetime)
#adding website name
ijob_am["Website_name"]="ijob.am"
#filling empty string with None
ijob_am=frame_intitial_clean(ijob_am)


#FULL.am

#dropping unnecessary columns
full.drop(["Education","Experience","Preferred Age","Preferred Gender","Organization Type"], axis=1, inplace=True)
#renaming columns, to keep consistency 
full.rename(columns={"Categories":"Job Industry","Description":"Job Content","Link":"Job Link","Position":"Job Title",
                    "Organization":"Company","Scrape Date":'Scrape_Date',"Term Time":"Employment Type","Location":"Job Location"}, inplace=True)

#extracting salary values if they're in list
for i in range(0,len(full)):
    if type(full["Salary"][i])==list:
        full["Salary"][i]="".join(str(full["Salary"][i]))
full.Salary=full.Salary.apply(lambda x:str(x).replace("[","").replace("]",""))
#making Post date a datetime
full["Post Date"]=full["Post Date"].apply(lambda x: pd.to_datetime(x))
#mapping full and part time jobs
full["Employment Type"]=full["Employment Type"].map({"Լրիվ աշխատանքային օր":"Full time","Ոչ լրիվ աշխատանքային օր":"Part time"})
#changing Location name if only district name is noted
full["Job Location"]=full["Job Location"].apply(location_change)
#adding website name
full["Website_name"]="full.am"
#filling empty string with None
full=frame_intitial_clean(full)


#JOB.am

#making full list 
job_am.urls="https://job.am/"+job_am.urls
#renaming respective columns
job_am.rename(columns={"Industry":"Job Industry","Qualification":"Job Content","date_added":"Scrape_Date",
                      "urls":"Job Link","Post date":"Post Date"}, inplace=True)

#removing square brackets
job_am["Job Content"]=job_am["Job Content"].apply(lambda x:x.replace("[","").replace("]",""))
#filling empty string with None
job_am=frame_intitial_clean(job_am)
#converting scrape date to datetime, initially is a 13-digit number making no sense
job_am["Scrape_Date"]=job_am["Scrape_Date"].apply(timestamp_to_datetime)

#converting post date  and deadline to datetime
job_am["Post Date"]=pd.to_datetime(job_am["Post Date"])
job_am["Deadline"]=pd.to_datetime(job_am["Deadline"])

#adding website name
job_am["Website_name"]="job.am"

#MYJOB.am

#combining Qualifications and responsibilities
myjob_am["Job Content"]=myjob_am.loc[:,["Qualifications","Responsibilities"]].sum(axis=1)

#dropping non-neccessary columns
myjob_am.drop(["Additional","Qualifications","Responsibilities","UniqueID"], axis=1, inplace=True)

#renaming columns to keep consistency 
myjob_am.rename(columns={"Address":"Job Location","Category":"Job Industry","Close Date":"Deadline","Link":"Job Link",
                "Open Date":"Post Date","Organization":"Company","Position":"Job Title","Scrape Date":"Scrape_Date"},inplace=True )

#converting url to complete one
myjob_am["Job Link"]=myjob_am["Job Link"].apply(url_complete)

#converting post date and deadline to datetime
myjob_am["Deadline"]=pd.to_datetime(myjob_am["Deadline"])
myjob_am["Post Date"]=pd.to_datetime(myjob_am["Post Date"])
#adding website name
myjob_am["Website_name"]="myjob.am"
#cleaning job location
myjob_am["Job Location"]=myjob_am["Job Location"].apply(lambda x:str(x).split(",")[0])
#adding website name
myjob_am["Website_name"]="myjob.am"
#filling empty string with None
myjob_am=frame_intitial_clean(myjob_am)


#JOBFINDER.am

#dropping unnecessary columns
jobfinder.drop(['Active Jobs', 'Industry', 'Job History', 'Job Type', 'Job Views',
                'Page Views','Age','Experience','Preferred_gender','About','Procedure'], axis=1, inplace=True)
#renaming columns to keep consistency 
jobfinder.rename(columns={"Employment Term":"Employment Type","Job Category":"Job Industry","Job Description":"Job Content",
                 "Scrape Date":"Scrape_Date"}, inplace=True)
#merging Job content and qualification
jobfinder["Job Content"]=jobfinder["Job Content"].apply(lambda x:str(x))+jobfinder["Qualification"].apply(lambda x:str(x))
#droppping Qualification after merging
jobfinder.drop('Qualification', axis=1, inplace=True)

#replace -NAs with None
jobfinder=frame_intitial_clean(jobfinder)
#changing job location
jobfinder["Job Location"]=jobfinder["Job Location"].apply(lambda x: x.split(",")[1])

#adding website name
jobfinder["Website_name"]="jobfinder.am"



#WORKNET.am

#dropping unnecessary columns
worknet.drop(["Gender","Job Title","Job Type","website"], axis=1, inplace=True)

#dropping missing values
worknet.dropna(axis=0,subset=["Post Date"], inplace=True)

#renaming columns to keep consistency 
worknet.rename(columns={"Added":"Scrape_Date","Employment Term":"Employment Type","Job Description":"Job Content",
               "Job Title (en)":"Job Title"}, inplace=True)
#standardizing scrape date
worknet["Scrape_Date"]=worknet["Scrape_Date"].apply(lambda x:str(x))
worknet["Scrape_Date"]=worknet["Scrape_Date"].apply(timestamp_to_datetime)
#standardizing job location
worknet["Job Location"]=worknet["Job Location"].apply(lambda x:str(x).split(",")[0])
#changing post date to str
worknet["Post Date"]=worknet["Post Date"].apply(lambda x:str(x).replace("առաջ",""))
worknet["Post Date"]=worknet["Post Date"].apply(lambda x:x.split(","))

worknet["Post Date"]=worknet["Post Date"].apply(post_date_changer)

worknet["Post Date"]=worknet["Post Date"].apply(post_date_standardizer)

worknet["Post Date"]=worknet["Scrape_Date"]-pd.TimedeltaIndex(worknet["Post Date"], unit='D')
#replacing empty strings and string 'None'-s with None
worknet=frame_intitial_clean(worknet)
#adding website name
worknet["Website_name"]="worknet.am"


#CAREERCENTER-yahoogroups

#dropping unnecessary columns
career_center2.drop("number", axis=1, inplace=True)
#renaming columns to keep consistency 
career_center2.rename(columns={"url":"Job Link"}, inplace=True)
#standartizing post dates
career_center2["Post Date"]=pd.to_datetime(career_center2["Post Date"].apply(date_maker))
#adding website name
career_center2["Website_name"]="careercenter.am"
#removing observations with no separation of company and industry
career_center2=career_center2[career_center2["Job Title"].str.contains("/")]
#separating job title and company and making them lower
career_center2["Job Title"]=career_center2["Job Title"].apply(job_title_separator)
career_center2["Company"]=career_center2["Job Title"].apply(company_splitter)
career_center2["Job Title"]=career_center2["Job Title"].apply(job_title_splitter)
career_center2["Job Title"]=career_center2["Job Title"].apply(job_title_cleaner)

#filling empty string with None
career_center2=frame_intitial_clean(career_center2)


#CAREERCENTER's modified website

#filling empty string with None
careercenter=frame_intitial_clean(careercenter)
careercenter["Deadline"]=careercenter["Deadline"].apply(date_transformer)
careercenter["Job Location"]=careercenter["Job Location"].apply(lambda x:x.split(',')[0].lower())
#Merging all in one final dataframe

final_merged=pd.concat([staff_am,hr_am,job_am,jobfinder,myjob_am,worknet,full,career_center2,ijob_am,careercenter], ignore_index=True, sort=True)

final_merged=final_merged[final_merged["Job Content"].apply(lambda x:type(x)!=int)]
#dropping observations having NA in Job Content
final_merged.dropna(axis=0, subset=["Job Content"],inplace=True)
final_merged.drop("_id",axis=1,inplace=True)
final_merged.replace(r'\s+|\\n', ' ', regex=True,inplace=True)
final_merged.info()
#dropping "_id" column, to be able to load to Mongo
load_to_mongoDB("labor_market","final_merged",final_merged)

print("You're done, Master!")