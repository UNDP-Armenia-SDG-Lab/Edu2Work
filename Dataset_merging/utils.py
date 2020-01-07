import pandas as pd 
import config
import numpy as np
import ast
from datetime import  datetime
from pymongo import MongoClient
import json
import re


def read_from_mongoDB(database, collection):
    ''' Reading from MongoDB database a collection and converting it into dataframe
        Args:
            database, collection-names of db and collection of interest
        Returns:
            pandas dataframe'''
    client = MongoClient('localhost', 27017)
    db = client[database]
    coll = db[collection]
    coll_df = pd.DataFrame(list(coll.find({})))
    return coll_df

def load_to_mongoDB(database, collection, dataframe): 
    '''Function loading dataframe to db's collection of interest
        Args:
            database, collection, dataframe -respective names
        Returns:
            None, loads to MongoDB the dataframe.'''
    client = MongoClient()
    col = client[database][collection]
    records = json.loads(dataframe.T.to_json()).values()
    col.insert_many(records)

def get_data_excel(db_name,col_name):
    '''Function getting data in excel format from db
        Args:
            database, collection-respective names
        Returns:
            File in excel format, saves in local directory from where this .py is run'''
    client=MongoClient('localhost', 27017)
    db=client[db_name]
    col=db[col_name]
    col_data=pd.DataFrame(list(col.find({})))
    col_data.to_excel(col_name+"_upd.xlsx")
    #return col_data


def frame_intitial_clean(frame):
    """ Standartization of missing values:replacement of empty strings and numpy NaN's with None
        Args:
            dataframe: dataframe that has empty strings, None
        Returns:
            dataframe: Dataframe with standartized missing values"""
    frame_clean = None
    try:
        frame_clean=frame.replace(r'^\s*$', np.nan, regex=True)
        frame_clean=frame_clean.replace("-N/A", np.nan).replace("'None'", np.nan).replace("NA", np.nan)
        frame_clean=frame_clean.where(frame_clean.notna(),None)
    except RuntimeError as e:
        print("frame initial cleaning failed with error: ", e)
        return frame_clean
    return frame_clean



def location_change(x): #aka changer
    '''Function changing job location name
    Args:
        string, Job Location
    Returns:
        Job location name, if only Yerevan districts are noted, makes them Yerevan+district'''
    if x in config.districts:
        changed ='Երևան' + " "+x
    return x


def timestamp_to_datetime(x):
    #aka converter 
    ''' Function converting given encoded integer of length 13 into readable date format
    Args:
        integer, encoded timestamp 
    Returns:
        date in datetime format'''
    if len(str(x))==13:
        x=str(x)[:-3]
        x=datetime.fromtimestamp(int(x))
    return x

def url_complete(url): #aka changer
    ''' Function adding main url-myjob.am, to urls lacking it
    Args:
        url, string 
    Returns:
        full url
    Note: is applicable only to myjob.am dataframe's urls '''
    url_complete=None
    try:
        if "https://www.myjob.am/" not in url:
            url_complete ="https://www.myjob.am/"+url
    except:
        url_complete = url
    return url_complete


def clean_str(df):
    #function keeping only 1st element of 0rd tuple,applicable to only hr_am's "Job Industry" 
    ans = []
    try:
        for index,row in df.iterrows():
            if row['Job Industry']:
                literal = row['Job Industry']
                if type(row['Job Industry']) is str:
                    literal = ast.literal_eval(literal)
                if not literal:
                    ans.append(None)
                    continue
                                    
                clean_st = " ".join(literal[0][1])
                ans.append(clean_st)
            else:
                ans.append(None) 
    except RuntimeError as e:
        print(e)
        return ans
    return ans



def date_conv(i):
    '''Converting given unstructured text to days 
    in order to further calculate post date
    Args:
        date in string format, i.e. "5 months"
    Returns:
        integer representing number of days
    Note: is applicable mainly to ijob.am's Post Dates'''
    if "months" in i:
        i=int(i.replace("months","").strip())
        i=31*i
    elif "month" in i:
        i=int(i.replace("month","").strip())
        i=31*i    
    elif "weeks" in i:
        i=int(i.replace("weeks","").strip())
        i=7*i 
    elif "week" in i:
        i=int(i.replace("week","").strip())
        i=7*i
    elif "days" in i:
        i=int(i.replace("days","").strip())
    elif "day" in i:
        i=int(i.replace("day","").strip())
    elif "hour" or "hours" or "min" or "mins" in i:
        i=0
    return i


def post_date_changer(text): #aka post_conv
    '''Converting given unstructured text to days 
    in order to further calculate post date
    Args:
        date in string format, i.e. "5 ամիս"
    Returns:
        integer representing number of days
    Note: is applicable mainly to worknet.am's Post Dates'''

    for i in range(0,len(text)):  
        try:
            if "տարի" in text[i]:
                text[i]=text[i].replace("տարի","")
                text[i]=int(text[i])*365
            elif "ամիս" in text[i]:
                text[i]=text[i].replace("ամիս","")
                text[i]=int(text[i])*31
            elif "Այսօր" in text[i]:
                text[i]=0
            elif 'օր' in text[i]:
                text[i]=text[i].replace('օր',"")
                text[i]=int(text[i])
            elif 'Երեկ'in text[i]:
                text[i]=1
        except:
            continue
    return text


def post_date_standardizer(x):
    '''Summing days of post date and making values integers
    Args:
        number in string format
    Returns:
        integer representing number of days
    Note: is applicable mainly to worknet.am's Post Dates'''
    if len(x)>=2:
        x=int(sum(x))
    elif type(x) is list:
        x=x[0]
    return x


def date_maker(x):
    '''adding 2019 to dates missing it-meaning the date was from current 2019 year 
    Args:
        date in string format, i.e. 17 April
    Returns:
        full date, with year
    Note: is mainly applicable to careercenter's yahoo-based dataset's Post Date'''
    if len(x.split(","))!=2:
        x+=" 2019"
    return x



def date_transformer(x):
    '''function leaving only dates in upgraded careercenter's Deadlines
    Args:
        date in string format
    Returns date in string format, leaving out dates with no digits,
        mainly noted as "ASAP", or "Whenever 4 people form a group"'''
    try:
        if x[0].isdigit():
            pass
        else:
            x=None
    except:
        pass
    return x

def job_title_separator(text):
    '''Splitting given job title to keep only job title and company name.
    i.e.'14020JOB - Executive Assistant / Ameriabank '
    Args:
        job title with unique job number
    Returns:
        job title and company name
    Note: is applicable mainly to career_center2's Job Titles'''
    split=""
    try:
        if len(text.split(" - "))>=2:
            split=text.split(" - ")[1]
        else:
            split=text
    except:
        pass
    return split

def company_splitter(text):
    '''Splitting companies from given job title
    i.e.'Executive Assistant / Ameriabank '
    Args:
        job title with '/' separator
    Returns:
        splitted company name
    Note: is applicable mainly to career_center2's Job Titles'''
    comp=""
    try:
        if len(text.split("/"))>=2:
            comp=text.split("/")[-1]
        else:
            comp=text
    except:
        pass
    return comp

def job_title_splitter(text):
    '''Splitting companies from given job title
    i.e.'Executive Assistant / Ameriabank '
    Args:
        job title with '/' separator
    Returns:
        splitted job title,keeping everything except the last element of the splitted list
    Note: is applicable mainly to career_center2's Job Titles'''
    comp=""
    try:
        if len(text.split("/"))>=2:
            comp="".join(text.split("/")[:-1])
        else:
            comp=text
    except:
        pass
    return comp

def job_title_cleaner(text):
    '''Cleaning job title from redundant digits at the beginning of job title.
    i.e.'680Operations Manager'
    Args:
        job title
    Returns:
        job title without digits at its start
    Note: is applicable mainly to career_center2's Job Titles'''
    clean=""
    try:
        clean=re.sub(r"(^\d+)", "",text).lower()
    except:
        clean=text
    return clean
            