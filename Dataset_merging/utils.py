import pandas as pd 
import config
import numpy as np
from datetime import  datetime



def na_filler(dataframe):
    """
    Standartization of missing values:replacement of empty strings and numpy NaN's with None
    Args:
        dataframe: dataframe that has empty strings, None
    Returns:
        dataframe: Dataframe with standartized missing values
    """
    dataframe=dataframe.replace(r'^\s*$', np.nan, regex=True)
    dataframe=dataframe.replace("-N/A", np.nan).replace("'None'", np.nan).replace("NA", np.nan)
    dataframe=dataframe.where(dataframe.notna(),None)
    return dataframe


def location_change(x): #aka changer
    '''Function changing job location name 
    to Yerevan+Yerevan district, if only districts are noted'''
    if x in config.districts:
        changed ='Երևան' + " "+x
    return x


def timestamp_to_datetime(x): #aka converter 
    if len(str(x))==13:
        x=str(x)[:-3]
        x=datetime.fromtimestamp(int(x))
    return x

def url_complete(url): #aka changer
    url_complete=None
    try:
        if "https://www.myjob.am/" not in url:
            url_complete ="https://www.myjob.am/"+url
    except:
        url_complete = url
    return url_complete

def frame_intitial_clean(frame):
    frame_clean = None
    try:
        frame_clean=frame.replace(r'^\s*$', np.nan, regex=True)
        frame_clean=frame_clean.replace("NA", np.nan)
        frame_clean=frame_clean.where(myjob_am.notna(),None)
    except RuntimeError as e:
        print("frame initial cleaning failed with error: ", e)
        return frame_clean
    return frame_clean


def date_conv(i): #
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
    '''summing days of post date and making values integer'''
    if len(x)>=2:
        x=int(sum(x))
    elif type(x) is list:
        x=x[0]
    return x


def date_maker(x):
    '''adding 2019 to dates missing it-meaning the date was from current 2019 year '''
    if len(x.split(","))!=2:
        x+=" 2019"
    return x



def date_transformer(x):
    '''function leaving only dates in careercenter'''
    try:
        if x[0].isdigit():
            pass
        else:
            x=None
    except:
        pass
    return x





#look at post_date change rbased on frame_intitial_clean!!!! the one with post etc