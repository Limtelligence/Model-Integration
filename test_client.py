## GV Model integration through API
import os
from dotenv import load_dotenv
import requests
import requests.auth
import json
import pandas as pd 
import numpy as np
import time
import random

#Druid
DRUIDURL= os.getenv('DRUIDURL')
ASSETID= os.getenv('ASSETID')
INTERVAL= os.getenv('INTERVAL')

##Get Token
TOKENURL= os.getenv('TOKENURL')
CLIENTID= os.getenv('CLIENTID')
CLIENTSECRET= os.getenv('CLIENTSECRET')
USERNAME= os.getenv('USERNAME')
PASSWORD= os.getenv('PASSWORD')

##Post Score
SCOREURL= os.getenv('SCOREURL')


## assetId / 파라미터 physical_name / Interva(10분)에 따른 Druid 데이터 조회
def getDataFromDruid(ASSETID,INTERVAL):
    url = DRUIDURL
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    query = """SELECT * FROM \"druid\".\"asset_trace_asset_ss_model_01\" 
    where assetId='{}'  
    and __time between  CURRENT_TIMESTAMP - INTERVAL '{}' MINUTE 
    and CURRENT_TIMESTAMP""".format(ASSETID,INTERVAL)
    data = {"query" : query}
    res = requests.post(url, data=json.dumps(data), headers=headers)

    res = json.loads(res.content)
    df = pd.DataFrame(res)
    print(df.head())
    return(df)


##Modeling
def simpleModeling():
    score=random.random()
    return(score)


def getToken():

    auth =(CLIENTID, CLIENTSECRET)
    data = {'grant_type': 'password','username': USERNAME, 'password': PASSWORD}
    res = requests.post(TOKENURL, data=data, verify=False, allow_redirects=False, auth=auth)

    if(res.status_code!=200):
        print("Get Token Error")
        print(res.text)
    else:
        return(json.loads(res.text)['access_token'])



def postScore(token,score):
    headers = {'Content-type':'application/json', 'Authorization' : 'Bearer '+token}
    data = {"timestamp":round(time.time()*1000),
        "assetId": "64fd18b7-4f4e-48be-aa8b-f00666392a43",
        "modelType": "EXT_v1.0",
        "fromTimestamp": 1591847815917,
        "toTimestamp": 1591847815917,
        "assetScore": score}

    res = requests.post(SCOREURL , headers = headers , data = json.dumps(data))
    if(res.status_code!=201):
        print("Post Score Error")
        print(res.text)
    else:
        print("Score Sent")



if __name__ == "__main__":

    load_dotenv()

    getDataFromDruid(ASSETID,10)
    score=simpleModeling()
    token=getToken()

    postScore(token,score)
