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


## assetId / 파라미터 physical_name / Interva(10분)에 따른 Druid 데이터 조회
def getDataFromDruid(TABLENAME,ASSETID,INTERVAL):
    url = DRUIDURL
    table = TABLENAME
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    query = """SELECT * FROM \"druid\".\"{}\" 
    where assetId='{}'  
    and __time between  CURRENT_TIMESTAMP - INTERVAL '{}' MINUTE 
    and CURRENT_TIMESTAMP""".format(TABLENAME,ASSETID,INTERVAL)
    print(query)
    data = {"query" : query}
    res = requests.post(url, data=json.dumps(data), headers=headers)

    res = json.loads(res.content)
    df = pd.DataFrame(res)
    print(df.head())
    return(df)


##Modeling
def simpleModeling(data):
    input = data
    score =random.random()
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
        "assetId": ASSETIDGV,
        "modelType": "EXT_v1.0",
        "modelName": MODELNAME,
        "fromTimestamp": 0000000000000,
        "toTimestamp": 0000000000000,
        "assetScore": score}

    res = requests.post(SCOREURL , headers = headers , data = json.dumps(data))
    if(res.status_code!=201):
        print("Post Score Error")
        print(res.text)
    else:
        print("Score Sent")
        print(res.text)



if __name__ == "__main__":
    #Load Global Env
    load_dotenv()
    
    #Druid
    DRUIDURL= os.getenv('DRUIDURL')
    ASSETID= os.getenv('ASSETID')
    INTERVAL= os.getenv('INTERVAL')
    TABLENAME= os.getenv('TABLENAME')

    ##Get Token
    TOKENURL= os.getenv('TOKENURL')
    CLIENTID= os.getenv('CLIENTID')
    CLIENTSECRET= os.getenv('CLIENTSECRET')
    USERNAME= os.getenv('USERNAME')
    PASSWORD= os.getenv('PASSWORD')

    ##Post Score
    ASSETIDGV= os.getenv('ASSETIDGV')
    SCOREURL= os.getenv('SCOREURL')
    MODELNAME= os.getenv('MODELNAME')

    ##Main
    data= getDataFromDruid(TABLENAME,ASSETID,10)
    score= simpleModeling(data)
    token= getToken()

    postScore(token,score)
