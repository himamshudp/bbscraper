#!/usr/bin/env python
# coding: utf-8

# In[5]:


from bs4 import BeautifulSoup as bs4
import requests
import pandas as pd
import time
from datetime import datetime
import pytz
import json
import os
import warnings
warnings.filterwarnings("ignore")
import re
from selenium import webdriver
import schedule
from firebase_admin import storage
import time
from url_cat_list import *


# In[15]:


def Bigbascket_scraper():
 
    # headless chorme
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    
    #for cloud
    chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
    driver = webdriver.Chrome(executable_path=os.environ.get("CHROMEDRIVER_PATH"), chrome_options=chrome_options)
    
    #forlocal machine
    #chromedriver = 'E://chromedriver.exe'
    #driver = webdriver.Chrome(executable_path=chromedriver, chrome_options=chrome_options)
    url_scrap()
    url_selected = pd.read_csv(r'bb_selected_link.csv')
    product_url =  url_selected['link'].tolist()
    p_cat=  url_selected['category_name'].tolist()
    product_url_try = product_url[:2]
       
    bb_df = pd.DataFrame(columns=['Product_category', 'Product_name', 'O_price', 'D_price','product_quantity', 'Brand_name'])

    for url in product_url_try:
        driver.get(url)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight/3.5);window.scrollTo(0, document.body.scrollHeight/3.7);")
        time.sleep(30)  # 2 Sec for ssh
        html = driver.execute_script("return document.documentElement.outerHTML")
        soup = bs4(html, 'html.parser')
        # print(soup)
        print(url)

        br_name = soup.findAll("h6", {"ng-bind":"vm.selectedProduct.p_brand"})
        pr_name = soup.findAll("a", {"ng-bind":"vm.selectedProduct.p_desc"})
        or_price = soup.findAll("span", {"class":"mp-price ng-scope"})
        disc_price = soup.findAll("span", {"class":"discnt-price"})
        p_qty = soup.findAll("div", {"class":"col-sm-12 col-xs-7 qnty-selection"})
        #p_cat=soup.findAll("div",{"class":"dp_headding"})

        b_name=[]
        p_name=[]
        o_price =[]
        d_price =[]
        qty =[]
        p_c=[]

        for bpoints,ppoints,opoints,dpoints,qpoints,pcatpoints in zip(br_name,pr_name,or_price,disc_price,p_qty,p_cat):
            b_name.append(str(bpoints.text))
            p_name.append(str(ppoints.text))
            o_price.append(str(opoints.text))
            d_price.append(str(dpoints.text))
            qty.append(str(qpoints.text))
            p_c.append(str(pcatpoints))

        #crete dictionary
        dictionary={}
        keys=['Product_category', 'Product_name', 'O_price', 'D_price','product_quantity', 'Brand_name']
        values=[p_c,p_name,o_price,d_price,qty,b_name]
        bigbasket_df_al= dict(zip(keys,values))
        df=pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in bigbasket_df_al.items() ]))
        result = bb_df.append(df)
        bb_df=result
    print(bb_df)
    #bb_df.to_csv(r'bb_scaped_data_30nov.csv', index = False)
    #df=pd.read_csv('bb_scaped_data_30nov.csv')
    df_copy = bb_df
    #convert all colms to lower case
    df_copy[df_copy.columns] = df_copy.apply(lambda x: x.astype(str).str.lower())
    #replace new line with space
    df_copy= df_copy.replace('\n','', regex=True)
    df_copy['weight']= df_copy['product_quantity'].str.split('-').str[0]
    df_copy['weight']= df_copy['weight'].str.strip()
    df_copy['weight'] = df_copy['weight'].replace({'x':'#'}, regex=True)
    df_copy= df_copy[~df_copy['weight'].str.contains('\#')]
    df_copy= df_copy[~df_copy['weight'].str.contains('combo|pack of')]
    df_copy= df_copy[~df_copy['weight'].str.contains('\(')]
    df_copy['scale']= df_copy['weight'].astype(str).str.split(' ').str[1]
    df_copy['scale'] = df_copy['scale'].replace({'pcs|pellets|sachets|pouch|cup':'pc'}, regex=True)
    df_copy['scale'] = df_copy['scale'].replace({'gm':'g'}, regex=True)
    df_copy['scale'] = df_copy['scale'].replace({'lt|ltr':'l'}, regex=True)
    df_copy['App_brand'] = df_copy['Brand_name']
    df_copy["Organic_flag"] = pd.np.where(df_copy['Product_name'].str.contains('organic'),'organic','non_organic')
    df_copy['O_price']=df_copy.O_price.str.extract('(\d+)')
    df_copy['D_price']=df_copy.D_price.str.extract('(\d+)')
    df_copy['Weight']=df_copy.product_quantity.str.extract('(\d+)')
    df_copy['Unit']= df_copy['scale']
    df_copy['Website']='BigBascket'

    prod_name_list_dmart=df_copy["Product_name"].tolist()
    #Reading the standard ingredient list
    df_col=pd.read_excel("check.xlsx",sheet_name='Details')
    #lowering the data to maintaing uniformity of dataset
    df_col=df_col.apply(lambda x: x.astype(str).str.lower())
    # connverting data  columns to list for further processing
    sorted_name_list=df_col["Sorted Name"].tolist()
    product_category_list=df_col["Product_Category"].tolist()

    app_name=[]
    for j in prod_name_list_dmart:
        list1=[]
        for i in sorted_name_list:
            try:
                    if i in j:
                        list1.append(i)               
            except:
                        list1=[]
        app_name.append(list1) 
    #fill [] with null keyword   
    index=[i for i,x in enumerate(app_name) if x ==[]]
    for i in range(len(app_name)):
         for j in index:
                if i==j:
                    app_name[i]='Null'
    #take max length words from list
    app_name1 = []
    for name in app_name:
        name1 = max((name for name in name if name), key=len)  
        app_name1.append(name1)
    df_copy['App_name'] = app_name1

    app_name_dmart=df_copy["App_name"].tolist()
    app_cat=[]
    for i in range(len(app_name_dmart)):
        list2=[]
        for j in range(len(sorted_name_list)):
            try:
                    if app_name_dmart[i] == sorted_name_list[j] :
                        list2.append(product_category_list[j])               
            except:
                        list2=[]
        app_cat.append(list2) 
    #fill [] with null keyword   
    index=[i for i,x in enumerate(app_cat) if x ==[]]
    for i in range(len(app_cat)):
         for j in index:
                if i==j:
                    app_cat[i]='Null'
    #take max length words from list
    app_cat1 = []
    for name in app_cat:
        name1 = max((name for name in name if name), key=len)  
        app_cat1.append(name1)
    df_copy['App_category'] = app_cat1
    cols = df_copy.select_dtypes(['object']).columns
    df_copy[cols] = df_copy[cols].apply(lambda x: x.str.strip())
    df_copy[cols] = df_copy[cols].apply(lambda x: x.str.capitalize())
    df_copy.to_csv(r'bb_30nov_all.csv',index = False)
    df_bb= df_copy[['Product_name', 'App_brand', 'O_price','D_price','Weight','Unit', 'App_name','Organic_flag','App_category','Website']]
    df_bb.to_csv(r'bb_30_nov.csv',index = False)
    print("bb_30nov.csv created")

    obj = df_bb.to_json(orient="split")
    parsed = json.loads(obj)
    data= json.dumps(parsed, indent=4)
    #firebase
    firebase_app = None
    PROJECT_ID = 'webdatascaper'

    IS_EXTERNAL_PLATFORM = True # False if using Cloud Functions

    #if firebase_app:
    #    return firebase_app

    import firebase_admin
    from firebase_admin import credentials

    if IS_EXTERNAL_PLATFORM:
        cred = credentials.Certificate('webdatascaper-firebase-adminsdk-r9721-6539dd32b9.json')
    else:
        cred = credentials.ApplicationDefault()

    firebase_app = firebase_admin.initialize_app(cred, {'storageBucket': f"{PROJECT_ID}.appspot.com"})

    name = '/bigbascket/bb_1_dec.json'
    bucket = storage.bucket()
    blob = bucket.blob(name)
    blob.upload_from_string(json.dumps(data, indent=2))
    data=json.loads(blob.download_as_string())
    print("file Uploaded")


# In[17]:


Bigbascket_scraper()


# In[ ]:





# In[ ]:




