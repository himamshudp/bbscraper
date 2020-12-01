#!/usr/bin/env python
# coding: utf-8

# In[18]:


from bs4 import BeautifulSoup as bs4
import requests
import pandas as pd
import time
import os
import warnings
warnings.filterwarnings("ignore")
import re
from selenium import webdriver

def url_scrap():
 
    # headless chorme
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")

    #forlocal machine
    chromedriver = 'E://chromedriver.exe'
    driver = webdriver.Chrome(executable_path=chromedriver, chrome_options=chrome_options)

    #for cloud
    #chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
    #driver = webdriver.Chrome(executable_path=os.environ.get("CHROMEDRIVER_PATH"), chrome_options=chrome_options)
    
    url="https://www.bigbasket.com/product/all-categories/"
    driver.get(url)
    time.sleep(8)  # 2 Sec for ssh
    html = driver.execute_script("return document.documentElement.outerHTML")
    soup = bs4(html, 'html.parser')
    # print(soup)
    print(url)
    category_name = []
    extensions = []
    url_list = []
    domain="https://www.bigbasket.com/"
    for points in soup.findAll("a",{"append-parameter":"?nc=nb"}):
        pcategory=str(points.text)
        category_name.append(pcategory)
        url_ext=(re.findall(r'"(.*?)"', str(points)))
        extensions.append(url_ext[1])             
    for points in extensions:
        url=domain+str(points)
        url_list.append(url)
    df_url = pd.DataFrame(url_list,columns =['link']) 
    df_egg=df_url[df_url['link'].str.contains('eggs-meat-fish')]
    df_egg['category_name']='eggs-meat-fish'
    df_gwf=df_url[df_url['link'].str.contains('gourmet-world-food')]
    df_gwf['category_name']='gourmet-world-food'
    df_grains=df_url[df_url['link'].str.contains('foodgrains-oil-masala')]
    df_grains['category_name']='foodgrains-oil-masala'
    df_veg=df_url[df_url['link'].str.contains('fruits-vegetables')]
    df_veg['category_name']='fruits-vegetables'
    df_backery=df_url[df_url['link'].str.contains('bakery-cakes-dairy')]
    df_backery['category_name']='bakery-cakes-dairy'
    df_bev=df_url[df_url['link'].str.contains('beverages')]
    df_bev['category_name']='beverages'
    
    df_snacks=df_url[df_url['link'].str.contains('snacks-branded-foods')]
    df_snacks['category_name']='snacks-branded-foods'
    
    frames=[df_egg,df_gwf,df_grains,df_veg,df_backery,df_bev,df_snacks]
    url_selected=pd.concat(frames)
    url_selected.to_csv(r'bb_selected_link.csv',index = False)

    


# In[ ]:





# In[ ]:




