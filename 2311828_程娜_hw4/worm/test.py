import requests
import os
import pandas as pd
import selenium
import time
import subprocess
import wget
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor,as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options as FirefoxOptions
firefox_options=FirefoxOptions()
firefox_options.add_argument('--headless')
firefox_options.page_load_strategy='none'
geckodriver_path='D:\Software\Python\Scripts\geckodriver.exe'
service=FirefoxService(executable_path=geckodriver_path)

urls=[f'https://xb.nankai.edu.cn/category/16/{i}' for i in range(1,95)] #92
title2url_df=pd.DataFrame(columns=['url'])
title2url_df.index.name='title'

res_dict={}#URL->text
href_urls=[]

errfile=open('err.txt',"a")

def crawlIndex(urls):
    driver=webdriver.Firefox(service=service,options=firefox_options)
    cnt=0
    href_urls=[]
    
    if 'pages' not in os.listdir():
        os.mkdir('pages')
    for url in urls:
        cnt+=1
        print(f"{cnt} : Processing Page: {url}")
        response=requests.get(url)
        if(response.status_code==200):
            soup=BeautifulSoup(response.content,'html.parser')
            links=soup.find_all('a',href=True,target="_blank")
            for link in links:
                if '/article/' not in link['href']:
                    continue
                href=link['href'].replace('/article/','https://xb.nankai.edu.cn/article/')
                if len(link.get_text().replace('/','^').split())<1:
                    continue
                text=(link.get_text().replace('/','^').split())[0]

                res_dict[href]=text
                
                print(f"href:{href}, text:{text}")

                
                try:
                    driver.get(href)
                    time.sleep(2)
                    cur_url=driver.current_url
                    filepath='./pages/'+text+'.pdf'
                    print(f"href:{href}")
                    # download_path=os.path.join(os.getcwd(),'./pages/'+text+'.pdf')
                    # subprocess.run(['wget','-O','./pages/'+text+'.pdf',cur_url])
                    # wget.download(cur_url,'./pages/'+text+'.pdf')
                    response=requests.get(cur_url,stream=True)
                    title2url_df.loc[text+'.pdf']=href
                    if response.status_code==200:
                        with open(filepath,'wb') as file:
                            for chunk in response.iter_content(chunk_size=8192):
                                file.write(chunk)
                        print("Download complete!")
                    else:
                        print(f"Failed to download: {response.status_code}",file=errfile)

                except:
                    print(f'error in {href}')
    driver.quit()
    return href_urls

def procPage(url):
    if 'pages' not in os.listdir():
        os.mkdir('./pages')
    try:
        if url.startswith('http') or url.startswith('https'):
            response = requests.get(url)
            soup=BeautifulSoup(response.content,'html.parser')
            if soup.title:
                title_text=soup.title.string.replace('/','^')
                print(f'Title:{title_text}')
                if len(title_text)>0:
                    filepath=f'pages/{title_text}.html'
                    with open(filepath,'w',encoding='utf-8') as f:
                        f.write(str(soup.prettify()))
                    title2url_df.loc[title_text]=url
                
    except:
        print(f"error in {url}")
        
def do_page(url_list):
    cnt=0
    url_list=list(set(url_list))
    for url in url_list:
        cnt+=1
        print(f"{cnt}/{len(url_list)} : Processing Page: {url}")
        procPage(url)
        
        
if __name__=='__main__':
    # crawlIndex(urls=urls)
    # procPage("https://news.nankai.edu.cn/mtnk/system/2024/12/01/030064788.shtml")
    href_urls=crawlIndex(urls=urls)
    do_page(href_urls)
    title2url_df.to_csv("title2url.csv")
# 'https://news.nankai.edu.cn/mtnk/index.shtml'        

