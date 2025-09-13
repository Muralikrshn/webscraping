
import requests
import pandas as pd
import re
import os
# os.chdir('H:/')


# In[2]:


#scraping function
def scrape(url):
    session=requests.Session()    
    session.headers.update(
            {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'})    
    response=session.get(url)    
    return response

#get data
def etl(response):
    
    #create a list
    text=response.content.decode('utf-8').split('\r')  
    print(type(text))
    #create index for each block
    assets=[i for i in text if 'CHICAGO MERCANTILE EXCHANGE' in i]
    ind=[text.index(i) for i in assets]

    overall = []

    #etl
    for i in ind:
        
        commodity=text[i].split(' - CHICAGO MERCANTILE EXCHANGE')[0].replace('\n','')
        commodity_code=text[i].split('Code-')[-1].replace('\n','')
        date=re.search('\d{2}\/\d{2}\/\d{2}',text[i+1]).group()
        contractunit=re.search('(?<=\().*(?=OPEN INTEREST)',text[i+7]).group().replace(')','')
        open_interest=re.search('(?<=OPEN INTEREST\:).*',text[i+7]).group()
        non_commercial_long_commitment,non_commercial_short_commitment, \
        non_commercial_spread_commitment,commercial_long_commitment, \
        commercial_short_commitment,total_long_commitment,total_short_commitment, \
        non_reportable_long_commitment,non_reportable_short_commitment=re.findall('\S+',text[i+9])
        changedate=re.search('\d{2}\/\d{2}\/\d{2}',text[i+11]).group()
        change_open_interest=text[i+11].split(' ')[-1].replace(')','')
        non_commercial_long_change,non_commercial_short_change, \
        non_commercial_spread_change,commercial_long_change, \
        commercial_short_change,total_long_change,total_short_change, \
        non_reportable_long_change,non_reportable_short_change=re.findall('\S+',text[i+12])
        non_commercial_long_percent,non_commercial_short_percent, \
        non_commercial_spread_percent,commercial_long_percent, \
        commercial_short_percent,total_long_percent,total_short_percent, \
        non_reportable_long_percent,non_reportable_short_percent=re.findall('\S+',text[i+15])
        totaltraders=text[i+17].split(' ')[-1].replace(')','')
        non_commercial_long_traders,non_commercial_short_traders, \
        non_commercial_spread_traders,commercial_long_traders, \
        commercial_short_traders,total_long_traders,total_short_traders=re.findall('\S+',text[i+18])
        
        temp=[commodity,commodity_code,date,contractunit,open_interest,
              non_commercial_long_commitment,non_commercial_short_commitment,
              non_commercial_spread_commitment,commercial_long_commitment,
              commercial_short_commitment,total_long_commitment,
              total_short_commitment,non_reportable_long_commitment,
              non_reportable_short_commitment,changedate,change_open_interest,
              non_commercial_long_change,non_commercial_short_change,
              non_commercial_spread_change,commercial_long_change,
              commercial_short_change,total_long_change,total_short_change,
              non_reportable_long_change,non_reportable_short_change,
              non_commercial_long_percent,non_commercial_short_percent,
              non_commercial_spread_percent,commercial_long_percent,
              commercial_short_percent,total_long_percent,
              total_short_percent,non_reportable_long_percent,
              non_reportable_short_percent,totaltraders,
              non_commercial_long_traders,non_commercial_short_traders,
              non_commercial_spread_traders,commercial_long_traders,
              commercial_short_traders,total_long_traders,total_short_traders]
        
        overall+=temp
    return text 
    
# In[4]:

def main():

    # url='https://books.toscrape.com/catalogue/page-1.html'
    url='https://www.cftc.gov/dea/futures/deacmesf.htm'
    
    #scrape
    response=scrape(url)

    #get data
    df=etl(response)
    with open("output.txt", "w", encoding="utf-8") as f:
      f.write("\n".join(df))

    # df.to_csv('trader commitment report.csv',index=False)
    

if __name__ == "__main__":
    main()