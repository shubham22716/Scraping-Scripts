# -*- coding: utf-8 -*-
"""
Created on Mon Jun 12 02:36:40 2023

@author: User
"""

from selenium import webdriver
from time import time, sleep
from selenium.webdriver.common.by import By 
import psycopg2
import pandas as pd

# set up the webdriver
options = webdriver.ChromeOptions()
options.add_argument("--headless")

query = '''
select product_id,shortsku as product_shortsku, product_name, category as product_category  from lpdatamart.tbl_d_product where listable =1 and instock=1 limit 10000
'''
user = 'abc'
password = 'def'
database = 'db'
host = 'host'
conn = psycopg2.connect(database, user = user, password = password, 
                        host = "host", port = "5439")
hf = pd.read_sql_query(query, con = conn)

final_df = pd.DataFrame(columns = ['product_name', 'product_url', 'image_url', 'purchase_date', 'review_author', 
                                           'review_title', 'review_date', 'review_rating', 'review_text', 'liked', 
                                           'if_flagged', 'category'])

t0 = time()
for i in range(50):
    driver = webdriver.Chrome(options = options)
    url = "https://www.xyz.com/products/__{}.html".format(str(hf['product_shortsku'][i]))
    print("URL No. {} : {}".format(i+1, url))
    print(hf['product_shortsku'][i])
    try:
        driver.get(url)
        driver.execute_script("window.scrollTo(0,2100)")
        sleep(2)
        
        product_name = []
        product_url = []
        shortsku = []
        img_src = []
        review_list = []
        rating_list = []
        author_info = []
        review_date = []
        purchase_date = []
        flagged=[]
        helpful = []
        review_title = []
        cats = []
    
    #     driver.get_screenshot_as_file(r'Desktop/screenshot.png')
        
        # to click until all reviews are open
        
        k = 0
        while(k == 0):
            try:
                reviews_tab = driver.find_element(By.CLASS_NAME, "Button--tertiary")
                reviews_tab.click()
                sleep(2)
            except:
                k = 1
        
        sleep(2)
        
        try:
            abc = driver.find_elements(By.CLASS_NAME, "ProductReviewsList__content")
        except:
            pass
        
        try:
            cde = driver.find_element(By.CLASS_NAME, "pdProdImg")
        except:
            pass
        
        for e in abc:
            product_name.append(hf['product_name'][i])
            product_url.append(url)
            shortsku.append(hf['product_shortsku'][i])
            cats.append(hf['product_category'][i])
            
            try:
                img_src.append(cde.get_attribute('src'))
            except:
                img_src.append('N/A')
            
            try:
                r = e.find_element(By.TAG_NAME,"p")
                review_list.append(r.text)
            except:
                review_list.append('')
            
            try:
                title = e.find_element(By.CLASS_NAME,"ProductReviewCard__title")
                review_title.append(title.text)
            except:
                review_title.append('N/A')
            
            try:
                ra = e.find_elements(By.CLASS_NAME,"ProductReviewRatingStars__stars--fullStarRating")
                rating_list.append(len(ra))
            except:
                rating_list.append('N/A')
            
            try:
                au = e.find_element(By.CLASS_NAME, "ProductReviewCard__reviewerName")
                author_info.append(au.text.split('|')[0])
                review_date.append(au.text.split('|')[1])
            except:
                author_info.append('N/A')
                review_date.append('N/A')
            
            try:
                pu = e.find_element(By.CLASS_NAME,"ProductReviewCard__purchaseDate")
                purchase_date.append(pu.text.split('on')[1])
            except:
                purchase_date.append('N/A')
            
            try:
                likes = e.find_element(By.CLASS_NAME,"ProductReviewUpDownVote__upVotes")
            except:
                likes.append(0)
            
            try:
                helpful.append(likes.text)
            except:
                helpful.append('N/A')
            
            try:
                flag = e.find_element(By.CLASS_NAME,"productReviewInappropriateFlag")
                if flag.text == 'Flag':
                    flagged.append('Yes')
                else:
                    flagged.append('No')
            except:
                flagged.append('N/A')
        
        df = pd.DataFrame(columns = ['product_name', 'product_url', 'image_url', 'purchase_date', 'review_author', 
                                     'review_title', 'review_date', 'review_rating', 'review_text', 'liked', 
                                     'if_flagged', 'category'])
        
        df['product_name'] = product_name
        df['product_url'] = product_url
        df['image_url'] = img_src
        df['purchase_date'] = purchase_date
        df['review_author'] = author_info
        df['review_title'] = review_title
        df['review_date'] = review_date
        df['review_rating'] = rating_list
        df['review_text'] = review_list
        df['liked'] = helpful
        df['if_flagged'] = flagged
        df['category'] = cats
        
        final_df = pd.concat([final_df, df], axis = 'index', ignore_index = True)
        print("Reviews Scraped for this URL : ", len(df))
        print("Total Reviews Scraped so far : ", len(final_df))
        print('****************************************************************************')
    except:
        if driver:
            driver.close()
        print("Error while Scraping from this URL. Going to the next one.")
        sleep(2)
        continue

t1 = time()

secs = round(t1-t0, 3)
print("\n\nTime taken in secs : ", secs)

print(final_df.shape)
final_df = final_df.drop_duplicates(subset = ['review_text'], keep = 'last', ignore_index = True)
print(final_df.shape)

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

final_df['product_name'] = final_df['product_name'].apply(lambda x: str(x).replace(';', '-'))
final_df['review_author'] = final_df['review_author'].apply(lambda x: str(x).replace('\n', '').strip(' '))
final_df.head(50)

print(len(final_df))
fout = './lp_product_reviews_sel_{}records.csv'.format(len(final_df))
final_df.to_csv(r'Desktop/final_df.csv')


