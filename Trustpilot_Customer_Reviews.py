# -*- coding: utf-8 -*-
"""
@author: Xianqiao Li
"""

from bs4 import BeautifulSoup
from urllib2 import urlopen
from pandas import DataFrame
from time import strftime
import re, os, time, sqlite3

os.chdir(file_path)
link = 'https://www.trustpilot.com/review/www.bhphotovideo.com?page='
database = Filepath/sqlitestudio-3.1.1/SQLiteStudio/Customer_Reviews

cols = ['User', 'Rating', 'Time', 'Title', 'Review']
tmp = DataFrame(columns=cols)
df = DataFrame(columns=cols)

# Populate tables in sqlite
def table_populate(conn, c, name, rating, time, title, comment):
    c.execute("INSERT INTO Users VALUES (?, ?, ?)", (name, rating, time))
    c.execute("INSERT INTO Reviews VALUES (?, ?, ?)", (title, comment, time))

# Get data and create dataframe to store the data
def get_data(link):  
    for c in range(5):
        try:
            sc = urlopen(link)
            print "%s attempt" % str(c+1)
            break
        except Exception as e:
            print e
            continue
    
    source = sc.read()#.decode(encoding='utf-8')
    
    soup = BeautifulSoup(source, 'lxml')
    
    names = soup.find_all('a', attrs={'class': 'user-review-name-link'})
    name_list = [name.text.strip() for name in names]
    
    rating_list = re.findall('<.*?star-rating count-(\d+) size-medium.*?>', source, flags=re.I | re.S)
    
    timestamps = soup.find_all('time', attrs={'class': 'ndate'})
    time_list = [stamp["title"] for stamp in timestamps]
    
    summaries = soup.find_all('a', attrs={'class': 'review-title-link'})
    summary_list = [summary.text for summary in summaries]
    
    reviews = soup.find_all('div', attrs={'class': 'review-body'})
    review_list = [review.text.strip() for review in reviews]
    
    conn = sqlite3.connect(database)
    c = conn.cursor()
    
    for i in xrange(len(names)):
        tmp.loc[i] = [name_list[i], rating_list[i], time_list[i], summary_list[i], review_list[i]]
        table_populate(conn, c, name_list[i], rating_list[i], time_list[i], summary_list[i], review_list[i])
        try:
            conn.commit()
        except Exception as e:
            print e
            conn.rollback()
            print "Rolled back"           
    conn.close()
    

if __name__ == "__main__":
    t0 = time.time()
    for c in range(1, 31):
        ww = link+str(c)
        print "Downloading %s" % ww
        try:
            get_data(ww)
            df = df.append(tmp, ignore_index=True)
            time.sleep(1)
        except Exception as e:
            with open('error.log', 'a') as f:
                f.write(str(ww)+'\n'+str(e)+'\n')
                print e
            continue
    t_end = time.time()
    print "Total elapsed time: %s seconds" % str(int(t_end - t0))