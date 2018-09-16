# -*- coding: utf-8 -*-
"""
@author: Xianqiao Li
"""

import requests, json, time, os
import pandas as pd

# Set up woking directory
os.chdir('/Documents/Fortune_500_Ranks')

# Construct URLs
urls = ['http://fortune.com/api/v2/list/1666518/expand/item/ranking/asc/{}/100'.format(str(k)) for k in range(0, 1000, 100)]

# Create dataframes to store data
temp = pd.DataFrame(columns=['Company', 'Rank', 'Revenues($M)'])
df = pd.DataFrame()

# Create a function that is reuseable
def get_data(link):
    for c in range(5):
        try:
            wb_data = requests.get(link, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36'})
            data = json.loads(wb_data.text)
            break
        except Exception as e:
            print e
            with open('error_fortune_list.txt', 'a') as f:
                f.write(link + '\n' + str(e) + '\n')

    iter_len = link.split('/')[-1]
        
    for i in range(int(iter_len)):
        temp.loc[i] = [data['list-items'][i]['title'], data['list-items'][i]['rank'], data['list-items'][i]['highlights'][1]['value']]

# Call the function and start the process in the main script
if __name__ == '__main__':
    t0 = time.time()
    try:
        for url in urls:
            print "Fetching %s" % url
            get_data(url)
            time.sleep(5)
            df = df.append(temp, ignore_index=True)
    except Exception as e:
        with open('error_fortune_list.txt', 'a') as f:
            f.write(str(e) + '\n')

    t1 = time.time()
    print "Total runtime is: %s seconds" % str(int(t1-t0))

    df.to_excel('Fortune_1000_ranks.xlsx', index=False)
    print "Finished"