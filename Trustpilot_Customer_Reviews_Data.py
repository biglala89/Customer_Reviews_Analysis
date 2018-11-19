# -*- coding: utf-8 -*-
"""
@author: Xianqiao Li
"""

from bs4 import BeautifulSoup
from urllib2 import urlopen
import re, os, time, sqlite3

# SQL statements
drop_reviews = """DROP TABLE IF EXISTS Reviews;"""
drop_users = """DROP TABLE IF EXISTS Users;"""
create_reviews = """CREATE TABLE IF NOT EXISTS Reviews (
						ID INTEGER PRIMARY KEY AUTOINCREMENT,
						Rating INT,
						Comment VARCHAR,
						Timestamp_ [CURRENT_TIMESTAMP]
					);"""
create_users = """CREATE TABLE IF NOT EXISTS Users (
						ID INTEGER PRIMARY KEY AUTOINCREMENT,
						Name VARCHAR
					);"""

class Scrape_User_Reviews(object):
	def __init__(self):
		self.link = 'https://www.trustpilot.com/review/www.bhphotovideo.com?page='
		self.db_name = 'Customer_Reviews'
		self.database = os.getcwd()+'/{}.db'.format(self.db_name)
		self.number_of_pages = 3

	# Create connection
	def Create_DB_and_Tables(self):
		try:
			conn = sqlite3.connect(self.database)
			print sqlite3.version
			self.Execute_SQL(conn, drop_reviews)
			self.Execute_SQL(conn, drop_users)
			self.Execute_SQL(conn, create_reviews)
			self.Execute_SQL(conn, create_users)
		except Exception as e:
			print e
		finally:
			conn.close()

	# Create tables in database
	def Execute_SQL(self, conn, query):
		try:
			c = conn.cursor()
			c.execute(query)
		except Exception as e:
			print e

	# Populate tables function
	def Table_Populate(self, conn, name, rating, time, comment):
		cur = conn.cursor()
		cur.execute("INSERT INTO Users (Name) VALUES (?)", [name])
		cur.execute("INSERT INTO Reviews (Rating, Comment, Timestamp_) VALUES (?, ?, ?)", [rating, comment, time])

	# Get data and populate tables in database
	def Parse_Data(self, webpage):  
		for i in xrange(3):
			try:
				sc = urlopen(webpage)
				time.sleep(0.5)
				break
			except Exception as e:
				print "%s attempt failed, giving it another try" % str(i+1)
				with open('error.log', 'a') as f:
					f.write(str(e)+'\n')
				continue
		
		source = sc.read()#.decode(encoding='utf-8')
		soup = BeautifulSoup(source, 'lxml')
		
		names = soup.find_all('h3', attrs={'class': 'consumer-info__details__name'})
		name_list = [name.text.strip() for name in names]
		
		rating_list = re.findall('<.*?star-rating star-rating-(\d+) star-rating--medium.*?>', source, flags=re.I | re.S)
		
		timestamps = soup.find_all('time', attrs={'class': 'ndate'})
		time_list = [stamp["title"] for stamp in timestamps]
		
		reviews = soup.find_all('p', attrs={'class': 'review-info__body__text'})
		review_list = [review.text.strip() for review in reviews]
		
		try:
			conn = sqlite3.connect(self.database)
			for i in xrange(len(names)):
				self.Table_Populate(conn, name_list[i], rating_list[i], time_list[i], review_list[i])
			conn.commit()
		except Exception as e:
			print e
			conn.rollback()
			print "Roll back"
		finally:
			conn.close()
		
	# Main function
	def Main(self):
		for num in xrange(1, self.number_of_pages+1):
			page = self.link+str(num)
			print "Downloading %s" % page
			try:
				self.Parse_Data(page)
			except Exception as e:
				with open('error.log', 'a') as f:
					f.write(str(page)+'\n'+str(e)+'\n')
				continue

if __name__ == "__main__":
	scraper = Scrape_User_Reviews()
	t_start = time.time()
	scraper.Create_DB_and_Tables()
	scraper.Main()
	t_end = time.time()
	print "Total time elapsed: %s seconds" % str(int(t_end - t_start))