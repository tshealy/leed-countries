from bs4 import BeautifulSoup
from urllib import request
import urllib
import re
import pickle
import csv
import json

import sqlite3

def setup_db():
   conn = sqlite3.connect('usa_scraped_2.sqlite3')
   c = conn.cursor()
   # c.execute("""CREATE TABLE "api_project" ("id" integer NOT NULL PRIMARY KEY, "is_confidential" varchar(255) NOT NULL, "name" varchar(255) NULL, "street" varchar(255) NULL, "city" varchar(255) NULL, "zip_code" varchar(255) NULL, "country" varchar(255) NULL, "leed_version" varchar(255) NULL, "points_achieved" integer NULL, "certification_level" varchar(255) NULL, "certification_date" date NULL, "owner_types" varchar(255) NULL, "gross_square_foot" bigint NULL, "total_property_area" bigint NULL, "project_types" varchar(255) NULL, "registration_date" date NULL);
   # """)
   c.execute('CREATE TABLE "points_data" ("id" integer NOT NULL PRIMARY KEY, "points_achieved" text)')
   conn.commit()
   conn.close()


def pull_score_card(id):
   scores_dict = {}
   leed_url = 'http://www.usgbc.org/?q=projectscorecard/{}'.format(id)
   leed_req = urllib.request.Request(leed_url, headers={'User-Agent': 'Mozilla/5.0'})
   leed_content = urllib.request.urlopen(leed_req).read()
   leed_soup = BeautifulSoup(leed_content)
   sub_ids = leed_soup.find_all( "td", class_="credit-id")
   sub_ids = [sub_ids[i].get_text() for i in range(len(sub_ids))]
   points_objects = leed_soup.find_all( "td", class_="point possible")
   points_achieved = [int(re.split(r"/", points_objects[i].get_text())[0]) for i in range(len(points_objects))]
   points_possible = [int(re.split(r"/", points_objects[i].get_text())[1]) for i in range(len(points_objects))]
   scores_dict[id] = [{sub_ids[i]: (points_achieved[i], points_possible[i])} for i in range(len(sub_ids))]
   return scores_dict

def scraper():
   with open('usa_private_ids_copy.csv', encoding='ISO-8859-1') as infile:
       ids = csv.reader(infile)
       for id in ids:
           print(id)
           one_score = pull_score_card(id[0])
           one_score_key, one_score_value = list(one_score.items())[0]
           conn = sqlite3.connect('usa_scraped_2.sqlite3')
           c = conn.cursor()
           c.execute("INSERT OR IGNORE INTO points_data (id, points_achieved) VALUES (?, ?)",
                     (one_score_key, json.dumps(one_score_value)))

           conn.commit()
           conn.close()



if __name__ == "__main__":
   setup_db()
   scraper()
