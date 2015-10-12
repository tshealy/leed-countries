import pandas as pd
import sqlite3
import ast
import itertools
import numpy as np


def get_buildings():
    buildings_all = pd.read_csv("../original_data/PublicLEEDProjectDirectory.csv", skiprows=3, encoding="ISO-8859-1")
    buildings_all = buildings_all[buildings_all['Isconfidential'] == 'No']
    buildings_2009 = buildings_all[buildings_all["LEEDSystemVersionDisplayName"] == "LEED-NC v2009"]
    return(buildings_2009)

def my_filter_2009(df, country_code):
    countries = df.dropna(subset = ['PointsAchieved'])
    countries['PointsAchieved'] = countries['PointsAchieved'].convert_objects(convert_numeric=True)
    countries = countries[countries['PointsAchieved'] > 39]
    countries = countries[countries['Country'] == country_code]
    countires = countries[countries["OwnerTypes"] == 'Corporate: Privately Held']
    return(countries)

def read_table(sqlite_data):
# Read sqlite query results into a pandas DataFrame
    con = sqlite3.connect(sqlite_data)
    credits = pd.read_sql_query("SELECT * from points_data", con)
    con.close()
    return(credits)

def make_df(credits):
    count = 0
    x_df = pd.DataFrame()
    for i in credits.iloc[:,0]:
        x_df[i] = pd.Series({key :value[0] for i in ast.literal_eval(credits.iloc[count,1]) for key, value in i.items()})
        count +=1
    new_x = x_df.transpose()
    return(new_x)

def make_merge(country, credits_df):
    merged = country.merge(credits_df, right_index=True, left_on=['ID'])
    return(merged)

def making_it_happen(sqlite_data, country_code):
    buildings_2009 = get_buildings()
    country = my_filter_2009(buildings_2009, country_code)
    credits = read_table(sqlite_data)
    credits_df = make_df(credits)
    final_df = make_merge(country, credits_df)
    return(final_df)

def make_save_df(sqlite_data, country_code, file_name):
    df = making_it_happen(sqlite_data, country_code)
    df.to_pickle(file_name)
    print('{} '.format(file_name) + 'pickled')
    print(df.head())


if __name__ == "__main__":
    make_save_df("usa_scraped_2.sqlite3", 'US', 'usa_credits_2.pickle')
