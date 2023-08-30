#!/usr/bin/env python3


import requests
from bs4 import BeautifulSoup
import pandas as pd
import uuid
import hashlib
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import json



# Load the Snowflake configuration from the JSON file
with open("snow_cred.json") as f:
    config = json.load(f)
    
    
# Creating Snowflake Connection
snow_engine = create_engine(URL(
    account = config['account'],
    user = config['user'],
    password = config['password'],
    database = config['database'],
    schema = config['schema'],
    warehouse = config['warehouse'],
    role= config['role']
))

snow_connection = snow_engine.connect()
snow_connection


country_url_list = ['https://www.viamichelin.fr/web/Stations-service?address=Czechia',]
countries = ['France', 'United Kingdom', 'Switzerland', 'Germany', 'Belgium', 'Netherlands', 'Spain', 'Luxembourg', 'Czech Republic', 'Italy']
# 'https://www.viamichelin.fr/web/Stations-service?address=France',


def get_rqt(url):
    r = requests.get(url)
    sp = BeautifulSoup(r.content, 'html.parser')
    return sp


def get_pagi_url_max_num(page_n):
    pg = BeautifulSoup(str(page_n[0]), 'html.parser')
    pg_a = pg.findAll('a')

    if len(pg_a) > 0:
        pg_a = pg_a[-1]
        pgination_url = pg_a['href']
        pgination_url = pgination_url[0:pgination_url.index('=')+1]
        max_pg = int(pg_a.get_text())
        return pgination_url, max_pg
    
    
def get_pagination(url):
    r = requests.get(url)
    #print(r)
    soup = BeautifulSoup(r.content, 'html.parser')
    page_n = soup.findAll("p", class_="pagination-second-line")
    return page_n


def save_data(data_df):
    target_table = 'viamichelin_stations'
    del_id_lst = str(list(data_df['id'])).replace('[','(').replace(']',')')    
    try:
        # Delete existing rows to be updated with new version
        del_existing_rows_query = "DELETE FROM " +target_table+ " WHERE id IN " + del_id_lst
        with snow_engine.connect() as conn:
            conn.execute(del_existing_rows_query)

        # Save data in the DataFrame    
        data_df.to_sql(target_table, con=snow_engine, schema='public', index=False, if_exists='append')
        print('done')
    except Exception as err:
        print(err)    

    
def get_and_save_data(url, ctry):
    r = requests.get(url)
    print(url, r)
    soup = BeautifulSoup(r.content, 'html.parser')

    # Get station Names
    station_names = soup.findAll('div', class_='poi-item-name truncate')
    station_names = [val.get_text() for val in station_names]

    # Get station Addresses
    station_address = soup.findAll('div', class_="poi-item-details-address truncate")
    station_address = [val.get_text() for val in station_address]
    print(len(station_names), len(station_address))

    # Combine the Scrapped data into a list ready to be converted into a DataFrame
    dat_zip = zip(station_names[:2], station_address[:2])

    # Clear the list to save memory        
    station_names.clear()
    station_address.clear()

    zipped_dat = list(dat_zip)
    dat_zip = ''
    data = [list(i) for i in zipped_dat]
    zipped_dat.clear()

    # Create DataFrame from the data list
    data_df = pd.DataFrame(data, columns=['Station_Name', 'Station_Address'])
    data.clear()

    # Generate a UUID for each row based on the values in 'Station_Name' and 'Station_Address'
    generate_uuid = lambda row: uuid.uuid5(uuid.NAMESPACE_DNS, str(row['Station_Name']) + str(row['Station_Address'])).hex

    # Add new columns to the created DataFrame
    data_df["country"] = ctry
    data_df['id'] = data_df.apply(generate_uuid, axis=1)

    #Rearrange DataFrame
    data_df = data_df[['id', 'Station_Name', 'Station_Address', 'country']]
    
    # Save the Data to DB
    save_data(data_df)
    data_df
    print(data_df)
        

for url, ctry in zip(country_url_list, countries):
    try:
        # Scrape the page and save data
        get_and_save_data(url, ctry)        
        
        # Get and prepare Pagination        
        page_n = get_pagination(url)
        if get_pagi_url_max_num(page_n):
            print('More')
            pgination_url, max_pg = get_pagi_url_max_num(page_n)
            for i in range(2, (max_pg-max_pg) + 4):  # range(2, max_pg+1)
                get_and_save_data(pgination_url+str(i), ctry)
        else:
            print('less')
            continue
        
    except Exception as err:
        print(err)
        