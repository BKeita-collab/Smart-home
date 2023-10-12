#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 21 14:22:29 2022

@author: brahimakeita
"""
import pandas as pd
import psycopg2
import datetime
from psycopg2 import sql
import schedule
import time
import random
from functools import partial as pt

def manage_data(data):
    data_mg = pd.read_csv(data)
    data_mg = data_mg.rename(colums = {"KWH/hh (per half hour) ": "conso_KWH_hh" , 
                                       'LCLid':'lclid',
                                       'stdorToU' : 'stdortou'})
    ## Converting into datetime 
    data_mg['DateTime'] = pd.to_datetime(data_mg['DateTime'])
    
        
    data_BI = data_mg[(data_mg['DateTime'] > '2013-02-26 00:30:00') & (data_mg['DateTime'] < '2014-02-26 00:30:00')]
    
    data_BI['DateTime'] = data_BI['DateTime'] + datetime.timedelta(days = 8*365)
    
    return data_BI
    

def get_data(data):
    original_data = pd.read_csv(data)
    original_data = original_data.rename(columns = {"KWH/hh (per half hour) ": "conso_kwh_hh" , 
                                       "LCLid":"lclid",
                                       "DateTime":"datetime",
                                       "stdorToU" : "stdortou"})
    #original_data['id'] = original_data['id'].astype(int)
    #original_data['KWH/hh (per half hour) '] = original_data['KWH/hh (per half hour) '].astype(float)
    
    list_column = list(original_data.columns)
    data_list = [tuple(row) for row in original_data.itertuples(index=False)]
    #pick_up = random.randint(1, len(data_list))
    
    f_element = data_list[0]
    
    
    # Remove the data 
    original_data.drop(0, inplace = True)
    
    ## Save the data without the added value 
    original_data.to_csv(data, index = False)
    #print("-----", data_list)
    #original_data.drop(0)
    
    return list_column, f_element

def connect(name_db):
    conn = psycopg2.connect(
    port="5433",
    database=name_db,
    user="postgres",
    password="postgres")
    
    return conn


def create_tab(table = 'test_30', connection = 'PDM5'):
    """ create tables in the PostgreSQL database"""
    #connection = 
    connection = connect(connection)
    print("I'm in 2222")
    cur = connection.cursor()    
    commands = sql.SQL(
        "\
        CREATE TABLE {tables} ( \
            id SERIAL PRIMARY KEY,\
            LCLid TEXT ,\
            stdorToU TEXT,\
            DateTime TIMESTAMP,\
            conso_KWH_hh NUMERIC)"

        ).format(tables = sql.Identifier(table))
    cur.execute(commands)
    connection.commit()
    connection.close() 
    
    return schedule.CancelJob

def insert_data(data, conn = 'PDM5', table = 'test_30'):
    """ insert multiple vendors into the vendors table  """
    conn = connect(conn)
    fields = get_data(data)
    
    print('----', fields[1] , '@@@@@', fields[0])
    print("I'm in last")
    
    command = sql.SQL( 
        "INSERT INTO {tables} ({fields})\
 VALUES(\
        %s,%s,%s, %s)"
     ).format(
         tables = sql.Identifier(table),
         fields = sql.SQL(',').join(
             sql.Identifier(n) for n in fields[0])
         )

    # create a new cursor
    cur = conn.cursor()
    # execute the INSERT statement
    cur.execute(command,fields[1])
    # commit the changes to the database
    conn.commit()
    # close communication with the database
    cur.close()

# #schedule.every(10).seconds.do(create_tab)
schedule.every(20).seconds.do(insert_data,'sample_data.csv' )


while True:
     schedule.run_pending()
     time.sleep(1)


    
    