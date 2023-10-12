#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 22 15:32:51 2022

@author: brahimakeita
"""

import pandas as pd

data = pd.read_excel('Final_data_1week_Ori.xlsx')

data['threshold'] = 14/48


def threshold(conso):
    if conso < 14 :
        return "You're efficiently using energy"
    else:
        return "You're overconsumming energy"



equipement  = ['heater', 'freezer', 'LED' , 'kitchen', 'others']

weights_winter = [0.5 ,0.13, 0.07, 0.2, 0.1]

weights_summer = [0.1, 0.2, 0.05, 0.4, 0.25]

winter_months = ['November', 'December', 'January', 'February', 'March', 'April']

summer_months = ['May', 'June', 'July', 'August', 'September', 'October']

### Creation of the monthly 

data['price'] = data['KWH/hh (per half hour) '] * 0.185*2

def seasonality(months):
    
    if months in ['November', 'December', 'January', 'February', 'March', 'April']:
        return 'winter_months'
    
    elif months in ['May', 'June', 'July', 'August', 'September', 'October']:
        return 'summer_months'
    

def equipement_heater(row):
    
    if row['seasonality'] == 'winter_months':
        return row['KWH/hh (per half hour) '] * 0.5 
    else:
        return row['KWH/hh (per half hour) '] * 0.1
    

def equipement_freeze(row):
    
    if row['seasonality'] == 'winter_months':
        return row['KWH/hh (per half hour) '] * 0.13 
    else:
        return row['KWH/hh (per half hour) '] * 0.2


def equipement_led(row):
    if row['seasonality'] == 'winter_months':
        return row['KWH/hh (per half hour) '] * 0.07
    else:
        return row['KWH/hh (per half hour) '] * 0.05

def equipement_kitchen(row):
    if row['seasonality'] == 'winter_months':
        return row['KWH/hh (per half hour) '] * 0.2 
    else:
        return row['KWH/hh (per half hour) '] * 0.4

def equipement_others(row):
    
    if row['seasonality'] == 'winter_months':
        return row['KWH/hh (per half hour) '] * 0.1 
    else:
        return row['KWH/hh (per half hour) '] * 0.25


# data['alert_message'] = data['KWH/hh (per half hour) '].apply(threshold)
    

    
#data['heat'] = data['conso'].apply(equipement_heater,data['Month'], winter_months, summer_months)
#data['alert_message'] = data['KWH/hh (per half hour) '].apply()