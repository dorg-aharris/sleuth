import os
import gc
import re
import csv
import sys
import time
import xlrd
import boto3
import numpy
import string
import pandas
import socket
import pymysql
import argparse
import datetime
import requests
import sqlalchemy
import phonenumbers
from time import sleep
from collections import Counter
from nameparser import HumanName
from backoff import on_exception, expo
from ratelimit import limits, RateLimitException
from multiprocessing.dummy import Pool as ThreadPool

def run_thread(df, process, division):
    i = 0
    iteration = 0
    while i < len(df):
        gc.collect() #clear memory cache (garbage collect) to minimize usage.
        try:
            sub_df = df[i:i+division]
            i += division
        except IndexError:
            sub_df = df[i:len(df)]
        failures = []
        start = time.time()
        pool = ThreadPool(5)
        rows = sub_df.to_dict(orient='records')
        results = pool.map(process, rows) #returns list of dictionaries
        duration = time.time() - start
        pool.close()
        iteration +=1


def personal_analysis_function(row):
    try:
        api_call_id = row['id']
        original_fn_ipa = (str(doublemetaphone(str(row['first_name']))).replace("'", '').replace("(", '').replace(")", '').replace(",",''))
        original_ln_ipa = (str(doublemetaphone(str(row['last_name']))).replace("'", '').replace("(", '').replace(")", '').replace(",",''))
        owner_fn_ipa = (str(doublemetaphone(str(row['owner_firstname']) if row['owner_firstname'] else ''))).replace("'",'').replace("(", '').replace(")", '').replace(",", '')
        owner_ln_ipa = (str(doublemetaphone(str(row['owner_lastname']) if row['owner_lastname'] else ''))).replace("'", '').replace("(", '').replace(")", '').replace(",", '')
        sec_fn_ipa = (str(doublemetaphone(str(row['2nd_most_associated_firstname']) if row['2nd_most_associated_firstname'] else ''))).replace("'", '').replace("(", '').replace(")", '').replace(",", '')
        sec_ln_ipa = (str(doublemetaphone(str(row['2nd_most_associated_lastname']) if row['2nd_most_associated_lastname'] else ''))).replace("'",'').replace("(", '').replace(")", '').replace(",", '')
        owner_fn_levy = (levenshtein(original_fn_ipa, owner_fn_ipa))
        owner_ln_levy = (levenshtein(original_ln_ipa, owner_ln_ipa))
        sec_fn_levy = (levenshtein(original_fn_ipa, sec_fn_ipa))
        sec_ln_levy = (levenshtein(original_ln_ipa, sec_ln_ipa))
        owner_confidence = (owner_fn_levy * .175) + (owner_ln_levy * .825)
        sec_confidence = (sec_fn_levy * .175) + (sec_ln_levy * .825)
        phone_confidence = max(((owner_fn_levy * .175) + (owner_ln_levy * .825)), ((sec_fn_levy * .175) + (sec_ln_levy * .825)))
        if row['line_type'] == 'TollFree':
            line_status = 11
        elif (row['owner_name'] == '' or row['owner_name'] == None) and (row['line_type'] == '' or row['line_type'] == None):
            line_status = 9
        elif (row['owner_name'] == '' or row['owner_name'] == None) and (row['line_type'] != '' or row['line_type'] == None):
            line_status = 10
        elif (owner_confidence == 100) or (sec_confidence == 100):
            line_status = 4
        elif (owner_ln_levy == 100 and sec_ln_levy == 100 and (row['line_type'] in ('Mobile', 'NonFixedVOIP'))):
            line_status = 5
        elif (owner_ln_levy == 100 and sec_fn_levy == 100):
            line_status = 6
        elif (phone_confidence > 86):
            line_status = 7
        elif (owner_ln_levy == 100):
            line_status = 8
        else:
            line_status = str('')
        if line_status in (4,5,6,7,8,9,10,11):
            vars = (api_call_id, row['sleuth_stage_id'], row['source_id'], row['co_id'], row['original_contact_id'],
                    row['first_name'], row['last_name'], row['full_name'], row['line'], row['sleuth_api_status'],
                    row['country_calling_code'], row['line_type'], row['carrier'], row['is_prepaid'], row['is_commercial'],
                    row['owner_type'], row['owner_name'], row['2nd_most_associated_name'], row['warnings'],
                    owner_confidence, sec_confidence, phone_confidence, '', '', '', line_status)
            write_to_files(session_files[3], vars)
    except UnicodeEncodeError:
        owner_confidence = 0
        sec_confidence = 0
        phone_confidence = 0
        api_call_id = row['id']
        if row['line_type'] == 'TollFree':
            line_status = 11
        elif (row['owner_name'] == '' or row['owner_name'] == None) and (row['line_type'] == '' or row['line_type'] == None):
            line_status = 9
        elif (row['owner_name'] == '' or row['owner_name'] == None) and (row['line_type'] != '' or row['line_type'] == None):
            line_status = 10
        else:
            line_status = str('')
        if line_status in (9,10,11):
            vars = (api_call_id, row['sleuth_stage_id'], row['source_id'], row['co_id'], row['original_contact_id'],
                    row['first_name'], row['last_name'], row['full_name'], row['line'], row['sleuth_api_status'],
                    row['country_calling_code'], row['line_type'], row['carrier'], row['is_prepaid'], row['is_commercial'],
                    row['owner_type'],row['owner_name'], row['2nd_most_associated_name'], row['warnings'],
                    owner_confidence, sec_confidence, phone_confidence, '', '', '', line_status)
            write_to_files(session_files[3], vars)
    except:
        sleuth_id = row['sleuth_stage_id']
        data = (str(sys.exc_info()[0].__name__), '', sleuth_id)
        write_to_files(session_files[1], data)


def run_match_logic(evaluation_set, source):
    evaluation_set = pandas.read_csv(evaluation_set)
    run_thread(evaluation_set, personal_analysis_function, 1000)