import os
import gc
import re
import csv
import sys
import time
import xlrd
import boto3
import numpy
import pylev
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
import strategic_detective
from collections import Counter
from nameparser import HumanName
from metaphone import doublemetaphone
from backoff import on_exception, expo
from strategic_detective import run_match_logic
from ratelimit import limits, RateLimitException
from multiprocessing.dummy import Pool as ThreadPool

def getParameter(param_name):
# Create the SSM Client
    ssm = boto3.client('ssm', region_name='us-west-2')
# Get the requested parameter
    response = ssm.get_parameters(Names=[param_name,],WithDecryption=True)
# Store the credentials in a variable
    credentials = response['Parameters'][0]['Value']
    return credentials

country_abr = ['AF', 'AL', 'DZ', 'AS', 'AD', 'AO', 'AI', 'AQ', 'AG', 'AR', 'AM', 'AW', 'AU', 'AT', 'AZ', 'BS', 'BH',
               'BD', 'BB', 'BY', 'BE', 'BZ', 'BJ', 'BM', 'BT', 'BO', 'BQ', 'BA', 'BW', 'BV', 'BR', 'IO', 'BN', 'BG',
               'BF', 'BI', 'CV', 'KH', 'CM', 'KY', 'CF', 'TD', 'CL', 'CN', 'CX', 'CC', 'CO', 'KM', 'CD', 'CG', 'CK',
               'CR', 'HR', 'CU', 'CW', 'CY', 'CZ', 'CI', 'DK', 'DJ', 'DM', 'DO', 'EC', 'EG', 'SV', 'GQ', 'ER', 'EE',
               'SZ', 'ET', 'FK', 'FO', 'FJ', 'FI', 'FR', 'GF', 'PF', 'TF', 'GA', 'GM', 'GE', 'DE', 'GH', 'GI', 'GR',
               'GL', 'GD', 'GP', 'GU', 'GT', 'GG', 'GN', 'GW', 'GY', 'HT', 'HM', 'VA', 'HN', 'HK', 'HU', 'IS', 'IN',
               'ID', 'IR', 'IQ', 'IE', 'IM', 'IL', 'IT', 'JM', 'JP', 'JE', 'JO', 'KZ', 'KE', 'KI', 'KP', 'KR', 'KW',
               'KG', 'LA', 'LV', 'LB', 'LS', 'LR', 'LY', 'LI', 'LT', 'LU', 'MO', 'MK', 'MG', 'MW', 'MY', 'MV', 'ML',
               'MT', 'MH', 'MQ', 'MR', 'MU', 'YT', 'MX', 'FM', 'MD', 'MC', 'MN', 'ME', 'MS', 'MA', 'MZ', 'MM', 'NA',
               'NR', 'NP', 'NL', 'NC', 'NZ', 'NI', 'NE', 'NG', 'NU', 'NF', 'MP', 'NO', 'OM', 'PK', 'PW', 'PS', 'PA',
               'PG', 'PY', 'PE', 'PH', 'PN', 'PL', 'PT', 'PR', 'QA', 'RO', 'RU', 'RW', 'RE', 'BL', 'SH', 'KN', 'LC',
               'MF', 'PM', 'VC', 'WS', 'SM', 'ST', 'SA', 'SN', 'RS', 'SC', 'SL', 'SG', 'SX', 'SK', 'SI', 'SB', 'SO',
               'ZA', 'GS', 'SS', 'ES', 'LK', 'SD', 'SR', 'SJ', 'SE', 'CH', 'SY', 'TW', 'TJ', 'TZ', 'TH', 'TL', 'TG',
               'TK', 'TO', 'TT', 'TN', 'TR', 'TM', 'TC', 'TV', 'UG', 'UA', 'AE', 'GB', 'UY', 'UZ', 'VU', 'VE', 'VN',
               'VG', 'VI', 'WF', 'EH', 'YE', 'ZM', 'ZW', 'AX', ]

#retrieve all parameters for the application
##ultra-rare do not steal
server = 'sleuthdb.cibydfkixtvw.us-west-2.rds.amazonaws.com'
account_sid = getParameter('icehook-accountsid')
auth_token = getParameter('icehook-authtoken')
sleuth_pass = getParameter('rops-sleuth-pwd')
wp_api_key = getParameter('rops-white-pages-api-key')
wp_url_base = "https://proapi.whitepages.com/3.0/phone?"
exclude = set(string.punctuation)
SECOND = 1
# client = Client(account_sid, auth_token)
owner_co_id_cats = []
session_files = []
ip = []

def window_update(id, login_id, query):
    connection = pymysql.connect(host=server,
                                 user='app_sleuth',
                                 password=sleuth_pass,
                                 db='sleuth',
                                 charset='utf8',
                                 use_unicode=True)
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        connection.close()
    except:
        cursor.execute("""INSERT INTO sleuth.sleuth_errors(error, sleuth_stage_id, login_id) 
                          VALUES('{0}','{1}','{2}')""".format(*(str(sys.exc_info()[0].__name__), id, login_id)))
        connection.commit()
        connection.close()
        print('Error, check the error logs.')


def import_csvs(path, db):
    print(str("Importing file : " + path))
    # try:
    database_username = 'app_sleuth'
    database_password = sleuth_pass
    database_ip = server
    database_name = 'sleuth'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password,
                                                          database_ip, database_name))
    frame = pandas.read_csv(path)
    frame.to_sql(con=database_connection, name=db, if_exists='append', index=False)
    print("Successful import!")
    # except:
    #     print("Failed import!")
    #     pass


def write_to_files(file, vars):
    with open(file, 'a') as f:
        writer = csv.writer(f)
        writer.writerow(vars)
    f.close


def get_data(query):
    connection = pymysql.connect(host=server, user='app_sleuth', password=sleuth_pass, db='sleuth', charset='utf8', use_unicode=True)
    data = pandas.read_sql(query,connection)
    connection.close()
    return data


def get_dm_data(query):
    connection = pymysql.connect(host='10.255.8.46', user='app_sleuth', password=sleuth_pass, db='sleuth', charset='utf8', use_unicode=True)
    data = pandas.read_sql(query,connection)
    connection.close()
    return data


def name_split(var, part):
    return HumanName(var)[part]


def co_cat_function(row):
    concat = ''.join(ch for ch in str(str(row['company_name']).lower() +
                                      str(row['company_id']).lower()) if ch not in exclude).replace(' ','')
    return concat


def levenshtein(a, b):
    len_a = len(a)
    len_b = len(b)
    distance = pylev.levenshtein(a, b)
    maxLength = max(len_a, len_b)
    result = maxLength - distance
    percentage = (result / maxLength) * 100
    return percentage


def correct_line(line, type):
    clean_line = re.sub('[^A-Za-z0-9]+', '', str(line))
    parsed_line = phonenumbers.format_number(phonenumbers.parse(clean_line, 'US'),
                                             phonenumbers.PhoneNumberFormat.INTERNATIONAL)
    iso = phonenumbers.region_code_for_number(phonenumbers.parse(clean_line, 'US'))
    clean = (0,0,0)
    if len(parsed_line) == 15 and iso in ['US', 'CA']:
        clean = (parsed_line[3:], '0', iso)
    if len(parsed_line) == 15 and iso is None:
        clean = (parsed_line[3:], '23', iso)
    if any(c.isalpha() for c in clean_line):
        clean = (parsed_line, '24', iso)
    if iso in country_abr:
        clean = (parsed_line, '24', iso)
    elif clean is (0,0,0):
        clean = (parsed_line[3:], '24', iso)
    if type == 'line':
        return clean[0]
    if type == 'status':
        return clean[1]
    if type == 'iso':
        return clean[2]


def dupe_fix(row):
    if row['duplicate'] == True:
        return '3'
    else:
        return row['status']


def strip_phones(s):
    exclude = set(string.punctuation)
    return ''.join(ch for ch in s if ch not in exclude)


def status(row):
    if row['line_status'] == '0':
        return row['status']
    else:
        return row['line_status']


def dorg_line(line_type):
    if line_type in ('Landline', 'FixedVOIP'):
        dorg_line = str('OFFICE LINE')
    elif line_type in ('Mobile', 'NonFixedVOIP'):
        dorg_line = str('MOBILE LINE')
    elif line_type == 'TollFree':
        dorg_line = str('TOLLFREE')
    else:
        dorg_line = str('CHECK DATA')
    return dorg_line


def create_headers(file, headers):
    with open(file, 'wb') as f:
        w = csv.writer(f)
        w.writerow(headers)
        for data in yourdata:
            w.writerow(data)


def strip_names(row):
    return (str(row['company_id']), str(row['company_name']).replace(" ", '').lower())


def get_rv_company_list():
    company_names = get_dm_data(str("""SELECT company_name, company_id FROM dorg.company_other_name
                                       UNION SELECT name, id FROM dorg.company"""))
    company_names['cat'] = company_names.apply(co_cat_function, axis=1)
    company_names['stripped'] = company_names.apply(strip_names, axis=1)
    return [list(company_names['cat']), list(company_names['stripped'])]


@on_exception(expo, RateLimitException)
@limits(calls=10, period=SECOND)
def get_call_data(url, params):
    return requests.get(url, params)


def sleuth_multiverse(row):
    login_id = ip[0]
    phone = re.sub('\W+', '', row['line'])
    sleuth_stage_id = row['original_id']
    #sleuth_stage_id = row['id']
    source_id = row['source_id']
    co_id = fix_coid(row['co_id'])
    original_contact_id = row['original_id']
    first_name = row['first_name']
    last_name = row['last_name']
    full_name = row['full_name']
    line = row['line']
    params = {"phone": phone, "api_key": wp_api_key}
    response = get_call_data(wp_url_base, params)
    sleuth_api_status = response.status_code
    row['API_Status'] = response.status_code
    if row['API_Status'] == 200:
        try:
            data = response.json()
            WP_id = data['id']
            country_calling_code = data['country_calling_code']
            line_type = data['line_type']
            carrier = data['carrier']
            is_prepaid = data['is_prepaid']
            is_commercial = data['is_commercial']
            owner_id = (data['belongs_to'][0]['id'] if data['belongs_to'] else '')
            owner_type = (data['belongs_to'][0]['type'] if data['belongs_to'] else '')
            owner_name = (data['belongs_to'][0]['name'] if data['belongs_to'] else '')
            owner_firstname = (data['belongs_to'][0]['firstname'] if data['belongs_to'] else '')
            owner_middlename = (data['belongs_to'][0]['middlename'] if data['belongs_to'] else '')
            owner_lastname = (data['belongs_to'][0]['lastname'] if data['belongs_to'] else '')
            owner_age_range = (data['belongs_to'][0]['age_range'] if data['belongs_to'] else '')
            owner_gender = (data['belongs_to'][0]['gender'] if data['belongs_to'] else '')
            owner_start_date = (data['belongs_to'][0]['link_to_phone_start_date'] if data['belongs_to'] else '')
            current_address_id = (data['current_addresses'][0]['id'] if data['current_addresses'] else '')
            current_address_type = (data['current_addresses'][0]['location_type'] if data['current_addresses'] else '')
            current_street_line1 = (data['current_addresses'][0]['street_line_1'] if data['current_addresses'] else '')
            current_street_line2 = (data['current_addresses'][0]['street_line_2'] if data['current_addresses'] else '')
            current_postal_code = (data['current_addresses'][0]['postal_code'] if data['current_addresses'] else '')
            current_zip4 = (data['current_addresses'][0]['zip4'] if data['current_addresses'] else '')
            current_state_code = (data['current_addresses'][0]['state_code'] if data['current_addresses'] else '')
            current_country_code = (data['current_addresses'][0]['country_code'] if data['current_addresses'] else '')
            current_latitude = (data['current_addresses'][0]['lat_long']['latitude'] if data['current_addresses'] else '')
            current_longitude = (data['current_addresses'][0]['lat_long']['longitude'] if data['current_addresses'] else '')
            current_address_accuracy = (data['current_addresses'][0]['lat_long']['accuracy'] if data['current_addresses'] else '')
            current_address_active = (data['current_addresses'][0]['is_active'] if data['current_addresses'] else '')
            current_address_delivery_point = (data['current_addresses'][0]['delivery_point'] if data['current_addresses'] else '')
            current_address_in_use = (data['current_addresses'][0]['link_to_person_start_date'] if data['current_addresses'] else '')
            historical_addy = data['historical_addresses']
            historical_list = [str(addy['lat_long']['latitude']) + "_" + str(addy['lat_long']['longitude']) for addy in historical_addy]
            historical_addresses = (str(", ".join(historical_list)) if data['historical_addresses'] else None)
            associated_people = data['associated_people']
            second_most_associated_person_id = (associated_people[0]['id'] if associated_people else None)
            second_most_associated_name = (associated_people[0]['name'] if associated_people else None)
            second_most_associated_firstname = (associated_people[0]['firstname'] if associated_people else None)
            second_most_associated_middlename = (associated_people[0]['middlename'] if associated_people else None)
            second_most_associated_lastname = (associated_people[0]['lastname'] if associated_people else None)
            second_most_associated_relation = (associated_people[0]['relation'] if associated_people else None)
            third_most_associated_person_id = (associated_people[1]['id'] if associated_people else None)
            third_most_associated_name = (associated_people[1]['name'] if associated_people else None)
            third_most_associated_firstname = (associated_people[1]['firstname'] if associated_people else None)
            third_most_associated_middlename = (associated_people[1]['middlename'] if associated_people else None)
            third_most_associated_lastname = (associated_people[1]['lastname'] if associated_people else None)
            third_most_associated_relation = (associated_people[1]['relation'] if associated_people else None)
            alternate_phones = (str(", ".join(data['alternate_phones'])) if data['alternate_phones'] else None)
            warnings = (data['warnings'][0] if data['warnings'] else None)
            errors = (data['error'][0] if data['error'] else None)
            data = [login_id, sleuth_stage_id, source_id, co_id, original_contact_id, first_name, last_name,
                    full_name, line, sleuth_api_status, WP_id, country_calling_code, line_type, carrier,
                    is_prepaid, is_commercial, owner_id, owner_type, owner_name, owner_firstname,
                    owner_middlename, owner_lastname, owner_age_range, owner_gender, owner_start_date,
                    current_address_id, current_address_type, current_street_line1, current_street_line2,
                    current_postal_code, current_zip4, current_state_code, current_country_code,
                    current_latitude, current_longitude, current_address_accuracy, current_address_active,
                    current_address_delivery_point, current_address_in_use, historical_addresses,
                    second_most_associated_person_id, second_most_associated_name,
                    second_most_associated_firstname, second_most_associated_middlename,
                    second_most_associated_lastname, second_most_associated_relation,
                    third_most_associated_person_id, third_most_associated_name,
                    third_most_associated_firstname, third_most_associated_middlename,
                    third_most_associated_lastname, third_most_associated_relation, alternate_phones]
            write_to_files(session_files[0], data)
        except UnicodeEncodeError:
            data = (str(sys.exc_info()[0].__name__), login_id, sleuth_stage_id)
            write_to_files(session_files[1], data)
        except IndexError:
            data = [login_id, sleuth_stage_id, source_id, co_id, original_contact_id, first_name, last_name, full_name, line, sleuth_api_status]
            write_to_files(session_files[0], data)
        except pymysql.err.ProgrammingError:
            data = [login_id, sleuth_stage_id, source_id, co_id, original_contact_id, first_name, last_name, full_name, line,sleuth_api_status]
            write_to_files(session_files[0], data)
        except:
            data = (str(sys.exc_info()[0].__name__), login_id, sleuth_stage_id)
            write_to_files(session_files[1], data)
    else:
        try:
            data = (str("There was an issue processing this line"), login_id, sleuth_stage_id)
            write_to_files(session_files[1], data)
        except:
            data = (str(sys.exc_info()[0].__name__), login_id, sleuth_stage_id)
            write_to_files(session_files[1], data)


def clear_file(filename):
    f = open(filename, "w+")
    f.close()


def fix_coid(co_id):
    try:
        return str(co_id).replace(".0","")
    except:
        return ''


def personal_analysis_function(row):
    try:
        co_id = fix_coid(row['co_id'])
        api_call_id = row['id']
        original_fn_ipa = (str(doublemetaphone(str(row['first_name']))).replace("'", '').replace("(", '').replace(")", '').replace(",",''))
        original_ln_ipa = (str(doublemetaphone(str(row['last_name']))).replace("'", '').replace("(", '').replace(")", '').replace(",",''))
        owner_fn_ipa = (str(doublemetaphone(str(row['owner_firstname']) if row['owner_name'] else ''))).replace("'",'').replace("(", '').replace(")", '').replace(",", '')
        owner_ln_ipa = (str(doublemetaphone(str(row['owner_lastname']) if row['owner_name'] else ''))).replace("'", '').replace("(", '').replace(")", '').replace(",", '')
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
        elif (row['owner_name'] == '' or row['owner_name'] is None  or row['owner_name'] == 'NULL') and \
                (row['line_type'] == '' or row['line_type'] is None or row['line_type'] == 'NULL'):
            line_status = 9
        elif (row['owner_name'] == '' or row['owner_name'] is None or row['owner_name'] == 'NULL') and \
                (row['line_type'] != '' or row['line_type'] is None or row['line_type'] == 'NULL'):
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
            vars = (api_call_id, row['sleuth_stage_id'], row['source_id'], co_id, row['original_contact_id'],
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
        elif (row['owner_name'] == '' or row['owner_name'] == None or row['owner_name'] == 'NULL') and \
                (row['line_type'] != '' or row['line_type'] == None or row['line_type'] == 'NULL'):
            line_status = 10
        else:
            line_status = str('')
        if line_status in (9,10,11):
            vars = (api_call_id, row['sleuth_stage_id'], row['source_id'], co_id, row['original_contact_id'],
                    row['first_name'], row['last_name'], row['full_name'], row['line'], row['sleuth_api_status'],
                    row['country_calling_code'], row['line_type'], row['carrier'], row['is_prepaid'], row['is_commercial'],
                    row['owner_type'],row['owner_name'], row['2nd_most_associated_name'], row['warnings'],
                    owner_confidence, sec_confidence, phone_confidence, '', '', '', line_status)
            write_to_files(session_files[3], vars)
    except:
        sleuth_id = row['sleuth_stage_id']
        data = (str(sys.exc_info()[0].__name__), '', sleuth_id)
        write_to_files(session_files[1], data)


# cats = get_rv_company_list()
# co_cat = cats[0]
# name_tuples = cats[1]
def company_analysis_function(row):
    # try:
    api_call_id = row['id']
    co_id = fix_coid(row['co_id'])
    stripped = str(str(row['owner_name']).lower()).replace(' ','')
    analysis_cat = ''.join(ch for ch in str(str(row['owner_name']).lower() +
                                            co_id.lower()) if ch not in exclude).replace(' ','')
    if str(analysis_cat) in co_cat:
        line_status = 12
    else:
        line_status = str('')
    if line_status not in (12, 13):
        co_id_list = [i for i in name_tuples if i[0] == co_id]
        relevant_list = [i[1] for i in co_id_list]
        try:
            if stripped in any(stripped in i for i in relevant_list):
                line_status = 12
            else:
                line_status = str('')
        except:
            line_status = str('')
    if line_status in (12,13):
        vars = (api_call_id, row['sleuth_stage_id'], row['source_id'], co_id, row['original_contact_id'],
                row['first_name'], row['last_name'], row['full_name'], row['line'], row['sleuth_api_status'],
                row['country_calling_code'], row['line_type'], row['carrier'], row['is_prepaid'], row['is_commercial'],
                row['owner_type'], row['owner_name'], row['2nd_most_associated_name'], row['warnings'],
                '', '', '', '98', '', '', line_status)
        write_to_files(session_files[4], vars)
    # except:
    #     sleuth_id = row['sleuth_stage_id']
    #     data = (str(sys.exc_info()[0].__name__), '', sleuth_id)
    #     write_to_files(session_files[1], data)


def reconsidered_analysis_function(row):
    try:
        api_call_id = row['id']
        co_id = fix_coid(row['co_id'])
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
        owner_ln_original_fn = (levenshtein(owner_ln_ipa, original_fn_ipa))
        owner_fn_orginal_ln = (levenshtein(owner_fn_ipa, original_ln_ipa))
        sec_ln_original_fn = (levenshtein(sec_ln_ipa, original_fn_ipa))
        sec_fn_original_ln = (levenshtein(sec_fn_ipa, original_ln_ipa))
        owner_co_id_cats.append(str(str(row['owner_name']) + '@' + str(co_id)))
        owner_confidence = (owner_fn_levy * .175) + (owner_ln_levy * .825)
        sec_confidence = (sec_fn_levy * .175) + (sec_ln_levy * .825)
        phone_confidence = max(((owner_fn_levy * .175) + (owner_ln_levy * .825)), ((sec_fn_levy * .175) + (sec_ln_levy * .825)))
        if sec_ln_levy == 100:
            line_status = 14
        elif owner_fn_levy == 100:
            line_status = 15
        elif owner_fn_orginal_ln == 100:
            line_status = 16
        elif owner_ln_original_fn == 100:
            line_status = 16
        elif sec_fn_original_ln == 100:
            line_status = 17
        elif sec_ln_original_fn == 100:
            line_status = 17
        elif (row['is_prepaid'] == '' or row['is_prepaid'] is None or row['is_prepaid'] == 'NULL') and \
                (row['owner_type'] != 'Person'):
            line_status = 18
        elif (row['is_prepaid'] == 'TRUE'):
            line_status = 19
        elif (len(row['line']) > 1) and (row['owner_type'] == 'Business'):
            line_status = 20
        elif (row['owner_type'] == 'Person'):
            line_status = 21
        else:
            line_status = 22
        if line_status in (14,15,16,17,18,19,20,21,22):
            vars = (api_call_id, row['sleuth_stage_id'], row['source_id'], co_id, row['original_contact_id'],
                        row['first_name'], row['last_name'], row['full_name'], row['line'], row['sleuth_api_status'],
                        row['country_calling_code'], row['line_type'], row['carrier'], row['is_prepaid'], row['is_commercial'],
                        row['owner_type'], row['owner_name'], row['2nd_most_associated_name'], row['warnings'],
                        owner_confidence, sec_confidence, phone_confidence, '', '', '', line_status)
            write_to_files(session_files[5], vars)
    except UnicodeEncodeError:
        owner_confidence = 0
        sec_confidence = 0
        phone_confidence = 0
        api_call_id = row['id']
        co_id = fix_coid(row['co_id'])
        vars = (api_call_id, row['sleuth_stage_id'], row['source_id'], co_id, row['original_contact_id'],
                    row['first_name'], row['last_name'], row['full_name'], row['line'], row['sleuth_api_status'],
                    row['country_calling_code'], row['line_type'], row['carrier'], row['is_prepaid'], row['is_commercial'],
                    row['owner_type'], row['owner_name'], row['2nd_most_associated_name'], row['warnings'],
                    owner_confidence, sec_confidence, phone_confidence, '', '', '', '')
        write_to_files(session_files[5], vars)
    except:
        sleuth_id = row['sleuth_stage_id']
        data = (str(sys.exc_info()[0].__name__), '', sleuth_id)
        write_to_files(session_files[1], data)


def finalize(source):
    data = get_data("""SELECT analysis_id, sleuth_stage_id, api_call_id, source_id, co_id, original_contact_id, first_name, 
                       last_name, line, line_type, line_status FROM sleuth.sleuth_analysis WHERE source_id = {0}"""
                    .format(source))
    data['line_status'] = data['line_status'].astype(int)
    statuses = get_data("""SELECT line_status, display_name FROM sleuth.sleuth_status""")
    statuses['line_status'] = statuses['line_status'].astype(int)
    merged = pandas.merge(data, statuses, on=['line_status'])
    merged['DORG_linetype'] = merged['line_type'].apply(dorg_line)
    merged['line_status_display_name'] = merged['display_name']
    merged = merged.drop(columns='display_name')
    merged.to_csv(session_files[7], index=False)
    import_csvs(session_files[7], 'sleuth_results')


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


def run_match_logic(evaluation_set, source):
    analysis_columns = ['api_call_id', 'sleuth_stage_id', 'source_id', 'co_id', 'original_contact_id', 'first_name',
                        'last_name', 'full_name', 'line', 'sleuth_api_status', 'country_calling_code', 'line_type',
                        'carrier', 'is_prepaid', 'is_commercial','owner_type','owner_name', '2nd_most_associated_name',
                        'warnings', 'owner_confidence', 'sec_confidence', 'phone_confidence', 'company_name_confidence',
                        'co_name_unions', 'matched', 'line_status']
    evaluation_set = pandas.read_csv(session_files[2])
    run_thread(evaluation_set, personal_analysis_function, 1000)
    evaluation_set['api_call_id'] = evaluation_set['id']
    personal_analysis_set = pandas.read_csv(session_files[3])
    personal_analysis_set.columns = analysis_columns
    clear_file(session_files[3])
    personal_analysis_set.to_csv(session_files[3], index=False)
    evaluation_set.drop(evaluation_set[evaluation_set['api_call_id'].isin(personal_analysis_set['api_call_id']) == True].index,
                        inplace=True)
    clear_file(session_files[2])
    evaluation_set.to_csv(session_files[2], index=False)
    # evaluation_set = pandas.read_csv("/Users/andrewharris/Desktop/Push Folder/Sleuth Terminal/07:08:2019_08.36.19_test_file_job/evaluation_set2.csv")
    run_thread(evaluation_set, company_analysis_function, 1000)
    corporate_analysis_set = pandas.read_csv(session_files[4])
    corporate_analysis_set.columns = analysis_columns
    clear_file(session_files[4])
    corporate_analysis_set.to_csv(session_files[4], index=False)
    evaluation_set['api_call_id'] = evaluation_set['id']
    evaluation_set.drop(evaluation_set[evaluation_set['api_call_id'].isin(corporate_analysis_set['api_call_id']) == True]
                        .index,inplace=True)
    # evaluation_set.to_csv("/Users/andrewharris/Desktop/Push Folder/Sleuth Terminal/07:08:2019_08.36.19_test_file_job/evaluation_set3.csv", index=False)
    run_thread(evaluation_set, reconsidered_analysis_function, 1000)
    recon_analysis_set = pandas.read_csv(session_files[5])
    recon_analysis_set.columns = analysis_columns
    clear_file(session_files[5])
    recon_analysis_set.to_csv(session_files[5], index=False)
    person = pandas.read_csv(session_files[3])
    corporate = pandas.read_csv(session_files[4])
    recon = pandas.read_csv(session_files[5])
    final = person.append(corporate)
    final = final.append(recon)
    final.to_csv(session_files[6], index=False)
    import_csvs(session_files[6], 'sleuth_analysis')


def main(input_file, job_name, id, cid, phone_number, name, outfile, mode):
    #log ip address
    IPAddr = socket.gethostbyname(socket.gethostname())
    ip.append(str(IPAddr))

    # create folder for batch_processes
    path = str(datetime.datetime.now().strftime("%m:%d:%Y_%H.%M.%S")) + \
           str("_" + str(input_file.split("/")[-1]).split(".")[0] + "_job")
    os.mkdir(path)
    # path = str("/Users/andrewharris/Desktop/Push Folder/Sleuth Terminal/07:08:2019_08.36.19_test_file_job/")
    session_files.append(str(path + "/api_data.csv"))
    session_files.append(str(path + "/error_data.csv"))
    session_files.append(str(path + "/evaluation_set.csv"))
    session_files.append(str(path + "/personal_analysis_data.csv"))
    session_files.append(str(path + "/corporate_analysis_data.csv"))
    session_files.append(str(path + "/recon_analysis_data.csv"))
    session_files.append(str(path + "/final_analysis_data.csv"))
    session_files.append(str(path + "/results.csv"))

    #create source for job
    query = """INSERT INTO sleuth.sleuth_source(name) VALUES('{0}')""".format(job_name)
    window_update('sleuth_terminal', IPAddr, query)
    query = """SELECT source_id FROM sleuth.sleuth_source ORDER BY source_id DESC LIMIT 1"""
    source = get_data(query)['source_id'].iloc[0]

    #configure file for import
    if input_file.endswith(".xlsx"):
        data = pandas.read_excel(input_file, index_col=None)
    else:
        data = pandas.read_csv(input_file, error_bad_lines=False)
    print(data.info())
    data = data[[cid, id, name, phone_number]]
    data.columns = ['co_id', 'original_id', 'full_name', 'line']
    print(data.info())
    data['source_id'] = source
    data['first_name'] = data['full_name'].apply(name_split, args=['first'])
    data['last_name'] = data['full_name'].apply(name_split, args=['last'])
    data = data[['source_id', 'co_id', 'original_id', 'first_name', 'last_name', 'full_name', 'line']]
    print(data.info())

    #prep line status for relevant stage file
    data['line'] = data['line'].apply(correct_line, args=['line'])
    data['iso_code'] = data['line'].apply(correct_line, args=['iso'])
    data['status'] = data['line'].apply(correct_line, args=['status'])
    data = data[['source_id', 'co_id', 'original_id', 'first_name', 'last_name', 'full_name', 'line', 'status']]
    data['duplicate'] = data.duplicated('line', keep=False)
    data['status'] = data.apply(dupe_fix, axis=1)

    #trim known bad lines
    if mode == 0:
        data['line_stripped'] = data['line'].apply(strip_phones)
        bad_lines = get_dm_data("""SELECT DISTINCT(number) AS 'line_stripped' FROM RO.live_numbers_blacklist""")
        data = pandas.merge(data, bad_lines, on=['line_stripped'], how='left', indicator='Exist')
        data['line_status'] = numpy.where(data.Exist == 'both', '2', '0')
        data['status'] = data.apply(status, axis=1)
    else:
        pass
    data = data[['source_id', 'co_id', 'original_id', 'first_name', 'last_name', 'full_name', 'line', 'status']]
    data = data.rename(columns={'status': 'line_status'})
    data.to_csv((path + "/data_staged_for_ingestion.csv"), index=False)
    import_csvs(str(path + "/data_staged_for_ingestion.csv"), 'sleuth_stage')

    #section data for api
    done_set = data[data['line_status'] != '0']
    done_set.to_csv((path + "/finished_data_set.csv"), index=False)
    call_set = data[data['line_status'] == '0']
    call_set.to_csv((path + "/processing_data_set.csv"), index=False)

    #begin api calls
    run_thread(call_set, sleuth_multiverse, 5000)
    data = pandas.read_csv(session_files[0])
    clear_file(session_files[0])
    data.columns = ['login_id', 'sleuth_stage_id', 'source_id', 'co_id', 'original_contact_id', 'first_name',
                    'last_name',
                    'full_name', 'line', 'sleuth_api_status', 'WP_id', 'country_calling_code', 'line_type', 'carrier',
                    'is_prepaid', 'is_commercial', 'owner_id', 'owner_type', 'owner_name', 'owner_firstname',
                    'owner_middlename', 'owner_lastname', 'owner_age_range', 'owner_gender', 'owner_start_date',
                    'current_address_id', 'current_address_type', 'current_street_line1', 'current_street_line2',
                    'current_postal_code', 'current_zip4', 'current_state_code', 'current_country_code',
                    'current_latitude', 'current_longitude', 'current_address_accuracy', 'current_address_active',
                    'current_address_delivery_point', 'current_address_in_use', 'historical_addresses',
                    '2nd_most_associated_person_id', '2nd_most_associated_name',
                    '2nd_most_associated_firstname', '2nd_most_associated_middlename',
                    '2nd_most_associated_lastname', '2nd_most_associated_relation',
                    '3rd_most_associated_person_id', '3rd_most_associated_name',
                    '3rd_most_associated_firstname', '3rd_most_associated_middlename',
                    '3rd_most_associated_lastname', '3rd_most_associated_relation', 'alternate_phones', 'warnings',
                    'errors']
    data['co_id'] = data['co_id'].apply(fix_coid)
    data.to_csv(session_files[0], index=False)
    import_csvs(session_files[0], 'sleuth_api_call')

    #evaluate processed data
    evaluation_set = get_data(str("""SELECT id, sleuth_stage_id, source_id, co_id, original_contact_id, first_name,
                                            last_name, full_name, line, sleuth_api_status, country_calling_code,
                                            line_type, carrier, is_prepaid, is_commercial, owner_type, owner_name,
                                            owner_firstname, owner_lastname, 2nd_most_associated_name,
                                            2nd_most_associated_firstname, 2nd_most_associated_lastname, warnings
                                     FROM sleuth.sleuth_api_call WHERE source_id = {0}""").format(source))
    evaluation_set.to_csv(session_files[2], index=False)
    run_match_logic(evaluation_set, source)
    finalize(source)


# main('/Users/andrewharris/Desktop/Push Folder/Sleuth Terminal/190702_phone_health_sleuth.csv',
#      'July Health Check',
#      'original_id',
#      'co_id',
#      'line',
#      'full_name',
#      '/Users/andrewharris/Desktop/Push Folder/Sleuth Terminal/test_file.csv',
#      1)
# import_csvs("/Users/andrewharris/Desktop/Push Folder/Sleuth Terminal/07:08:2019_15.49.15_190702_phone_health_sleuth_job/api_data.csv",
#             "sleuth_api_call")

# if __name__ == '__main__':
#     # Command line arguments.
#     parser = argparse.ArgumentParser(description=__doc__)
#     parser.add_argument('-i', '--input_file', help='The path to the file you want to run', required=True)
#     parser.add_argument('-jn', '--job_name', help="What you want to call this batch process", required=True)
#     parser.add_argument('-id', '--id', help='Column containing a Unique Row ID', required=True)
#     parser.add_argument('-cid', '--co_id', help='Column containing Company ID', required=True)
#     parser.add_argument('-pn','--phone_number', help='Column containing phone number', required=True)
#     parser.add_argument('-n', '--name', help="Column containing the contact's name", required=True)
#     parser.add_argument('-o', '--outfile', help='The path to the file you ', required=True)
#     parser.add_argument('-m', '--mode', help='"0" for STANDARD PROCESSING\n "1" for HEALTH CHECK', required=True)
#
#     args = parser.parse_args()
#
#     main(args.input_file, args.job_name, args.id, args.cid, args.phone_number, args.name, args.outfile, args.mode)
#
import os
import gc
import re
import csv
import sys
import time
import xlrd
import boto3
import numpy
import pylev
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
import strategic_detective
from collections import Counter
from nameparser import HumanName
from metaphone import doublemetaphone
from backoff import on_exception, expo
from strategic_detective import run_match_logic
from ratelimit import limits, RateLimitException
from multiprocessing.dummy import Pool as ThreadPool

def getParameter(param_name):
# Create the SSM Client
    ssm = boto3.client('ssm', region_name='us-west-2')
# Get the requested parameter
    response = ssm.get_parameters(Names=[param_name,],WithDecryption=True)
# Store the credentials in a variable
    credentials = response['Parameters'][0]['Value']
    return credentials

country_abr = ['AF', 'AL', 'DZ', 'AS', 'AD', 'AO', 'AI', 'AQ', 'AG', 'AR', 'AM', 'AW', 'AU', 'AT', 'AZ', 'BS', 'BH',
               'BD', 'BB', 'BY', 'BE', 'BZ', 'BJ', 'BM', 'BT', 'BO', 'BQ', 'BA', 'BW', 'BV', 'BR', 'IO', 'BN', 'BG',
               'BF', 'BI', 'CV', 'KH', 'CM', 'KY', 'CF', 'TD', 'CL', 'CN', 'CX', 'CC', 'CO', 'KM', 'CD', 'CG', 'CK',
               'CR', 'HR', 'CU', 'CW', 'CY', 'CZ', 'CI', 'DK', 'DJ', 'DM', 'DO', 'EC', 'EG', 'SV', 'GQ', 'ER', 'EE',
               'SZ', 'ET', 'FK', 'FO', 'FJ', 'FI', 'FR', 'GF', 'PF', 'TF', 'GA', 'GM', 'GE', 'DE', 'GH', 'GI', 'GR',
               'GL', 'GD', 'GP', 'GU', 'GT', 'GG', 'GN', 'GW', 'GY', 'HT', 'HM', 'VA', 'HN', 'HK', 'HU', 'IS', 'IN',
               'ID', 'IR', 'IQ', 'IE', 'IM', 'IL', 'IT', 'JM', 'JP', 'JE', 'JO', 'KZ', 'KE', 'KI', 'KP', 'KR', 'KW',
               'KG', 'LA', 'LV', 'LB', 'LS', 'LR', 'LY', 'LI', 'LT', 'LU', 'MO', 'MK', 'MG', 'MW', 'MY', 'MV', 'ML',
               'MT', 'MH', 'MQ', 'MR', 'MU', 'YT', 'MX', 'FM', 'MD', 'MC', 'MN', 'ME', 'MS', 'MA', 'MZ', 'MM', 'NA',
               'NR', 'NP', 'NL', 'NC', 'NZ', 'NI', 'NE', 'NG', 'NU', 'NF', 'MP', 'NO', 'OM', 'PK', 'PW', 'PS', 'PA',
               'PG', 'PY', 'PE', 'PH', 'PN', 'PL', 'PT', 'PR', 'QA', 'RO', 'RU', 'RW', 'RE', 'BL', 'SH', 'KN', 'LC',
               'MF', 'PM', 'VC', 'WS', 'SM', 'ST', 'SA', 'SN', 'RS', 'SC', 'SL', 'SG', 'SX', 'SK', 'SI', 'SB', 'SO',
               'ZA', 'GS', 'SS', 'ES', 'LK', 'SD', 'SR', 'SJ', 'SE', 'CH', 'SY', 'TW', 'TJ', 'TZ', 'TH', 'TL', 'TG',
               'TK', 'TO', 'TT', 'TN', 'TR', 'TM', 'TC', 'TV', 'UG', 'UA', 'AE', 'GB', 'UY', 'UZ', 'VU', 'VE', 'VN',
               'VG', 'VI', 'WF', 'EH', 'YE', 'ZM', 'ZW', 'AX', ]

#retrieve all parameters for the application
server = 'sleuthdb.cibydfkixtvw.us-west-2.rds.amazonaws.com'
account_sid = getParameter('icehook-accountsid')
auth_token = getParameter('icehook-authtoken')
sleuth_pass = getParameter('rops-sleuth-pwd')
wp_api_key = getParameter('rops-white-pages-api-key')
wp_url_base = "https://proapi.whitepages.com/3.0/phone?"
exclude = set(string.punctuation)
SECOND = 1
# client = Client(account_sid, auth_token)
owner_co_id_cats = []
session_files = []
ip = []

def window_update(id, login_id, query):
    connection = pymysql.connect(host=server,
                                 user='app_sleuth',
                                 password=sleuth_pass,
                                 db='sleuth',
                                 charset='utf8',
                                 use_unicode=True)
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        connection.close()
    except:
        cursor.execute("""INSERT INTO sleuth.sleuth_errors(error, sleuth_stage_id, login_id) 
                          VALUES('{0}','{1}','{2}')""".format(*(str(sys.exc_info()[0].__name__), id, login_id)))
        connection.commit()
        connection.close()
        print('Error, check the error logs.')


def import_csvs(path, db):
    print(str("Importing file : " + path))
    # try:
    database_username = 'app_sleuth'
    database_password = sleuth_pass
    database_ip = server
    database_name = 'sleuth'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password,
                                                          database_ip, database_name))
    frame = pandas.read_csv(path)
    frame.to_sql(con=database_connection, name=db, if_exists='append', index=False)
    print("Successful import!")
    # except:
    #     print("Failed import!")
    #     pass


def write_to_files(file, vars):
    with open(file, 'a') as f:
        writer = csv.writer(f)
        writer.writerow(vars)
    f.close


def get_data(query):
    connection = pymysql.connect(host=server, user='app_sleuth', password=sleuth_pass, db='sleuth', charset='utf8', use_unicode=True)
    data = pandas.read_sql(query,connection)
    connection.close()
    return data


def get_dm_data(query):
    connection = pymysql.connect(host='10.255.8.46', user='app_sleuth', password=sleuth_pass, db='sleuth', charset='utf8', use_unicode=True)
    data = pandas.read_sql(query,connection)
    connection.close()
    return data


def name_split(var, part):
    return HumanName(var)[part]


def co_cat_function(row):
    concat = ''.join(ch for ch in str(str(row['company_name']).lower() +
                                      str(row['company_id']).lower()) if ch not in exclude).replace(' ','')
    return concat


def levenshtein(a, b):
    len_a = len(a)
    len_b = len(b)
    distance = pylev.levenshtein(a, b)
    maxLength = max(len_a, len_b)
    result = maxLength - distance
    percentage = (result / maxLength) * 100
    return percentage


def correct_line(line, type):
    clean_line = re.sub('[^A-Za-z0-9]+', '', str(line))
    parsed_line = phonenumbers.format_number(phonenumbers.parse(clean_line, 'US'),
                                             phonenumbers.PhoneNumberFormat.INTERNATIONAL)
    iso = phonenumbers.region_code_for_number(phonenumbers.parse(clean_line, 'US'))
    clean = (0,0,0)
    if len(parsed_line) == 15 and iso in ['US', 'CA']:
        clean = (parsed_line[3:], '0', iso)
    if len(parsed_line) == 15 and iso is None:
        clean = (parsed_line[3:], '23', iso)
    if any(c.isalpha() for c in clean_line):
        clean = (parsed_line, '24', iso)
    if iso in country_abr:
        clean = (parsed_line, '24', iso)
    elif clean is (0,0,0):
        clean = (parsed_line[3:], '24', iso)
    if type == 'line':
        return clean[0]
    if type == 'status':
        return clean[1]
    if type == 'iso':
        return clean[2]


def dupe_fix(row):
    if row['duplicate'] == True:
        return '3'
    else:
        return row['status']


def strip_phones(s):
    exclude = set(string.punctuation)
    return ''.join(ch for ch in s if ch not in exclude)


def status(row):
    if row['line_status'] == '0':
        return row['status']
    else:
        return row['line_status']


def dorg_line(line_type):
    if line_type in ('Landline', 'FixedVOIP'):
        dorg_line = str('OFFICE LINE')
    elif line_type in ('Mobile', 'NonFixedVOIP'):
        dorg_line = str('MOBILE LINE')
    elif line_type == 'TollFree':
        dorg_line = str('TOLLFREE')
    else:
        dorg_line = str('CHECK DATA')
    return dorg_line


def create_headers(file, headers):
    with open(file, 'wb') as f:
        w = csv.writer(f)
        w.writerow(headers)
        for data in yourdata:
            w.writerow(data)


def strip_names(row):
    return (str(row['company_id']), str(row['company_name']).replace(" ", '').lower())


def get_rv_company_list():
    company_names = get_dm_data(str("""SELECT company_name, company_id FROM dorg.company_other_name
                                       UNION SELECT name, id FROM dorg.company"""))
    company_names['cat'] = company_names.apply(co_cat_function, axis=1)
    company_names['stripped'] = company_names.apply(strip_names, axis=1)
    return [list(company_names['cat']), list(company_names['stripped'])]


@on_exception(expo, RateLimitException)
@limits(calls=10, period=SECOND)
def get_call_data(url, params):
    return requests.get(url, params)


def sleuth_multiverse(row):
    login_id = ip[0]
    phone = re.sub('\W+', '', row['line'])
    sleuth_stage_id = row['original_id']
    #sleuth_stage_id = row['id']
    source_id = row['source_id']
    co_id = fix_coid(row['co_id'])
    original_contact_id = row['original_id']
    first_name = row['first_name']
    last_name = row['last_name']
    full_name = row['full_name']
    line = row['line']
    params = {"phone": phone, "api_key": wp_api_key}
    response = get_call_data(wp_url_base, params)
    sleuth_api_status = response.status_code
    row['API_Status'] = response.status_code
    if row['API_Status'] == 200:
        try:
            data = response.json()
            WP_id = data['id']
            country_calling_code = data['country_calling_code']
            line_type = data['line_type']
            carrier = data['carrier']
            is_prepaid = data['is_prepaid']
            is_commercial = data['is_commercial']
            owner_id = (data['belongs_to'][0]['id'] if data['belongs_to'] else '')
            owner_type = (data['belongs_to'][0]['type'] if data['belongs_to'] else '')
            owner_name = (data['belongs_to'][0]['name'] if data['belongs_to'] else '')
            owner_firstname = (data['belongs_to'][0]['firstname'] if data['belongs_to'] else '')
            owner_middlename = (data['belongs_to'][0]['middlename'] if data['belongs_to'] else '')
            owner_lastname = (data['belongs_to'][0]['lastname'] if data['belongs_to'] else '')
            owner_age_range = (data['belongs_to'][0]['age_range'] if data['belongs_to'] else '')
            owner_gender = (data['belongs_to'][0]['gender'] if data['belongs_to'] else '')
            owner_start_date = (data['belongs_to'][0]['link_to_phone_start_date'] if data['belongs_to'] else '')
            current_address_id = (data['current_addresses'][0]['id'] if data['current_addresses'] else '')
            current_address_type = (data['current_addresses'][0]['location_type'] if data['current_addresses'] else '')
            current_street_line1 = (data['current_addresses'][0]['street_line_1'] if data['current_addresses'] else '')
            current_street_line2 = (data['current_addresses'][0]['street_line_2'] if data['current_addresses'] else '')
            current_postal_code = (data['current_addresses'][0]['postal_code'] if data['current_addresses'] else '')
            current_zip4 = (data['current_addresses'][0]['zip4'] if data['current_addresses'] else '')
            current_state_code = (data['current_addresses'][0]['state_code'] if data['current_addresses'] else '')
            current_country_code = (data['current_addresses'][0]['country_code'] if data['current_addresses'] else '')
            current_latitude = (data['current_addresses'][0]['lat_long']['latitude'] if data['current_addresses'] else '')
            current_longitude = (data['current_addresses'][0]['lat_long']['longitude'] if data['current_addresses'] else '')
            current_address_accuracy = (data['current_addresses'][0]['lat_long']['accuracy'] if data['current_addresses'] else '')
            current_address_active = (data['current_addresses'][0]['is_active'] if data['current_addresses'] else '')
            current_address_delivery_point = (data['current_addresses'][0]['delivery_point'] if data['current_addresses'] else '')
            current_address_in_use = (data['current_addresses'][0]['link_to_person_start_date'] if data['current_addresses'] else '')
            historical_addy = data['historical_addresses']
            historical_list = [str(addy['lat_long']['latitude']) + "_" + str(addy['lat_long']['longitude']) for addy in historical_addy]
            historical_addresses = (str(", ".join(historical_list)) if data['historical_addresses'] else None)
            associated_people = data['associated_people']
            second_most_associated_person_id = (associated_people[0]['id'] if associated_people else None)
            second_most_associated_name = (associated_people[0]['name'] if associated_people else None)
            second_most_associated_firstname = (associated_people[0]['firstname'] if associated_people else None)
            second_most_associated_middlename = (associated_people[0]['middlename'] if associated_people else None)
            second_most_associated_lastname = (associated_people[0]['lastname'] if associated_people else None)
            second_most_associated_relation = (associated_people[0]['relation'] if associated_people else None)
            third_most_associated_person_id = (associated_people[1]['id'] if associated_people else None)
            third_most_associated_name = (associated_people[1]['name'] if associated_people else None)
            third_most_associated_firstname = (associated_people[1]['firstname'] if associated_people else None)
            third_most_associated_middlename = (associated_people[1]['middlename'] if associated_people else None)
            third_most_associated_lastname = (associated_people[1]['lastname'] if associated_people else None)
            third_most_associated_relation = (associated_people[1]['relation'] if associated_people else None)
            alternate_phones = (str(", ".join(data['alternate_phones'])) if data['alternate_phones'] else None)
            warnings = (data['warnings'][0] if data['warnings'] else None)
            errors = (data['error'][0] if data['error'] else None)
            data = [login_id, sleuth_stage_id, source_id, co_id, original_contact_id, first_name, last_name,
                    full_name, line, sleuth_api_status, WP_id, country_calling_code, line_type, carrier,
                    is_prepaid, is_commercial, owner_id, owner_type, owner_name, owner_firstname,
                    owner_middlename, owner_lastname, owner_age_range, owner_gender, owner_start_date,
                    current_address_id, current_address_type, current_street_line1, current_street_line2,
                    current_postal_code, current_zip4, current_state_code, current_country_code,
                    current_latitude, current_longitude, current_address_accuracy, current_address_active,
                    current_address_delivery_point, current_address_in_use, historical_addresses,
                    second_most_associated_person_id, second_most_associated_name,
                    second_most_associated_firstname, second_most_associated_middlename,
                    second_most_associated_lastname, second_most_associated_relation,
                    third_most_associated_person_id, third_most_associated_name,
                    third_most_associated_firstname, third_most_associated_middlename,
                    third_most_associated_lastname, third_most_associated_relation, alternate_phones]
            write_to_files(session_files[0], data)
        except UnicodeEncodeError:
            data = (str(sys.exc_info()[0].__name__), login_id, sleuth_stage_id)
            write_to_files(session_files[1], data)
        except IndexError:
            data = [login_id, sleuth_stage_id, source_id, co_id, original_contact_id, first_name, last_name, full_name, line, sleuth_api_status]
            write_to_files(session_files[0], data)
        except pymysql.err.ProgrammingError:
            data = [login_id, sleuth_stage_id, source_id, co_id, original_contact_id, first_name, last_name, full_name, line,sleuth_api_status]
            write_to_files(session_files[0], data)
        except:
            data = (str(sys.exc_info()[0].__name__), login_id, sleuth_stage_id)
            write_to_files(session_files[1], data)
    else:
        try:
            data = (str("There was an issue processing this line"), login_id, sleuth_stage_id)
            write_to_files(session_files[1], data)
        except:
            data = (str(sys.exc_info()[0].__name__), login_id, sleuth_stage_id)
            write_to_files(session_files[1], data)


def clear_file(filename):
    f = open(filename, "w+")
    f.close()


def fix_coid(co_id):
    try:
        return str(co_id).replace(".0","")
    except:
        return ''


def personal_analysis_function(row):
    try:
        co_id = fix_coid(row['co_id'])
        api_call_id = row['id']
        original_fn_ipa = (str(doublemetaphone(str(row['first_name']))).replace("'", '').replace("(", '').replace(")", '').replace(",",''))
        original_ln_ipa = (str(doublemetaphone(str(row['last_name']))).replace("'", '').replace("(", '').replace(")", '').replace(",",''))
        owner_fn_ipa = (str(doublemetaphone(str(row['owner_firstname']) if row['owner_name'] else ''))).replace("'",'').replace("(", '').replace(")", '').replace(",", '')
        owner_ln_ipa = (str(doublemetaphone(str(row['owner_lastname']) if row['owner_name'] else ''))).replace("'", '').replace("(", '').replace(")", '').replace(",", '')
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
        elif (row['owner_name'] == '' or row['owner_name'] is None  or row['owner_name'] == 'NULL') and \
                (row['line_type'] == '' or row['line_type'] is None or row['line_type'] == 'NULL'):
            line_status = 9
        elif (row['owner_name'] == '' or row['owner_name'] is None or row['owner_name'] == 'NULL') and \
                (row['line_type'] != '' or row['line_type'] is None or row['line_type'] == 'NULL'):
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
            vars = (api_call_id, row['sleuth_stage_id'], row['source_id'], co_id, row['original_contact_id'],
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
        elif (row['owner_name'] == '' or row['owner_name'] == None or row['owner_name'] == 'NULL') and \
                (row['line_type'] != '' or row['line_type'] == None or row['line_type'] == 'NULL'):
            line_status = 10
        else:
            line_status = str('')
        if line_status in (9,10,11):
            vars = (api_call_id, row['sleuth_stage_id'], row['source_id'], co_id, row['original_contact_id'],
                    row['first_name'], row['last_name'], row['full_name'], row['line'], row['sleuth_api_status'],
                    row['country_calling_code'], row['line_type'], row['carrier'], row['is_prepaid'], row['is_commercial'],
                    row['owner_type'],row['owner_name'], row['2nd_most_associated_name'], row['warnings'],
                    owner_confidence, sec_confidence, phone_confidence, '', '', '', line_status)
            write_to_files(session_files[3], vars)
    except:
        sleuth_id = row['sleuth_stage_id']
        data = (str(sys.exc_info()[0].__name__), '', sleuth_id)
        write_to_files(session_files[1], data)


# cats = get_rv_company_list()
# co_cat = cats[0]
# name_tuples = cats[1]
def company_analysis_function(row):
    # try:
    api_call_id = row['id']
    co_id = fix_coid(row['co_id'])
    stripped = str(str(row['owner_name']).lower()).replace(' ','')
    analysis_cat = ''.join(ch for ch in str(str(row['owner_name']).lower() +
                                            co_id.lower()) if ch not in exclude).replace(' ','')
    if str(analysis_cat) in co_cat:
        line_status = 12
    else:
        line_status = str('')
    if line_status not in (12, 13):
        co_id_list = [i for i in name_tuples if i[0] == co_id]
        relevant_list = [i[1] for i in co_id_list]
        try:
            if stripped in any(stripped in i for i in relevant_list):
                line_status = 12
            else:
                line_status = str('')
        except:
            line_status = str('')
    if line_status in (12,13):
        vars = (api_call_id, row['sleuth_stage_id'], row['source_id'], co_id, row['original_contact_id'],
                row['first_name'], row['last_name'], row['full_name'], row['line'], row['sleuth_api_status'],
                row['country_calling_code'], row['line_type'], row['carrier'], row['is_prepaid'], row['is_commercial'],
                row['owner_type'], row['owner_name'], row['2nd_most_associated_name'], row['warnings'],
                '', '', '', '98', '', '', line_status)
        write_to_files(session_files[4], vars)
    # except:
    #     sleuth_id = row['sleuth_stage_id']
    #     data = (str(sys.exc_info()[0].__name__), '', sleuth_id)
    #     write_to_files(session_files[1], data)


def reconsidered_analysis_function(row):
    try:
        api_call_id = row['id']
        co_id = fix_coid(row['co_id'])
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
        owner_ln_original_fn = (levenshtein(owner_ln_ipa, original_fn_ipa))
        owner_fn_orginal_ln = (levenshtein(owner_fn_ipa, original_ln_ipa))
        sec_ln_original_fn = (levenshtein(sec_ln_ipa, original_fn_ipa))
        sec_fn_original_ln = (levenshtein(sec_fn_ipa, original_ln_ipa))
        owner_co_id_cats.append(str(str(row['owner_name']) + '@' + str(co_id)))
        owner_confidence = (owner_fn_levy * .175) + (owner_ln_levy * .825)
        sec_confidence = (sec_fn_levy * .175) + (sec_ln_levy * .825)
        phone_confidence = max(((owner_fn_levy * .175) + (owner_ln_levy * .825)), ((sec_fn_levy * .175) + (sec_ln_levy * .825)))
        if sec_ln_levy == 100:
            line_status = 14
        elif owner_fn_levy == 100:
            line_status = 15
        elif owner_fn_orginal_ln == 100:
            line_status = 16
        elif owner_ln_original_fn == 100:
            line_status = 16
        elif sec_fn_original_ln == 100:
            line_status = 17
        elif sec_ln_original_fn == 100:
            line_status = 17
        elif (row['is_prepaid'] == '' or row['is_prepaid'] is None or row['is_prepaid'] == 'NULL') and \
                (row['owner_type'] != 'Person'):
            line_status = 18
        elif (row['is_prepaid'] == 'TRUE'):
            line_status = 19
        elif (len(row['line']) > 1) and (row['owner_type'] == 'Business'):
            line_status = 20
        elif (row['owner_type'] == 'Person'):
            line_status = 21
        else:
            line_status = 22
        if line_status in (14,15,16,17,18,19,20,21,22):
            vars = (api_call_id, row['sleuth_stage_id'], row['source_id'], co_id, row['original_contact_id'],
                        row['first_name'], row['last_name'], row['full_name'], row['line'], row['sleuth_api_status'],
                        row['country_calling_code'], row['line_type'], row['carrier'], row['is_prepaid'], row['is_commercial'],
                        row['owner_type'], row['owner_name'], row['2nd_most_associated_name'], row['warnings'],
                        owner_confidence, sec_confidence, phone_confidence, '', '', '', line_status)
            write_to_files(session_files[5], vars)
    except UnicodeEncodeError:
        owner_confidence = 0
        sec_confidence = 0
        phone_confidence = 0
        api_call_id = row['id']
        co_id = fix_coid(row['co_id'])
        vars = (api_call_id, row['sleuth_stage_id'], row['source_id'], co_id, row['original_contact_id'],
                    row['first_name'], row['last_name'], row['full_name'], row['line'], row['sleuth_api_status'],
                    row['country_calling_code'], row['line_type'], row['carrier'], row['is_prepaid'], row['is_commercial'],
                    row['owner_type'], row['owner_name'], row['2nd_most_associated_name'], row['warnings'],
                    owner_confidence, sec_confidence, phone_confidence, '', '', '', '')
        write_to_files(session_files[5], vars)
    except:
        sleuth_id = row['sleuth_stage_id']
        data = (str(sys.exc_info()[0].__name__), '', sleuth_id)
        write_to_files(session_files[1], data)


def finalize(source):
    data = get_data("""SELECT analysis_id, sleuth_stage_id, api_call_id, source_id, co_id, original_contact_id, first_name, 
                       last_name, line, line_type, line_status FROM sleuth.sleuth_analysis WHERE source_id = {0}"""
                    .format(source))
    data['line_status'] = data['line_status'].astype(int)
    statuses = get_data("""SELECT line_status, display_name FROM sleuth.sleuth_status""")
    statuses['line_status'] = statuses['line_status'].astype(int)
    merged = pandas.merge(data, statuses, on=['line_status'])
    merged['DORG_linetype'] = merged['line_type'].apply(dorg_line)
    merged['line_status_display_name'] = merged['display_name']
    merged = merged.drop(columns='display_name')
    merged.to_csv(session_files[7], index=False)
    import_csvs(session_files[7], 'sleuth_results')


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


def run_match_logic(evaluation_set, source):
    analysis_columns = ['api_call_id', 'sleuth_stage_id', 'source_id', 'co_id', 'original_contact_id', 'first_name',
                        'last_name', 'full_name', 'line', 'sleuth_api_status', 'country_calling_code', 'line_type',
                        'carrier', 'is_prepaid', 'is_commercial','owner_type','owner_name', '2nd_most_associated_name',
                        'warnings', 'owner_confidence', 'sec_confidence', 'phone_confidence', 'company_name_confidence',
                        'co_name_unions', 'matched', 'line_status']
    evaluation_set = pandas.read_csv(session_files[2])
    run_thread(evaluation_set, personal_analysis_function, 1000)
    evaluation_set['api_call_id'] = evaluation_set['id']
    personal_analysis_set = pandas.read_csv(session_files[3])
    personal_analysis_set.columns = analysis_columns
    clear_file(session_files[3])
    personal_analysis_set.to_csv(session_files[3], index=False)
    evaluation_set.drop(evaluation_set[evaluation_set['api_call_id'].isin(personal_analysis_set['api_call_id']) == True].index,
                        inplace=True)
    clear_file(session_files[2])
    evaluation_set.to_csv(session_files[2], index=False)
    # evaluation_set = pandas.read_csv("/Users/andrewharris/Desktop/Push Folder/Sleuth Terminal/07:08:2019_08.36.19_test_file_job/evaluation_set2.csv")
    run_thread(evaluation_set, company_analysis_function, 1000)
    corporate_analysis_set = pandas.read_csv(session_files[4])
    corporate_analysis_set.columns = analysis_columns
    clear_file(session_files[4])
    corporate_analysis_set.to_csv(session_files[4], index=False)
    evaluation_set['api_call_id'] = evaluation_set['id']
    evaluation_set.drop(evaluation_set[evaluation_set['api_call_id'].isin(corporate_analysis_set['api_call_id']) == True]
                        .index,inplace=True)
    # evaluation_set.to_csv("/Users/andrewharris/Desktop/Push Folder/Sleuth Terminal/07:08:2019_08.36.19_test_file_job/evaluation_set3.csv", index=False)
    run_thread(evaluation_set, reconsidered_analysis_function, 1000)
    recon_analysis_set = pandas.read_csv(session_files[5])
    recon_analysis_set.columns = analysis_columns
    clear_file(session_files[5])
    recon_analysis_set.to_csv(session_files[5], index=False)
    person = pandas.read_csv(session_files[3])
    corporate = pandas.read_csv(session_files[4])
    recon = pandas.read_csv(session_files[5])
    final = person.append(corporate)
    final = final.append(recon)
    final.to_csv(session_files[6], index=False)
    import_csvs(session_files[6], 'sleuth_analysis')


def main(input_file, job_name, id, cid, phone_number, name, outfile, mode):
    #log ip address
    IPAddr = socket.gethostbyname(socket.gethostname())
    ip.append(str(IPAddr))

    # create folder for batch_processes
    path = str(datetime.datetime.now().strftime("%m:%d:%Y_%H.%M.%S")) + \
           str("_" + str(input_file.split("/")[-1]).split(".")[0] + "_job")
    os.mkdir(path)
    # path = str("/Users/andrewharris/Desktop/Push Folder/Sleuth Terminal/07:08:2019_08.36.19_test_file_job/")
    session_files.append(str(path + "/api_data.csv"))
    session_files.append(str(path + "/error_data.csv"))
    session_files.append(str(path + "/evaluation_set.csv"))
    session_files.append(str(path + "/personal_analysis_data.csv"))
    session_files.append(str(path + "/corporate_analysis_data.csv"))
    session_files.append(str(path + "/recon_analysis_data.csv"))
    session_files.append(str(path + "/final_analysis_data.csv"))
    session_files.append(str(path + "/results.csv"))

    #create source for job
    query = """INSERT INTO sleuth.sleuth_source(name) VALUES('{0}')""".format(job_name)
    window_update('sleuth_terminal', IPAddr, query)
    query = """SELECT source_id FROM sleuth.sleuth_source ORDER BY source_id DESC LIMIT 1"""
    source = get_data(query)['source_id'].iloc[0]

    #configure file for import
    if input_file.endswith(".xlsx"):
        data = pandas.read_excel(input_file, index_col=None)
    else:
        data = pandas.read_csv(input_file, error_bad_lines=False)
    print(data.info())
    data = data[[cid, id, name, phone_number]]
    data.columns = ['co_id', 'original_id', 'full_name', 'line']
    print(data.info())
    data['source_id'] = source
    data['first_name'] = data['full_name'].apply(name_split, args=['first'])
    data['last_name'] = data['full_name'].apply(name_split, args=['last'])
    data = data[['source_id', 'co_id', 'original_id', 'first_name', 'last_name', 'full_name', 'line']]
    print(data.info())

    #prep line status for relevant stage file
    data['line'] = data['line'].apply(correct_line, args=['line'])
    data['iso_code'] = data['line'].apply(correct_line, args=['iso'])
    data['status'] = data['line'].apply(correct_line, args=['status'])
    data = data[['source_id', 'co_id', 'original_id', 'first_name', 'last_name', 'full_name', 'line', 'status']]
    data['duplicate'] = data.duplicated('line', keep=False)
    data['status'] = data.apply(dupe_fix, axis=1)

    #trim known bad lines
    if mode == 0:
        data['line_stripped'] = data['line'].apply(strip_phones)
        bad_lines = get_dm_data("""SELECT DISTINCT(number) AS 'line_stripped' FROM RO.live_numbers_blacklist""")
        data = pandas.merge(data, bad_lines, on=['line_stripped'], how='left', indicator='Exist')
        data['line_status'] = numpy.where(data.Exist == 'both', '2', '0')
        data['status'] = data.apply(status, axis=1)
    else:
        pass
    data = data[['source_id', 'co_id', 'original_id', 'first_name', 'last_name', 'full_name', 'line', 'status']]
    data = data.rename(columns={'status': 'line_status'})
    data.to_csv((path + "/data_staged_for_ingestion.csv"), index=False)
    import_csvs(str(path + "/data_staged_for_ingestion.csv"), 'sleuth_stage')

    #section data for api
    done_set = data[data['line_status'] != '0']
    done_set.to_csv((path + "/finished_data_set.csv"), index=False)
    call_set = data[data['line_status'] == '0']
    call_set.to_csv((path + "/processing_data_set.csv"), index=False)

    #begin api calls
    run_thread(call_set, sleuth_multiverse, 5000)
    data = pandas.read_csv(session_files[0])
    clear_file(session_files[0])
    data.columns = ['login_id', 'sleuth_stage_id', 'source_id', 'co_id', 'original_contact_id', 'first_name',
                    'last_name',
                    'full_name', 'line', 'sleuth_api_status', 'WP_id', 'country_calling_code', 'line_type', 'carrier',
                    'is_prepaid', 'is_commercial', 'owner_id', 'owner_type', 'owner_name', 'owner_firstname',
                    'owner_middlename', 'owner_lastname', 'owner_age_range', 'owner_gender', 'owner_start_date',
                    'current_address_id', 'current_address_type', 'current_street_line1', 'current_street_line2',
                    'current_postal_code', 'current_zip4', 'current_state_code', 'current_country_code',
                    'current_latitude', 'current_longitude', 'current_address_accuracy', 'current_address_active',
                    'current_address_delivery_point', 'current_address_in_use', 'historical_addresses',
                    '2nd_most_associated_person_id', '2nd_most_associated_name',
                    '2nd_most_associated_firstname', '2nd_most_associated_middlename',
                    '2nd_most_associated_lastname', '2nd_most_associated_relation',
                    '3rd_most_associated_person_id', '3rd_most_associated_name',
                    '3rd_most_associated_firstname', '3rd_most_associated_middlename',
                    '3rd_most_associated_lastname', '3rd_most_associated_relation', 'alternate_phones', 'warnings',
                    'errors']
    data['co_id'] = data['co_id'].apply(fix_coid)
    data.to_csv(session_files[0], index=False)
    import_csvs(session_files[0], 'sleuth_api_call')

    #evaluate processed data
    evaluation_set = get_data(str("""SELECT id, sleuth_stage_id, source_id, co_id, original_contact_id, first_name,
                                            last_name, full_name, line, sleuth_api_status, country_calling_code,
                                            line_type, carrier, is_prepaid, is_commercial, owner_type, owner_name,
                                            owner_firstname, owner_lastname, 2nd_most_associated_name,
                                            2nd_most_associated_firstname, 2nd_most_associated_lastname, warnings
                                     FROM sleuth.sleuth_api_call WHERE source_id = {0}""").format(source))
    evaluation_set.to_csv(session_files[2], index=False)
    run_match_logic(evaluation_set, source)
    finalize(source)


# main('/Users/andrewharris/Desktop/Push Folder/Sleuth Terminal/190702_phone_health_sleuth.csv',
#      'July Health Check',
#      'original_id',
#      'co_id',
#      'line',
#      'full_name',
#      '/Users/andrewharris/Desktop/Push Folder/Sleuth Terminal/test_file.csv',
#      1)
# import_csvs("/Users/andrewharris/Desktop/Push Folder/Sleuth Terminal/07:08:2019_15.49.15_190702_phone_health_sleuth_job/api_data.csv",
#             "sleuth_api_call")

# if __name__ == '__main__':
#     # Command line arguments.
#     parser = argparse.ArgumentParser(description=__doc__)
#     parser.add_argument('-i', '--input_file', help='The path to the file you want to run', required=True)
#     parser.add_argument('-jn', '--job_name', help="What you want to call this batch process", required=True)
#     parser.add_argument('-id', '--id', help='Column containing a Unique Row ID', required=True)
#     parser.add_argument('-cid', '--co_id', help='Column containing Company ID', required=True)
#     parser.add_argument('-pn','--phone_number', help='Column containing phone number', required=True)
#     parser.add_argument('-n', '--name', help="Column containing the contact's name", required=True)
#     parser.add_argument('-o', '--outfile', help='The path to the file you ', required=True)
#     parser.add_argument('-m', '--mode', help='"0" for STANDARD PROCESSING\n "1" for HEALTH CHECK', required=True)
#
#     args = parser.parse_args()
#
#     main(args.input_file, args.job_name, args.id, args.cid, args.phone_number, args.name, args.outfile, args.mode)
#
from datetime import datetime
ts = int("1513085351000")
print(datetime.utcfromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S'))