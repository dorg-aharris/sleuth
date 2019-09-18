import json
import requests
import boto3
import pandas
from metaphone import doublemetaphone
import time
import re
import pymysql
import unidecode
import itertools

start_time = time.time()
connection = pymysql.connect(host='10.255.8.46', user='aharris', password='heLH2iYp8uXQ', db='aharris', charset='utf8', use_unicode=True)
connection.autocommit(True)
cursor = connection.cursor()

wp_api_key = '017f0b5d696f49e194b65464e86e189d'
wp_url_base = "https://proapi.whitepages.com/3.0/phone?"

#dataset = pandas.read_sql("SELECT id, source_id, co_id, original_id, first_name, last_name, full_name, line FROM rover_tracking.sleuth_stage WHERE id IN (4175446,4185611)", connection)

def sleuth():
    sleep(1.5)
    print("I've pulled all the available records for your source, I'm starting your list.")
    dataset = pandas.read_sql("SELECT id, source_id, co_id, original_id, first_name, last_name, full_name, line FROM rover_tracking.sleuth_stage WHERE source_id=%s", (selector), connection)
    for index, row in dataset.iterrows():
        phone = re.sub('\W+','', row['line'])
        params = {"phone":phone, "api_key":wp_api_key}
        response = requests.get(wp_url_base, params)
        row['API_Status'] = response.status_code
        if row['API_Status'] == 200:
            try:
                data = response.json()
                row['WP_id'] = data['id']
                row['country_calling_code'] = data['country_calling_code']
                row['line_type'] = data['line_type']
                row['carrier'] = data['carrier']
                row['is_prepaid'] = data['is_prepaid']
                row['is_commercial'] = data['is_commercial']
                row['owner_id'] = data['belongs_to'][0]['id']
                row['owner_type'] = data['belongs_to'][0]['type']
                row['owner_name'] = data['belongs_to'][0]['name']
                row['owner_firstname'] = data['belongs_to'][0]['firstname']
                row['owner_middlename'] = data['belongs_to'][0]['middlename']
                row['owner_lastname'] = data['belongs_to'][0]['lastname']
                row['owner_age_range'] = data['belongs_to'][0]['age_range']
                row['owner_gender'] = data['belongs_to'][0]['gender']
                row['owner_start_date'] = data['belongs_to'][0]['link_to_phone_start_date']
                row['current_address_id'] = data['current_addresses'][0]['id']
                row['current_address_type'] = data['current_addresses'][0]['location_type']
                row['current_street_line1'] = data['current_addresses'][0]['street_line_1']
                row['current_street_line2'] = data['current_addresses'][0]['street_line_2']
                row['current_postal_code'] = data['current_addresses'][0]['postal_code']
                row['current_zip4'] = data['current_addresses'][0]['zip4']
                row['current_state_code'] = data['current_addresses'][0]['state_code']
                row['current_country_code'] = data['current_addresses'][0]['country_code']
                row['current_latitude'] = data['current_addresses'][0]['lat_long']['latitude']
                row['current_longitude'] = data['current_addresses'][0]['lat_long']['longitude']
                row['current_address_accuracy'] = data['current_addresses'][0]['lat_long']['accuracy']
                row['current_address_active'] = data['current_addresses'][0]['is_active']
                row['current_address_delivery_point'] = data['current_addresses'][0]['delivery_point']
                row['current_address_in_use'] = data['current_addresses'][0]['link_to_person_start_date']
                historical_addy = data['historical_addresses']
                historical_list = [str(addy['lat_long']['latitude']) + "_" + str(addy['lat_long']['longitude']) for addy in historical_addy]
                row['historical_addresses'] = (str(", ".join(historical_list)) if data['historical_addresses'] else None)
                associated_people = data['associated_people']
                row['sec_most_associated_person_id'] = (associated_people[0]['id'] if associated_people else None)
                row['sec_most_associated_name'] = (associated_people[0]['name'] if associated_people else None)
                row['sec_most_associated_firstname'] = (associated_people[0]['firstname'] if associated_people else None)
                row['sec_most_associated_middlename'] = (associated_people[0]['middlename'] if associated_people else None)
                row['sec_most_associated_lastname'] = (associated_people[0]['lastname'] if associated_people else None)
                row['sec_most_associated_relation'] = (associated_people[0]['relation'] if associated_people else None)
                row['third_most_associated_person_id'] = (associated_people[1]['id'] if associated_people else None)
                row['third_most_associated_name'] = (associated_people[1]['name'] if associated_people else None)
                row['third_most_associated_firstname'] = (associated_people[1]['firstname'] if associated_people else None)
                row['third_most_associated_middlename'] = (associated_people[1]['middlename'] if associated_people else None)
                row['third_most_associated_lastname'] = (associated_people[1]['lastname'] if associated_people else None)
                row['third_most_associated_relation'] = (associated_people[1]['relation'] if associated_people else None)
                row['alternate_phones'] = (str(", ".join(data['alternate_phones'])) if data['alternate_phones'] else None)
                row['warnings'] = (data['warnings'][0] if data['warnings'] else None)
                row['errors'] = (data['error'][0] if data['error'] else None)
                row_list = row.tolist()
                row_list = ['' if x is None else x for x in row_list]
                print(row_list)
                cursor.execute("INSERT INTO rover_tracking.sleuth_api_call(sleuth_stage_id, source_id, co_id, original_contact_id, first_name, last_name, full_name, line, sleuth_api_status, WP_id, country_calling_code, line_type, carrier, is_prepaid, is_commercial, owner_id, owner_type, owner_name, owner_firstname, owner_middlename, owner_lastname, owner_age_range, owner_gender, owner_start_date, current_address_id, current_address_type, current_street_line1, current_street_line2, current_postal_code, current_zip4, current_state_code, current_country_code, current_latitude, current_longitude, current_address_accuracy, current_address_active, current_address_delivery_point, current_address_in_use, historical_addresses, 2nd_most_associated_person_id, 2nd_most_associated_name, 2nd_most_associated_firstname, 2nd_most_associated_middlename, 2nd_most_associated_lastname, 2nd_most_associated_relation, 3rd_most_associated_person_id, 3rd_most_associated_name, 3rd_most_associated_firstname, 3rd_most_associated_middlename, 3rd_most_associated_lastname, 3rd_most_associated_relation, alternate_phones, warnings, errors) VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}', '{13}', '{14}', '{15}', '{16}', '{17}', '{18}', '{19}', '{20}', '{21}', '{22}', '{23}', '{24}', '{25}', '{26}', '{27}', '{28}', '{29}', '{30}', '{31}', '{32}', '{33}', '{34}', '{35}', '{36}', '{37}', '{38}', '{39}', '{40}', '{41}', '{42}', '{43}', '{44}', '{45}', '{46}', '{47}', '{48}', '{49}', '{50}', '{51}', '{52}', '{53}')".format(*row_list))
                connection.commit()
            except UnicodeEncodeError:
                pass
            except pymysql.err.ProgrammingError:
                pass
        else:
            try:
                row_list = row.tolist()
                cursor.execute("INSERT INTO rover_tracking.sleuth_stage_id(sleuth_stage_id, source_id, co_id, original_contact_id, first_name, last_name, full_name, line, sleuth_api_status) VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}')".format(*row_list))
                connection.commit()
            except UnicodeEncodeError:
                pass
            except pymysql.err.ProgrammingError:
                pass

#close all connections
connection.close()
cursor.close()

#response = requests.get(wp_url_base, )
