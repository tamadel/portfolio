
import os
import json
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import dialects
from sqlalchemy import text
import pytz
from datetime import datetime, timedelta
import boto3
from helpers import stopExecution, checkAndMakeDir, initLogger, getSecrets, sendEmail
from client import RestClient
import requests
from bs4 import BeautifulSoup
import sqlite3
import unicodedata

#===============================
# Haupt Programm
#===============================

def main():
    ROOT = os.path.dirname(os.path.abspath(__file__)).replace(r"/src", "").replace(r"/src", "")
    SCRIPT_NAME = os.path.basename(__file__).split(".")[0]
    conn = None
    
    #---------------------------
    # Global settings laden
    #---------------------------
    global_settings_file = os.path.join(ROOT, "config", "global_settings.json")
    with open(global_settings_file) as f:
        global_settings = json.load(f)

    #---------------------------
    # Settings laden
    #---------------------------
    setting_file = os.path.join(ROOT, "config", SCRIPT_NAME, "settings.json")
    with open(setting_file) as f:
        settings = json.load(f)

    #--------------------------
    # Logger initialisieren
    #--------------------------
    logger = initLogger(SCRIPT_NAME, stdout=True, log_level=global_settings['LOG_LEVEL'])
    logger.info(f"{SCRIPT_NAME} - Logger initialized")

    #--------------------------
    # Email Settings
    #--------------------------
    try:
        emailSettings = {
            "email_from": settings['EMAIL_FROM'],
            "recipients": settings['EMAIL_RECEIVER'],
            "support": settings['EMAIL_SUPPORT'],
            "subject": settings['EMAIL_SUBJECT']
        }
        logger.info(f"{SCRIPT_NAME} - Email settings set ({emailSettings})")
    except Exception as ex:
        msg = str(ex)
        error_msg = f"Error setting email settings: {msg}"
        stopExecution(
            script=SCRIPT_NAME,
            status_msg=error_msg,
            logger=logger,
            error_flag=True,
            sendEmailFlag=True,
            emailSettings=emailSettings,
            connection=conn
        )
        return 1
    #-------------------------
    # Secrets extrahieren
    # aws Secret Manager 
    #-------------------------
    try:
        secrets = getSecrets(global_settings['AWS_REGION'], settings['SECRETS'])
        logger.debug(f"{SCRIPT_NAME} - Secrets '{settings['SECRETS']}' loaded from AWS Secret Manager")
    except Exception as ex:
        error_msg = f"Error loading secrets '{settings['SECRETS']}' from AWS Secrets Manager: {ex}"
        stopExecution(
            script=SCRIPT_NAME,
            status_msg=error_msg,
            logger=logger,
            error_flag=True,
            sendEmailFlag=True,
            emailSettings=emailSettings,
            connection=conn
        )
        return 1
    #-------------------------
    # DB-Verbindung erstellen
    #------------------------
    try:
        engine = sa.create_engine(f"postgresql+psycopg2://{secrets['POSTGRES_SERVERLESS_V2']['USERNAME']}:{secrets['POSTGRES_SERVERLESS_V2']['PASSWORD']}@{secrets['POSTGRES_SERVERLESS_V2']['HOST']}:{secrets['POSTGRES_SERVERLESS_V2']['PORT']}/{secrets['POSTGRES_SERVERLESS_V2']['DATABASE']}")
        conn = engine.connect().execution_options(autocommit=True)
        logger.info(f"{SCRIPT_NAME} - Connected to '{secrets['POSTGRES_SERVERLESS_V2']['HOST']}' as user '{secrets['POSTGRES_SERVERLESS_V2']['USERNAME']}'")
    except Exception as ex:
        msg = str(ex)
        error_msg = f"Error connecting to '{secrets['POSTGRES_SERVERLESS_V2']['HOST']}' as user '{secrets['POSTGRES_SERVERLESS_V2']['USERNAME']}': {msg}"
        stopExecution(
            script=SCRIPT_NAME,
            status_msg=error_msg,
            logger=logger,
            error_flag=True,
            sendEmailFlag=True,
            emailSettings=emailSettings,
            connection=conn
        )
        return 1

    #------------------------
    # Get URLs from database
    #------------------------

    try:
        query = f"""
                SELECT 
                    bestandid,
                    rank,
                    url 
                FROM 
                    geo_afo_tmp.tmp_scrap_search_ch
                WHERE
                    scrap_name is null
                    or 
                    scrap_address is null
                ;
        """
        url_data = pd.read_sql(query, conn) 
        logger.info("get URLs to extract data")
    except Exception as ex:
        error_msg = f"Error getting URLs: {ex}"
        logger.error(error_msg)
        stopExecution(
            script=SCRIPT_NAME,
            status_msg=error_msg,
            logger=logger,
            error_flag=True,
            sendEmailFlag=True,
            emailSettings=emailSettings,
            connection=conn
        )
        return 1

    #-----------------
    # Process URLs
    #-----------------
    for index, row in url_data.iterrows(): 
        bestandid = row['bestandid']
        url = row['url'] 
        
        # URL Repair (Handle special characters)
        url = unicodedata.normalize('NFC', url)  # Normalize Unicode characters
        
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for bad status codes
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch URL: {url} - {e}")
            continue

        soup = BeautifulSoup(response.content, 'html.parser')

        try:
            # Adjust these selectors based on the actual HTML structure
            company_name = soup.find('h1').text.strip()
            address_parts = soup.find('span', class_='street-address').text.strip()
            zip_city = soup.find('span', class_='tel-zipcity').text.strip()
            address = f"{address_parts}, {zip_city}"
            phone_element = soup.find('a', class_='value tel-callable')
            phone = phone_element.get('href').replace('tel:', '') if phone_element else None
        except AttributeError:
            logger.error(f"Error parsing data from URL: {url}")
            continue

        # Create a dictionary to store the extracted data
        data = {
            'bestand_id': bestandid,
            'url': url,
            'company_name': company_name,
            'address': address,
            'phone': phone,
            # Add other fields as needed
        }

        # Insert data into the PostgreSQL database
        try:
            df = pd.DataFrame([data])
            df.to_sql(f"search_ch", engine, if_exists='append', schema=f"{settings['WEB_SCRAPING']}", index=False, 
                     dtype={
                         'bestand_id': dialects.postgresql.INTEGER,
                         'url': dialects.postgresql.TEXT,
                         'company_name': dialects.postgresql.TEXT,
                         'address': dialects.postgresql.TEXT,
                         'phone': dialects.postgresql.TEXT,
                         # Add data types for other fields
                     }
                     )
        except Exception as ex:
            logger.error(f"Error inserting data into database for URL: {url} - {ex}")
            continue

    logger.info(f"{SCRIPT_NAME} completed successfully.")

if __name__ == "__main__":
    main()
















#
# import os
# import json
# import pandas as pd
# import sqlalchemy as sa
# from sqlalchemy import dialects
# import pytz
# from datetime import datetime, timedelta
# import boto3
# from helpers import stopExecution, checkAndMakeDir, initLogger, getSecrets, sendEmail
# from client import RestClient
# import requests
# from bs4 import BeautifulSoup


# #===============================
# # Haupt Programm
# #===============================

# def main():
#     ROOT = os.path.dirname(os.path.abspath(__file__)).replace(r"/src", "").replace(r"/src", "")
#     SCRIPT_NAME = os.path.basename(__file__).split(".")[0]
#     conn = None
    
#     #---------------------------
#     # Global settings laden
#     #---------------------------
#     global_settings_file = os.path.join(ROOT, "config", "global_settings.json")
#     with open(global_settings_file) as f:
#         global_settings = json.load(f)
        
#     #---------------------------
#     # Settings laden
#     #---------------------------
#     setting_file = os.path.join(ROOT, "config", SCRIPT_NAME, "settings.json")
#     with open(setting_file) as f:
#         settings = json.load(f)
        
#     #--------------------------
#     # Logger initialisieren
#     #--------------------------
#     logger = initLogger(SCRIPT_NAME, stdout=True, log_level=global_settings['LOG_LEVEL'])
#     logger.info(f"{SCRIPT_NAME} - Logger initialized")
    
#     #--------------------------
#     # Email Settings
#     #--------------------------
#     try:
#         emailSettings = {
#             "email_from": settings['EMAIL_FROM'],
#             "recipients": settings['EMAIL_RECEIVER'],
#             "support": settings['EMAIL_SUPPORT'],
#             "subject": settings['EMAIL_SUBJECT']
#         }
#         logger.info(f"{SCRIPT_NAME} - Email settings set ({emailSettings})")
#     except Exception as ex:
#         msg = str(ex)
#         error_msg = f"Error setting email settings: {msg}"
#         stopExecution(
#                 script=SCRIPT_NAME,
#                 status_msg=error_msg,
#                 logger=logger, 
#                 error_flag=True, 
#                 sendEmailFlag=True,
#                 emailSettings=emailSettings,
#                 connection=conn
#         )
#         return 1
#     #-------------------------
#     # Secrets extrahieren
#     # aws Secret Manager 
#     #-------------------------
#     try:
#         secrets = getSecrets(global_settings['AWS_REGION'], settings['SECRETS'])
#         logger.debug(f"{SCRIPT_NAME} - Secrets '{settings['SECRETS']}' loaded from AWS Secret Manager")
#     except Exception as ex:
#         error_msg = f"Error loading secrets '{settings['SECRETS']}' from AWS Secrets Manager: {ex}"
#         stopExecution(
#                 script=SCRIPT_NAME, 
#                 status_msg=error_msg, 
#                 logger=logger, 
#                 error_flag=True, 
#                 sendEmailFlag=True, 
#                 emailSettings=emailSettings, 
#                 connection=conn
#         )
#         return 1
#     #-------------------------
#     # DB-Verbindung erstellen
#     #------------------------
#     try:
#         engine = sa.create_engine(f"postgresql+psycopg2://{secrets['GEO_DATABASE']['USER']}:{secrets['GEO_DATABASE']['PW']}@{secrets['GEO_DATABASE']['HOST']}:{secrets['GEO_DATABASE']['PORT']}/{secrets['GEO_DATABASE']['DATABASE']}")
#         conn = engine.connect().execution_options(autocommit=True)
#         logger.info(f"{SCRIPT_NAME} - Connected to '{secrets['GEO_DATABASE']['HOST']}' as user '{secrets['GEO_DATABASE']['USER']}'")
#     except Exception as ex:
#         msg = str(ex)
#         error_msg = f"Error connecting to '{secrets['GEO_DATABASE']['HOST']}' as user '{secrets['GEO_DATABASE']['USER']}': {msg}"
#         stopExecution(
#                 script=SCRIPT_NAME, 
#                 status_msg=error_msg,
#                 logger=logger, 
#                 error_flag=True, 
#                 sendEmailFlag=True,
#                 emailSettings=emailSettings, 
#                 connection=conn
#         )
#         return 1   
    
    
#     #============================
#     # 1) Scraping
#     #============================
#     url = f"{settings['URL']}"
#     response = requests.get(url)
#     soup = BeautifulSoup(response.content, 'html.parser')

#     # soup contains the parsed HTML content
#     locations = []

#     # Iterate over all location items
#     for location in soup.find_all(class_='location-overview__list-item'):
#         try:
#             # Extract the name
#             name = location.find('h4', class_='location-overview__item-name').text.strip()
#         except Exception as ex:
#             error_msg = f"Error Extracting Name: {ex}"
#             logger.error(error_msg)
#             continue 
        
#         try:
#             # Extract the address
#             address_div = location.find('div', class_='location-overview__item-address')
#             address_lines = [line.strip() for line in address_div.stripped_strings]
#             address = ", ".join(address_lines)  # Combine address lines into a single string
            
#         except Exception as ex:
#             error_msg = f"Error Extracting Address: {ex}"
#             logger.error(error_msg)
#             continue 
        
#         try:
#             # Extract latitude and longitude from the <a> tag with the class 'location-overview__item-link no-style'
#             link_tag = location.find('a', class_='location-overview__item-link no-style')
#             data_lat = link_tag.get('data-lat', None)  
#             data_lng = link_tag.get('data-lng', None) 
            
#         except Exception as ex:
#             error_msg = f"Error Extracting latitude and longitude: {ex}"
#             logger.error(error_msg)
#             continue 
        
#         try:
#             # Extract the URL from the <a> tag with the class 'location-overview__detail-link'
#             detail_link_tag = location.find('a', class_='location-overview__detail-link')
#             url = detail_link_tag.get('href', None) if detail_link_tag else None
            
#         except Exception as ex:
#             error_msg = f"Error Extracting the URL: {ex}"
#             logger.error(error_msg)
#             continue
        
        
#         try:
#             # Extract the opening times (if available)
#             opening_time_div = location.find('div', class_='location-overview__opening-times')
#             opening_times = opening_time_div.text.strip() if opening_time_div else None
            
#         except Exception as ex:
#             error_msg = f"Error Extracting the opening times or not Available: {ex}"
#             logger.error(error_msg)
#             continue           
        
#         try: 
#             # Append the data to the list
#             locations.append({
#                 'Name': name,
#                 'Address': address,
#                 'Latitude': data_lat,
#                 'Longitude': data_lng,
#                 'URL': url,
#                 'Opening Times': opening_times
#             })
            
#         except Exception as ex:
#             error_msg = f"Attribute Error: {ex}"
#             logger.error(error_msg)
#             continue             
            
#     # Create a DataFrame
#     df = pd.DataFrame(locations)
    
#     # ----------------------------
#     # 2) Rename Columns
#     # ----------------------------
#     df.rename(
#         columns={
#             'Name': 'name',
#             'Address': 'address',
#             'Latitude': 'latitude',
#             'Longitude': 'longitude',
#             'URL': 'url',
#             'Opening Times': 'opening_times'
#         },
#         inplace=True
#     )
    

#     # ----------------------------
#     # 3) Drop duplicates
#     # ----------------------------
#     # Decide which columns define "duplicate" for you.
#     # Example: If two rows have the exact same (name, address, latitude, longitude),
#     # we consider them duplicates:
#     subset_cols = ['name', 'address', 'latitude', 'longitude']
    
#     before_size = len(df)
#     df.drop_duplicates(subset=subset_cols, inplace=True)
#     after_size = len(df)
#     logger.info(f"Dropped {before_size - after_size} duplicates. Final row count: {after_size}")

#     # ----------------------------
#     # 4) Write to Database
#     # ----------------------------
#     try:
#         df.to_sql(f"search_ch", engine, if_exists='append', schema=f"{settings['WEB_SCRAPING']}", index=False, 
#             dtype={
#                     'name': dialects.postgresql.TEXT,
#                     'address': dialects.postgresql.TEXT,
#                     'url': dialects.postgresql.TEXT,
#                     'latitude': dialects.postgresql.FLOAT, 
#                     'longitude': dialects.postgresql.FLOAT, 
#                     'opening_times': dialects.postgresql.TEXT
#                 }
#             )  
#         logger.info(f"Items inserted successfully into '{settings['AFO_TMP_SCHEMA']}.tmp_results'")
        
#     except Exception as ex:
#         error_msg = f"Database insertion error: {ex}"
#         logger.error(error_msg)
#         stopExecution(
#             script=SCRIPT_NAME,
#             status_msg=error_msg, 
#             logger=logger, 
#             error_flag=True, 
#             sendEmailFlag=True, 
#             emailSettings=emailSettings, 
#             connection=conn
#         )
#         return 1

#     # ----------------------------
#     # 5) Add geo point LV95 
#     # ----------------------------
#     # Add a geometry column if needed
#     # This uses PostGIS geometry(POINT, 2056) for LV95
#     try:
#         query = f"""
#                     ALTER TABLE {settings['WEB_SCRAPING']}.search_ch
#                     ADD COLUMN IF NOT EXISTS 
#                             geo_point_lv95 geometry(POINT, 2056);
#         """
#         conn.execute(query)
#         logger.info("Add column geo_point_lv95 ")
        
#         # 4b) Populate geo_point_lv95 using lat/long (EPSG:4326) -> ST_Transform to EPSG:2056
#         query = f"""
#                     UPDATE {settings['WEB_SCRAPING']}.search_ch
#                     SET 
#                         geo_point_lv95 = ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326),2056)
#                     WHERE 
#                         geo_point_lv95 IS NULL
#                         AND 
#                         longitude IS NOT NULL
#                         AND 
#                         latitude IS NOT NULL
#                     ;
#         """
#         conn.execute(query)
#         logger.info("geo_point_lv95 column created using PostGIS ST_Transform(...)")
    
#     except Exception as ex:
#         error_msg = f"Error creating or populating geo_point_lv95 column: {ex}"
#         logger.error(error_msg)
#         stopExecution(
#             script=SCRIPT_NAME,
#             status_msg=error_msg,
#             logger=logger,
#             error_flag=True,
#             sendEmailFlag=True,
#             emailSettings=emailSettings,
#             connection=conn
#         )
#         return 1
    
#     logger.info(f"{SCRIPT_NAME} completed successfully.")
    
    
# if __name__ == "__main__":
#     main()    
