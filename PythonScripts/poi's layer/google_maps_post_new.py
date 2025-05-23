import os
import json
import pandas as pd
import sqlalchemy as sa
from datetime import datetime, timedelta
from helpers import stopExecution, checkAndMakeDir, initLogger, getSecrets, sendEmail
from client import RestClient

#======================
# Main Programm
#======================
def main():
    ROOT = os.path.dirname(os.path.abspath(__file__)).replace(r"/src", "")
    SCRIPT_NAME = os.path.basename(__file__).split(".")[0]

    conn = None  # Initialize connection to handle proper closing later

    
    try:
        #----------------------
        # Load global settings
        #----------------------
        global_settings_file = os.path.join(ROOT, "config", "global_settings.json")
        with open(global_settings_file) as f:
            global_settings = json.load(f)
            
        #------------------------------
        # Load script-specific settings
        #------------------------------
        setting_file = os.path.join(ROOT, "config", SCRIPT_NAME, "settings.json")
        with open(setting_file) as f:
            settings = json.load(f)
        
        #----------------------
        # Initialize logger
        #----------------------
        logger = initLogger(SCRIPT_NAME, stdout=True, log_level=global_settings['LOG_LEVEL'])
        logger.info(f"{SCRIPT_NAME} - Logger initialized")
        
        #----------------------
        # Email settings
        #----------------------
        try:
            emailSettings = {
                "email_from": settings['EMAIL_FROM'],
                "recipients": settings['EMAIL_RECEIVER'],
                "support": settings['EMAIL_SUPPORT'],
                "subject": settings['EMAIL_SUBJECT']
            }
            logger.info(f"{SCRIPT_NAME} - Email settings configured ({emailSettings})")
        except KeyError as ex:
            stopExecution(
                script=SCRIPT_NAME, 
                status_msg=f"Error setting email settings: {str(ex)}", 
                logger=logger, 
                error_flag=True, 
                sendEmailFlag=True, 
                emailSettings=None
            )
            return 1
        
        #--------------------------
        # Extract secrets from AWS
        #--------------------------
        try:
            secrets = getSecrets(global_settings['AWS_REGION'], settings['SECRETS'])
            logger.debug(f"{SCRIPT_NAME} - Secrets loaded from AWS Secret Manager")
        except Exception as ex:
            stopExecution(
                script=SCRIPT_NAME, 
                status_msg=f"Error loading secrets from AWS Secrets Manager: {str(ex)}", 
                logger=logger, 
                error_flag=True, 
                sendEmailFlag=True, 
                emailSettings=emailSettings
            )
            return 1
        
        #----------------------
        # Create DB connection
        #----------------------
        try:
            engine = sa.create_engine(
                f"postgresql+psycopg2://{secrets['GEO_DATABASE']['USER']}:{secrets['GEO_DATABASE']['PW']}@"
                f"{secrets['GEO_DATABASE']['HOST']}:{secrets['GEO_DATABASE']['PORT']}/{secrets['GEO_DATABASE']['DATABASE']}"
            )
            conn = engine.connect().execution_options(autocommit=True)
            logger.info(f"{SCRIPT_NAME} - Connected to '{secrets['GEO_DATABASE']['HOST']}' as user '{secrets['GEO_DATABASE']['USER']}'")
        except Exception as ex:
            stopExecution(
                script=SCRIPT_NAME, 
                status_msg=f"Error connecting to '{secrets['GEO_DATABASE']['HOST']}' as user '{secrets['GEO_DATABASE']['USER']}': {str(ex)}", 
                logger=logger, 
                error_flag=True, 
                sendEmailFlag=True, 
                emailSettings=emailSettings, 
                connection=None
            )
            return 1
        
        #--------------------------------
        # SQL queries and data extraction
        #--------------------------------
        try:
            query_plz = f"""
                            SELECT 
                                plz4
                                ,ort 
                            FROM 
                                {settings['AFO_PROD_SCHEMA']}.{settings['LAY_PLZ4_AKTUELL']}
                            ;
            """
            df_plz4 = pd.read_sql(query_plz, conn)
            logger.info(f"{SCRIPT_NAME} - Extracted {len(df_plz4)} PLZ4 records for verification")
            
        except Exception as ex:
            error_msg = f"Error Excuting query_plz4: {ex}"
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
        
        try:
            query_keyword = f"""
                                SELECT  
                                    poi_typ_neu
                                    ,hauptkategorie_neu
                                    ,kategorie_neu
                                    ,next_run_date
                                FROM 
                                    {settings['AFO_PROD_SCHEMA']}.{settings['GOOGLE_MAP_KEYWORD']}
                                WHERE 
                                    hauptkategorie_neu = {settings['HAUPTKATEGORIE']}
                                    --AND 
                                    --kategorie_neu in ({",".join(settings['KATEGORIE'])})
                                    AND 
                                    next_run_date = current_date
                                    -- AND 
                                    -- poi_typ_neu = 'Fahrradwerkstatt'
                                ;
            """
            df_keyword = pd.read_sql(query_keyword, conn)
            logger.info(f"{SCRIPT_NAME} - Extracted {len(df_keyword)} keywords for verification")
            
        except Exception as ex:
            error_msg = f"Error Excuting query_keyword: {ex}"
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
        
        #----------------------------
        # Initialize RestClient once
        #----------------------------
        #RestClient Initialization: moved outside the loop to avoid re-initializing for each iteration(to improve performance.)
        client = RestClient(secrets['DATA_FOR_SEO']['USER_NAME'], secrets['DATA_FOR_SEO']['ACCESS_KEY'])
        
            
        #---------------------------------------
        # Iterate over keywords and postal data
        #---------------------------------------
        #Replaced iterrows() with itertuples() for better performance during DataFrame iteration.
        for row_kw in df_keyword.itertuples(index=False):
            for row_plz in df_plz4.itertuples(index=False):
                search_text = f"{row_kw.poi_typ_neu} {int(row_plz.plz4)} {row_plz.ort}"
                #search_text = f"{row_kw.keyword}"
                logger.info(f"Preparing search text: '{search_text}'")

                #----------------------------
                # Simulating the API request 
                #----------------------------
                post_data = {
                    0: {
                        "language_code": "de",
                        "location_code": f"{settings['LOCATION_CODE']}",
                        "keyword": search_text,
                        "priority": 1,
                        "search_param": "filter=0",
                        "depth": f"{settings['DEPTH']}"
                        }
                    }
                # Log the post_data for debugging purposes
                logger.debug(f"Post data: {post_data}")
                
                #--------------------------------
                # client.post_request(post_data) 
                #--------------------------------
                try:
                    logger.info(f"{SCRIPT_NAME} - Sending request for category '{row_kw.poi_typ_neu}' with PLZ4 '{int(row_plz.plz4)}' and ort '{row_plz.ort}'...")
                    
                    response = client.post(f"{secrets['DATA_FOR_SEO']['POST_ENDPOINT']}", post_data)
                    
                    logger.info(f"{SCRIPT_NAME} - Received response for category '{row_kw.poi_typ_neu}' with PLZ4 '{int(row_plz.plz4)}' and ort '{row_plz.ort}' in {response['time']} seconds")
                
                except Exception as ex:
                    error_msg = f"Error in API request for category '{row_kw.poi_typ_neu}': {ex}"
                    logger.error(error_msg)
                    stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
                    continue
                
                if response["status_code"] == 20000:
                    if not response['tasks'][0].get('id'):
                        logger.info(f"{SCRIPT_NAME} - No results found for PLZ4 {int(row_plz.plz4)} + {str(row_plz.ort)}")
                    else:
                        task = response['tasks'][0]
                        id = task['id']
                        time = task['time']
                        cost = task['cost']
                        depth = task['data']['depth']
                        keyword = task['data']['keyword']   
                        now = datetime.now()
                        index_dict = {
                            'plz4': int(row_plz.plz4), 
                            'ort': str(row_plz.ort), 
                            'poi_typ': str(row_kw.poi_typ_neu), 
                            'keyword': keyword,
                            'depth': depth,
                            'time': time,
                            'cost': cost,
                            'id': id,
                            'post_created_ts': now,
                            'hauptkategorie_neu': str(row_kw.hauptkategorie_neu),
                            'kategorie_neu': str(row_kw.kategorie_neu)
                        }
                        metadata = pd.DataFrame([index_dict])  # Wrap index_dict in a list to create a DataFrame
                        
                        try:
                            metadata.to_sql(
                                            f"{settings['GOOGLE_MAP_METADATA']}", 
                                            engine, if_exists='append', 
                                            schema=f"{settings['GOOGLE_MAPS_SCHEMA']}",
                                            index=False
                            )
                            
                            logger.info("Metadata table 'GOOGLE_MAP_METADATA' created successfully")
                            
                        except Exception as ex:
                            error_msg = f"Error creating metadata table: {ex}"
                            logger.error(error_msg)
                            raise
                else:
                    logger.error(f"Error. Code: {response['status_code']} Message: {response['status_message']}")

    except Exception as ex:
        logger.error(f"An unexpected error occurred: {str(ex)}")
        stopExecution(
            script=SCRIPT_NAME, 
            status_msg=f"An error occurred: {str(ex)}", 
            logger=logger, 
            error_flag=True, 
            sendEmailFlag=True, 
            emailSettings=emailSettings, 
            connection=conn
        )
        return 0

if __name__ == "__main__":
    main()
