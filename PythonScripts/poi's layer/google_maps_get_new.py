import os
import json
import pandas as pd
import sqlalchemy as sa
from fuzzywuzzy import fuzz
from sqlalchemy import dialects
import pytz
from datetime import datetime, timedelta
import boto3
from helpers import stopExecution, checkAndMakeDir, initLogger, getSecrets, sendEmail
from client import RestClient

def main():
    ROOT = os.path.dirname(os.path.abspath(__file__)).replace(r"/src", "")
    SCRIPT_NAME = os.path.basename(__file__).split(".")[0]
    conn = None

    # TEST TEST
    
    a = 'Hallodfrsf'
    b = 'gdggdgHalro'
    
    rat = fuzz.partial_ratio(a,b)

    try:
        # ---------------------------
        # Global settings laden
        # ---------------------------
        global_settings_file = os.path.join(ROOT, "config", "global_settings.json")
        with open(global_settings_file) as f:
            global_settings = json.load(f)

        # ---------------------------
        # Settings laden
        # ---------------------------
        setting_file = os.path.join(ROOT, "config", SCRIPT_NAME, "settings.json")
        with open(setting_file) as f:
            settings = json.load(f)

        # --------------------------
        # Logger initialisieren
        # --------------------------
        logger = initLogger(SCRIPT_NAME, stdout=True, log_level=global_settings['LOG_LEVEL'])
        logger.info(f"{SCRIPT_NAME} - Logger initialized")

        # --------------------------
        # Email Settings
        # --------------------------
        try:
            emailSettings = {
                "email_from": settings['EMAIL_FROM'],
                "recipients": settings['EMAIL_RECEIVER'],
                "support": settings['EMAIL_SUPPORT'],
                "subject": settings['EMAIL_SUBJECT']
            }
            logger.info(f"{SCRIPT_NAME} - Email settings set ({emailSettings})")
        except KeyError as ex:
            error_msg = f"Error setting email settings: {ex}"
            stopExecution(
                    script=SCRIPT_NAME, 
                    status_msg=error_msg, 
                    logger=logger, 
                    error_flag=True, 
                    sendEmailFlag=True, 
                    emailSettings=None, 
                    connection=conn
            )
            return 1

        # ----------------------------------------
        # Secrets extrahieren - aws Secret Manager
        # ----------------------------------------
        try:
            secrets = getSecrets(global_settings['AWS_REGION'], settings['SECRETS'])
            logger.debug(f"{SCRIPT_NAME} - Secrets '{settings['SECRETS']}' loaded from AWS Secret Manager")
            
        except Exception as ex:
            error_msg = f"Error loading secrets '{settings['SECRETS']}' from AWS Secret Manager: {ex}"
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

        # -------------------------
        # DB-Verbindung erstellen
        # ------------------------
        try:
            engine = sa.create_engine(f"postgresql+psycopg2://{secrets['GEO_DATABASE']['USER']}:{secrets['GEO_DATABASE']['PW']}@{secrets['GEO_DATABASE']['HOST']}:{secrets['GEO_DATABASE']['PORT']}/{secrets['GEO_DATABASE']['DATABASE']}")
            conn = engine.connect().execution_options(autocommit=True)
            logger.info(f"{SCRIPT_NAME} - Connected to '{secrets['GEO_DATABASE']['HOST']}' as user '{secrets['GEO_DATABASE']['USER']}'")
            
        except Exception as ex:
            error_msg = f"Error connecting to '{secrets['GEO_DATABASE']['HOST']}' as user '{secrets['GEO_DATABASE']['USER']}': {ex}"
            stopExecution(
                    script=SCRIPT_NAME, 
                    status_msg=error_msg,
                    logger=logger, 
                    error_flag=True, 
                    sendEmailFlag=True, 
                    emailSettings=emailSettings, 
                    connection=None
            )
            return 1
        # -----------------------------------------------
        # iItems Tabelle erstellen oder schon existieret 
        # -----------------------------------------------
        try:
            query = f"""
                    CREATE TABLE IF NOT EXISTS {settings['GOOGLE_MAPS_SCHEMA']}.{settings['GOOGLE_MAP_ITEMS']} (
                        cid TEXT,
                        rank_absolute INTEGER,
                        keyword TEXT,
                        poi_typ TEXT,
                        exact_match INTEGER,
                        plz4 TEXT,
                        ort TEXT,
                        strasse TEXT,
                        country_code TEXT,
                        address TEXT,
                        title TEXT,
                        phone TEXT,
                        domain TEXT,
                        url TEXT,
                        rating JSONB,
                        total_photos INTEGER,
                        hotel_rating FLOAT,
                        category TEXT,
                        additional_categories JSONB,
                        category_ids_de JSONB,
                        category_ids JSONB,
                        work_hours JSONB,
                        geo_point_lv95 GEOMETRY(Point, 2056),
                        longitude FLOAT,
                        latitude FLOAT
                    );
            """
            conn.execute(query)
            logger.info(f"Table '{settings['GOOGLE_MAPS_SCHEMA']}.{settings['GOOGLE_MAP_ITEMS']}' ensured to exist.")
        except Exception as ex:
            error_msg = f"Error ensuring items table exists: {ex}"
            logger.error(error_msg)
            stopExecution(
                script=SCRIPT_NAME,
                status_msg=error_msg,
                logger=logger,
                error_flag=True,
                sendEmailFlag=True,
                emailSettings=None,
                connection=conn
            )
            return 1


        # -----------------------------------------------
        # Results Tabelle erstellen oder schon existieret 
        # -----------------------------------------------
        try:
            query = f"""
                CREATE TABLE IF NOT EXISTS {settings['GOOGLE_MAPS_SCHEMA']}.{settings['GOOGLE_MAP_RESULTS']} (
                                            type text ,
                                            rank_group INTEGER ,
                                            rank_absolute INTEGER ,
                                            domain text ,
                                            title text ,
                                            url text ,
                                            contact_url text ,
                                            contributor_url text ,
                                            book_online_url text ,
                                            rating jsonb ,
                                            hotel_rating float8 ,
                                            price_level text ,
                                            rating_distribution jsonb ,
                                            snippet text ,
                                            address text ,
                                            address_info jsonb ,
                                            place_id text ,
                                            phone text ,
                                            main_image text ,
                                            total_photos INTEGER ,
                                            category text ,
                                            additional_categories jsonb ,
                                            category_ids jsonb ,
                                            work_hours jsonb ,
                                            feature_id text ,
                                            cid text ,
                                            latitude float8 ,
                                            longitude float8 ,
                                            is_claimed bool ,
                                            local_justifications jsonb ,
                                            is_directory_item bool ,
                                            keyword text ,
                                            exact_match INTEGER ,
                                            poi_typ text 
                );
            """
            conn.execute(query)
            logger.info(f"Table '{settings['GOOGLE_MAPS_SCHEMA']}.{settings['GOOGLE_MAP_RESULTS']}' ensured to exist.")
        except Exception as ex:
            error_msg = f"Error ensuring items table exists: {ex}"
            logger.error(error_msg)
            stopExecution(
                script=SCRIPT_NAME,
                status_msg=error_msg,
                logger=logger,
                error_flag=True,
                sendEmailFlag=True,
                emailSettings=None,
                connection=conn
            )
            return 1
        
        # -----------------------------
        # Query API Metadata
        # -----------------------------
        try:
            query = f"""
                SELECT 
                    plz4,
                    poi_typ,
                    keyword,
                    id
                FROM 
                    {settings['GOOGLE_MAPS_SCHEMA']}.{settings['GOOGLE_MAP_METADATA']}
                WHERE
                    id IS NOT NULL
                    AND 
                    datetime IS NULL
                    --AND 
                    --kategorie_neu IN ({",".join(settings['KATEGORIE'])})
                ;
            """
            df_post_ids = pd.read_sql(query, conn)
            rec_count = df_post_ids.shape[0]
            logger.info(f"{SCRIPT_NAME} - Extracted {rec_count} id's for verification")
        except Exception as ex:
            error_msg = f"Error executing ID query: {ex}"
            logger.error(error_msg)
            stopExecution(SCRIPT_NAME, error_msg, logger, True, True, None, conn)
            return 1

        # ----------------------------
        # Initialize RestClient
        # ----------------------------
        client = RestClient(secrets['DATA_FOR_SEO']['USER_NAME'], secrets['DATA_FOR_SEO']['ACCESS_KEY'])
        zurich_tz = pytz.timezone('Europe/Zurich')

        # ----------------------------------------
        # Process Metadata and Update Tables
        # ----------------------------------------
        for row in df_post_ids.itertuples(index=False):
            try:
                # Prepare the API request
                try:
                    link_endpoint = f"{secrets['DATA_FOR_SEO']['GET_ENDPOINT']}/{row.id}"
                    logger.info(f"{SCRIPT_NAME} - Sending GET request for PLZ4 {int(row.plz4)}...")
                    response = client.get(link_endpoint)
                    
                except Exception as ex:
                    error_msg = f"Error making GET request for PLZ4: {ex}"
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
                

                # Process the API response
                try:
                    if response["status_code"] == 20000:
                        status_message = response['tasks'][0]['status_message']
                        status_code = response['tasks'][0]['status_code']
                        if not response['tasks'][0]['status_code'] == 20000:
                            #response['tasks'][0]['result'][0].get('items'):
                            try:
                                query = f"""
                                                UPDATE 
                                                    {settings['GOOGLE_MAPS_SCHEMA']}.{settings['GOOGLE_MAP_METADATA']}
                                                SET 
                                                    datetime = current_date,
                                                    item_type = 'maps_search',
                                                    n_result = 0,
                                                    status_message = '{status_message}',
                                                    status_code = '{status_code}'
                                                WHERE 
                                                    plz4 = {row.plz4} 
                                                    AND 
                                                    id = '{str(row.id)}'
                                                ;
                                            """
                                conn.execute(query)
                                logger.info(f"{SCRIPT_NAME} - No results found for PLZ4 {int(row.plz4)} and {str(row.id)} ")
                                
                            except Exception as ex:
                                error_msg = f"Error updating metadata with status_message {status_message} for PLZ4 {int(row.plz4)} and id {str(row.id)} : {ex}"
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
                        else:
                            # Process the results
                            try:
                                items_count = response['tasks'][0]['result'][0]['items_count']
                                logger.info(f"{SCRIPT_NAME} - {items_count} results found for PLZ4 {int(row.plz4)}")

                                items = response['tasks'][0]['result'][0]['items']
                                keyword = response['tasks'][0]['result'][0]['keyword']
                                items_dt = datetime.strptime(response['tasks'][0]['result'][0]['datetime'], '%Y-%m-%d %H:%M:%S +00:00').astimezone(zurich_tz)

                                df = pd.DataFrame(items)
                                df['keyword'] = keyword

                                # Filter and exact match processing
                                df = df[df['type'] == 'maps_search']
                                plz4 = str(row.plz4)
                                df['exact_match'] = df['address'].apply(lambda x: int(plz4 in str(x)) if pd.notna(x) else 0)

                                # Insert into GOOGLE_MAP_RESULTS
                                # Bulk insert results into the database
                                try:
                                    query = f"""
                                                DROP TABLE IF EXISTS {settings['AFO_TMP_SCHEMA']}.tmp_results;

                                                 CREATE TABLE {settings['AFO_TMP_SCHEMA']}.tmp_results (
                                                    type text ,
                                                    rank_group INTEGER ,
                                                    rank_absolute INTEGER ,
                                                    domain text ,
                                                    title text ,
                                                    url text ,
                                                    contact_url text ,
                                                    contributor_url text ,
                                                    book_online_url text ,
                                                    rating jsonb ,
                                                    hotel_rating float8 ,
                                                    price_level text ,
                                                    rating_distribution jsonb ,
                                                    snippet text ,
                                                    address text ,
                                                    address_info jsonb ,
                                                    place_id text ,
                                                    phone text ,
                                                    main_image text ,
                                                    total_photos INTEGER ,
                                                    category text ,
                                                    additional_categories jsonb ,
                                                    category_ids jsonb ,
                                                    work_hours jsonb ,
                                                    feature_id text ,
                                                    cid text ,
                                                    latitude float8 ,
                                                    longitude float8 ,
                                                    is_claimed bool ,
                                                    local_justifications jsonb ,
                                                    is_directory_item bool ,
                                                    keyword text ,
                                                    exact_match INTEGER ,
                                                    poi_typ text 
                                                );
                                        """     
                                    conn.execute(query)
                                    logger.info(f"tmp results '{settings['AFO_TMP_SCHEMA']}.tmp_results' table successfully created")
                                        
                                except Exception as ex:
                                    logger.error(f"Error while tmp_results '{settings['AFO_TMP_SCHEMA']}.tmp_results' table: {ex}")
                                    continue   
                                     
                                #Create Google_Map_Result (Table (1): google_map_results_<main_categorie_name>)          
                                try:
                                    df.to_sql(f"tmp_results", engine, if_exists='append', schema=f"{settings['AFO_TMP_SCHEMA']}", index=False,  
                                        dtype={
                                                'type': dialects.postgresql.TEXT,
                                                'rank_group': dialects.postgresql.INTEGER,
                                                'rank_absolute': dialects.postgresql.INTEGER,
                                                'domain': dialects.postgresql.TEXT,
                                                'title': dialects.postgresql.TEXT,
                                                'url': dialects.postgresql.TEXT,
                                                'contact_url': dialects.postgresql.TEXT,
                                                'contributor_url': dialects.postgresql.TEXT,
                                                'rating': dialects.postgresql.JSONB,
                                                'hotel_rating': dialects.postgresql.FLOAT,
                                                'price_level': dialects.postgresql.TEXT,
                                                'rating_distribution': dialects.postgresql.JSONB,
                                                'snippet': dialects.postgresql.TEXT,
                                                'address': dialects.postgresql.TEXT,
                                                'address_info': dialects.postgresql.JSONB,
                                                'place_id': dialects.postgresql.TEXT,
                                                'phone': dialects.postgresql.TEXT, 
                                                'main_image': dialects.postgresql.TEXT, 
                                                'total_photos': dialects.postgresql.INTEGER, 
                                                'category': dialects.postgresql.TEXT,
                                                'additional_categories': dialects.postgresql.JSONB, 
                                                'category_ids': dialects.postgresql.JSONB, 
                                                'work_hours': dialects.postgresql.JSONB, 
                                                'feature_id': dialects.postgresql.TEXT,
                                                'cid': dialects.postgresql.TEXT, 
                                                'latitude': dialects.postgresql.FLOAT, 
                                                'longitude': dialects.postgresql.FLOAT, 
                                                'is_claimed': dialects.postgresql.BOOLEAN, 
                                                'local_justifications': dialects.postgresql.JSONB,
                                                'is_directory_item': dialects.postgresql.BOOLEAN,
                                                'keyword': dialects.postgresql.TEXT,
                                            }
                                        # ,chunksize=1000  # Optimized for batch insert
                                        )  
                                    
                                    logger.info(f"Items inserted successfully into '{settings['AFO_TMP_SCHEMA']}.tmp_results'")
                                    
                                    try:
                                        query = f"""
                                                    INSERT INTO --seprate query with try except
                                                        {settings['GOOGLE_MAPS_SCHEMA']}.{settings['GOOGLE_MAP_RESULTS']}
                                                    SELECT 
                                                        * 
                                                    FROM 
                                                        {settings['AFO_TMP_SCHEMA']}.tmp_results
                                                    ;
                                        """
                                        conn.execute(query)
                                        logger.info(f"Table '{settings['GOOGLE_MAPS_SCHEMA']}.{settings['GOOGLE_MAP_RESULTS']}' successfully updated")
                                    
                                    except Exception as ex:
                                        logger.error(f"Error for '{settings['GOOGLE_MAPS_SCHEMA']}.{settings['GOOGLE_MAP_RESULTS']}' : {ex}")
                                        continue
                                    
                                    try:
                                        query = f"""
                                                    DELETE FROM {settings['AFO_TMP_SCHEMA']}.tmp_results
                                                    WHERE 
                                                        cid in (
                                                                select
                                                                    cid
                                                                from
                                                                    {settings['GOOGLE_MAPS_SCHEMA']}.{settings['GOOGLE_MAP_ITEMS']}
                                                        )
                                                        ;
                                        """
                                        conn.execute(query)
                                        logger.info(f"Delete cid in '{settings['AFO_TMP_SCHEMA']}.tmp_results' Table")
                                        
                                    except Exception as ex:
                                        logger.error(f"Error for deleting cid from '{settings['AFO_TMP_SCHEMA']}.tmp_results': {ex}")
                                        continue    
                                    
                                except Exception as ex:
                                    error_msg = f"Database insertion error for plz4 {int(row.plz4)}: {ex}"
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
                                
                                
                                # Update metadata
                                try:
                                    query = f"""
                                        UPDATE 
                                            {settings['GOOGLE_MAPS_SCHEMA']}.{settings['GOOGLE_MAP_METADATA']}
                                        SET 
                                            datetime = '{items_dt.strftime('%Y-%m-%d %H:%M:%S')}',
                                            n_result = {items_count},
                                            item_type = 'maps_search',
                                            status_message = '{status_message}',
                                            status_code = '{status_code}'
                                        WHERE 
                                            plz4 = {row.plz4} 
                                            AND 
                                            id = '{str(row.id)}'
                                        ;
                                    """
                                    conn.execute(query)
                                    logger.info(f"Metadata updated successfully in '{settings['GOOGLE_MAPS_SCHEMA']}.{settings['GOOGLE_MAP_METADATA']}'")
                                    
                                except Exception as ex:
                                    error_msg = f"Error updating metadata for PLZ4 {int(row.plz4)}: {ex}"
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

                                # Update GOOGLE_MAP_ITEMS using our SQL logic (Table (2): google_map_items_<main_categorie_name>)
                                try:
                                    query = f"""    --seprate query with try except
                                                    DROP TABLE IF EXISTS tmp_filtered_items;

                                                    CREATE TEMP TABLE tmp_filtered_items AS
                                                    SELECT
                                                        m.cid,
                                                        rank_absolute::INTEGER AS rank_absolute,
                                                        m.keyword,
                                                        TRIM(SUBSTRING(m.keyword FROM '^[^0-9]+')) AS poi_typ,
                                                        m.exact_match,
                                                        m.address_info->>'zip' AS plz4,
                                                        m.address_info->>'city' AS ort,
                                                        m.address_info->>'address' AS strasse,
                                                        m.address_info->>'country_code' AS country_code,
                                                        m.address,
                                                        m.title,
                                                        m.phone,
                                                        m.domain,
                                                        m.url,
                                                        m.rating,
                                                        m.total_photos,
                                                        m.hotel_rating,
                                                        m.category,
                                                        m.additional_categories,
                                                        m.category_ids_de::jsonb,
                                                        m.category_ids,
                                                        m.work_hours,
                                                        st_transform(ST_SetSRID(ST_MakePoint(m.longitude, m.latitude), 4326), 2056) AS geo_point_lv95,
                                                        m.longitude,
                                                        m.latitude
                                                    FROM (
                                                        SELECT
                                                            cid,
                                                            rank_absolute::INTEGER,
                                                            address_info,
                                                            keyword,
                                                            exact_match,
                                                            address,
                                                            title,
                                                            phone,
                                                            domain,
                                                            url,
                                                            rating,
                                                            total_photos,
                                                            hotel_rating,
                                                            category,
                                                            additional_categories,
                                                            'null' as category_ids_de,
                                                            category_ids,
                                                            work_hours,
                                                            longitude,
                                                            latitude,
                                                            ROW_NUMBER() OVER (PARTITION BY cid ORDER BY rank_absolute::INTEGER, random()) AS row_num
                                                        FROM {settings['AFO_TMP_SCHEMA']}.tmp_results
                                                        WHERE address_info->>'country_code' = 'CH'
                                                    ) m
                                                    WHERE m.row_num = 1
                                                    ;
                                            """
                                    conn.execute(query)
                                    logger.info(f"Tmp Items table successfully created")
                                
                                except Exception as ex:
                                    logger.error(f"Unexpected error for PLZ4 {int(row.plz4)}: {ex}")
                                    return 1
                                    
                                try:    # comment
                                    query = f"""           
                                                UPDATE  --seprate query with try except
                                                    tmp_filtered_items
                                                SET  
                                                    category_ids_de = CASE 
                                                        WHEN additional_categories IS NULL  
                                                            OR additional_categories::text = 'null'
                                                        THEN
                                                            to_jsonb(array[category])
                                                        WHEN jsonb_typeof(additional_categories::jsonb) = 'array'
                                                        THEN
                                                            jsonb_insert(additional_categories::jsonb, '{{0}}', to_jsonb(category))
                                                        ELSE      
                                                            to_jsonb(array[category] || additional_categories::text)
                                                    END
                                                ;
                                            """        
                                    conn.execute(query)
                                    logger.info(f"Tmp Items table successfully Updated with category_ids_de")
                                
                                except Exception as ex:
                                    logger.error(f"Unexpected error for PLZ4 {int(row.plz4)}: {ex}")
                                    return 1 
                                
                                #Abstand
                                try:
                                    query = f"""
                                                INSERT INTO --seprate query with try except
                                                    {settings['GOOGLE_MAPS_SCHEMA']}.{settings['GOOGLE_MAP_ITEMS']}
                                                SELECT 
                                                    * 
                                                FROM 
                                                    tmp_filtered_items
                                                ;
                                    """
                                    conn.execute(query)
                                    logger.info(f"Table Items '{settings['GOOGLE_MAPS_SCHEMA']}.{settings['GOOGLE_MAP_ITEMS']}' successfully Updated")
                                
                                except Exception as ex:
                                    logger.error(f"Unexpected error for PLZ4 {int(row.plz4)}: {ex}")
                                    return 1   


                            except Exception as ex:
                                error_msg = f"General Error: {ex}"
                                logger.error(error_msg)
                                stopExecution(SCRIPT_NAME, error_msg, logger, True, True, None, conn)
                                return 0
                
                except Exception as ex:
                    error_msg = f"Unexpected error for PLZ4 {int(row.plz4)}: {ex}"
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
            except Exception as ex:
                error_msg = f"Unexpected error for PLZ4 {int(row.plz4)}: {ex}"
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
        
        # items Table: cleaning and map the category to our category und our poi (Table (3): google_map_<main_categorie_name>)
        try:
            query = f"""
                    -- Create a temporary table for category mapping
                    DROP TABLE IF EXISTS tmp_category_mapping;
                    CREATE TEMP TABLE tmp_category_mapping 
                    AS
                    SELECT 
                        *
                    FROM 
                        geo_afo_prod.meta_poi_categories_business_data_aktuell
                    WHERE 
                        hauptkategorie_neu = {settings['HAUPTKATEGORIE']}
                    ;
            """
            conn.execute(query)
            logger.info(" Temporary table for category mapping successfully created")

        except Exception as ex:
            error_msg = f"Error creating Temporary table for category mapping: {ex}"
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
            query = f"""   
                    -- Items filtered with category and Hotel/Gastro updated table
                    DROP TABLE IF EXISTS tmp_google_map_category;
                    
                    CREATE TEMP TABLE tmp_google_map_category 
                    AS
                    SELECT 
                        *
                    FROM 
                        {settings['GOOGLE_MAPS_SCHEMA']}.{settings['GOOGLE_MAP_ITEMS']} t0
                    WHERE EXISTS (
                                    SELECT 1
                                    FROM 
                                        geo_afo_prod.meta_poi_categories_business_data_aktuell t2
                                    WHERE 
                                        t0.category_ids @> to_jsonb(t2.category_id)::jsonb
                                        AND 
                                        t2.hauptkategorie_neu = {settings['HAUPTKATEGORIE']}
                    );
            """
            conn.execute(query)
            logger.info(" Temporary table of items filtered by the relevant category successfully created")

        except Exception as ex:
            error_msg = f"Error creating temporary table of items filtered by the relevant category: {ex}"
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
            query = f"""
                    -- Create a temporary table to unfold category_ids for mapping to our poi_category
                    DROP TABLE IF EXISTS tmp_google_category;
                    
                    CREATE TEMP TABLE tmp_google_category 
                    AS
                    SELECT 
                        cid,
                        exact_match,
                        keyword AS kw_long,
                        split_part(keyword, ' ', 1) AS keyword,
                        jsonb_array_elements_text(category_ids_de::jsonb) AS category_de_ids,
                        jsonb_array_elements_text(category_ids::jsonb) AS category_en_ids,
                        title,
                        address,
                        strasse,
                        plz4,
                        ort,
                        domain,
                        url,
                        phone,
                        total_photos AS anz_fotos,
                        rating->>'value' AS google_bewertung,
                        rating->>'votes_count' AS anz_bewertungen,
                        work_hours,
                        work_hours->>'current_status' AS status,
                        geo_point_lv95,
                        category_ids_de,
                        category_ids,
                        longitude,
                        latitude 
                    FROM 
                        tmp_google_map_category
                    WHERE 
                        jsonb_typeof(category_ids::jsonb) = 'array'
                    ;
            """
            conn.execute(query)
            logger.info(" Temporary table table to unfold category_ids for mapping to our poi_category successfully created")

        except Exception as ex:
            error_msg = f"Error creating temporary table of unfolding category_ids for mapping: {ex}"
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
            query = f"""        
                    -- Create category table mapped with afo_poi_typ and afo_kategorie
                    DROP TABLE IF EXISTS {settings['GOOGLE_MAPS_SCHEMA']}.{settings['GOOGLE_MAP_CATEGORY']};
                    
                    CREATE TABLE {settings['GOOGLE_MAPS_SCHEMA']}.{settings['GOOGLE_MAP_CATEGORY']} 
                    AS
                    SELECT
                        t0.cid,
                        t0.exact_match,
                        t0.kw_long,
                        t0.keyword,
                        STRING_AGG(DISTINCT t1.hauptkategorie_neu, ' | ') AS afo_hauptkategorie,
                        STRING_AGG(DISTINCT t1.poi_typ_neu, ' | ') AS afo_poi_typ,
                        STRING_AGG(DISTINCT t1.kategorie_neu, ' | ') AS afo_category,
                        STRING_AGG(DISTINCT t0.category_en_ids, ' | ') AS category_en_ids,
                        STRING_AGG(DISTINCT t0.category_de_ids, ' | ') AS category_de_ids,
                        t0.title,
                        t0.address,
                        t0.strasse AS strasse_h_no,
                        REGEXP_REPLACE(t0.strasse, '[0-9]+[A-Za-z]*$', '') AS strasse,
                        (REGEXP_MATCH(t0.strasse, '[0-9]+[A-Za-z]*$'))[1] AS hausnummer,
                        t0.plz4,
                        t0.ort,
                        t0."domain",
                        t0.url,
                        t0.phone,
                        t0.anz_fotos,
                        t0.google_bewertung,
                        t0.anz_bewertungen,
                        t0.work_hours,
                        t0.status,
                        t0.geo_point_lv95,
                        t0.category_ids_de,
                        t0.category_ids,
                        t0.longitude,
                        t0.latitude
                    FROM 
                        tmp_google_category t0
                    LEFT JOIN 
                        tmp_category_mapping t1
                    ON 
                        t0.category_en_ids = t1.category_id
                    GROUP BY 
                        t0.cid, 
                        t0.title, 
                        t0.address, 
                        t0.strasse, 
                        t0.plz4, 
                        t0.ort, 
                        t0."domain", 
                        t0.url, 
                        t0.phone, 
                        t0.work_hours, 
                        t0.status, 
                        t0.geo_point_lv95, 
                        t0.longitude, 
                        t0.latitude, 
                        t0.exact_match, 
                        t0.kw_long, 
                        t0.keyword, 
                        t0.google_bewertung, 
                        t0.anz_bewertungen, 
                        t0.anz_fotos, 
                        t0.category_ids_de, 
                        t0.category_ids
                        ;
            """
            conn.execute(query)
            logger.info(f" {settings['GOOGLE_MAPS_SCHEMA']}.{settings['GOOGLE_MAP_CATEGORY']} successfully created")

        except Exception as ex:
            error_msg = f"Error updating the '{settings['GOOGLE_MAPS_SCHEMA']}.{settings['GOOGLE_MAP_CATEGORY']}' table: {ex}"
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
        
        # Periodic update of run timestamps
        try:
            update_query = f"""
                                UPDATE 
                                    {settings['AFO_PROD_SCHEMA']}.{settings['META_POI_GOOGLE_MAPS']}
                                SET 
                                    last_run_ts = current_timestamp
                                WHERE
                                    hauptkategorie_neu = {settings['HAUPTKATEGORIE']}
                                --AND
                                  --  kategorie_neu in ({",".join(settings['KATEGORIE'])})
                                ;

                                UPDATE
                                    {settings['AFO_PROD_SCHEMA']}.{settings['META_POI_GOOGLE_MAPS']}
                                SET 
                                    next_run_date = case  
                                        when periodicity = 'DAILY' then current_timestamp + INTERVAL '1 day'
                                        when periodicity = 'WEEKLY' then current_timestamp + INTERVAL '1 week'
                                        when periodicity = 'MONTHLY' then current_timestamp + INTERVAL '1 month'
                                        when periodicity = 'YEARLY' then current_timestamp + INTERVAL '1 year'
                                        else null
                                    end
                                    ;
                            """
            conn.execute(update_query)
            logger.info(f"Updated {settings['AFO_PROD_SCHEMA']}.{settings['META_POI_GOOGLE_MAPS']} with last_run_ts and next_run_date")

        except Exception as ex:
            error_msg = f"Error updating the '{settings['AFO_PROD_SCHEMA']}.{settings['META_POI_GOOGLE_MAPS']}' table: {ex}"
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
            
    except Exception as ex:
        error_msg = f"General Error: {ex}"
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
        return 0  
    
if __name__ == "__main__":
    main()
