import pandas as pd
import geopandas as gpd
import sqlalchemy as sa
import requests
import zipfile
import json 
import os
import boto3
from datetime import datetime, timedelta
from helpers import stopExecution
from helpers import checkAndMakeDir
from helpers import initLogger
from helpers import sendSESEmail
from helpers import getSecrets
from helpers import deleteFilesInS3Folder
from helpers import exportDataFrameToS3
from helpers import getFoldersInS3Bucket

#===================================
# Specific Functions
#==================================
# Function to get the most recent date from 'id' and return the corresponding Shapefile href
def get_most_recent_shapefile_href(stac_data, settings): # befor main() method 
    most_recent_id = None
    most_recent_date = None

    # Iterate over all features to find the most recent one based on the 'id' date
    for feature in stac_data['features']:
        
        # Extract the date part from the 'id' ("id": "swissboundaries3d_2024-01") >> ('2024-01')
        id_date_str = feature['id'].split('_')[1]
        id_date = datetime.strptime(id_date_str, "%Y-%m")

        # Update if this is the most recent date found
        if most_recent_date is None or id_date > most_recent_date:
            most_recent_date = id_date
            most_recent_id = feature['id']
            most_recent_feature = feature
            
    # Extract the Shapefile href from the 'assets' of the most recent feature
    if most_recent_feature:
        shapefile_key = f"swissboundaries3d_{most_recent_id.split('_')[1]}_{settings['EPSG_LV95']}_{settings['DATASET_VERSION']}{settings['FILE_EXTENSION']}" #in settings 2 
        url = most_recent_feature['assets'].get(shapefile_key, {}).get('href')
        
        return most_recent_id, url 

##==========================##
# Main Programm              #
##==========================##
def main():

    ROOT = os.path.dirname(os.path.abspath(__file__)).replace( r"/src", "" ).replace( r"\src", "" )
    SCRIPT_NAME = os.path.basename(__file__).split(".")[:-1][0]
    
    conn = None
    n_delete = n_update = n_new = n_replace_poly = n_overwritten_poly= 0
    

    ##------------------------------------------
    # upload>> globale Settings 
    ##------------------------------------------
    global_settings_file = f"{ROOT}/config/global_settings.json"
    with open(global_settings_file) as f:
        global_settings = json.load(f)
        
    ##------------------------------------------
    # upload>> Settings upload  
    ##------------------------------------------
    settings_file = f"{ROOT}/config/{SCRIPT_NAME}/settings.json"
    with open(settings_file) as f:
        settings = json.load(f)
        
    ##------------------------------------------
    # initiate>> Logger  
    ##------------------------------------------
    logger = initLogger(SCRIPT_NAME, stdout=True, log_level=global_settings['LOG_LEVEL'])
    logger.info(f"{SCRIPT_NAME} - Logger initialized")
    
    ##------------------------------------------
    # Set>> Email-Settings
    ##------------------------------------------
    try:
        emailSettings = {
            "email_from": settings['EMAIL_FROM'],
            "recipients": settings['EMAIL_RECEIVER'],
            "support": settings['EMAIL_SUPPORT'],
            "subject": settings['EMAIL_SUBJECT']
        }
        logger.info(f"{SCRIPT_NAME} - Email-Settings gesetzt ({emailSettings})")
        
    except Exception as ex:
        msg = str(ex)
        error_msg = f"Fehler beim Setzen der Email-Settings: {msg}" 
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        raise Exception(error_msg)
    
    ##------------------------------------------
    # Extract>> Secrets from AWS Secret Manager
    ##------------------------------------------
    try:
        secrets = getSecrets(global_settings['AWS_REGION'], settings['SECRETS'])
        logger.debug(f"{SCRIPT_NAME} - Secrets '{settings['SECRETS']}' aus AWS Secrets Manager geladen")
        
    except Exception as ex:
        msg = str(ex)
        error_msg = f"Fehler beim Laden der Secrets '{settings['SECRETS']}' aus AWS Secrets Manager: {ex}"
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        raise Exception(error_msg)
    
    
    ##------------------------------------------
    # Create>> DB connection
    ##------------------------------------------
    try:
        engine = sa.create_engine(f"postgresql+psycopg2://{secrets['GEO_DATABASE']['USER']}:{secrets['GEO_DATABASE']['PW']}@{secrets['GEO_DATABASE']['HOST']}:{secrets['GEO_DATABASE']['PORT']}/{secrets['GEO_DATABASE']['DATABASE']}")
        conn = engine.connect()
        logger.info(f"{SCRIPT_NAME} - Verbindung zu '{secrets['GEO_DATABASE']['HOST']}' mit User '{secrets['GEO_DATABASE']['USER']}' erstellt")
        
    except Exception as ex:
        msg = str(ex)
        error_msg = f"Fehler beim Erstellen der Verbindung zu '{secrets['GEO_DATABASE']['HOST']}' mit User '{secrets['GEO_DATABASE']['USER']}': {msg}"
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        raise Exception(error_msg)
    
    
    ##------------------------------------------
    # List S3 archive directories (New: Part I)
    ##------------------------------------------
    try:
        archiv_list = getFoldersInS3Bucket( # I have to use getfilefunction ??
            bucket=settings['S3_DATA_EXCHANGE_BUCKET'],
            prefix=settings['S3_ARCHIVE_PREFIX']
        )
        logger.debug( f"{SCRIPT_NAME} - Liste der Archiv-Verzeichnisse in 's3://{settings['S3_DATA_EXCHANGE_BUCKET']}/{settings['S3_ARCHIVE_PREFIX']}' erstellt ({len(archiv_list)} Verzeichnisse)" )
    
    except Exception as ex:
        msg = str(ex)
        error_msg = f"Fehler beim Erstellen der Liste der Archiv-Verzeichnisse in 's3://{settings['S3_DATA_EXCHANGE_BUCKET']}/{settings['S3_ARCHIVE_PREFIX']}': {msg}"
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        raise Exception(error_msg)
    
    ##------------------------------------------
    # Create>> Temp Dir 
    ##------------------------------------------
    tmp_dir = f"{ROOT}/tmp"
    retval, msg = checkAndMakeDir(tmp_dir)
    if retval == 0:
        logger.debug(f"{SCRIPT_NAME} - {msg}")
    else:
        error_msg = f"Fehler beim Erstellen des Temp-Verzeichnisses '{tmp_dir}': {msg}"
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        return 1
    
    ##------------------------------------------
    # Create>> Swisstopo data 
    ##------------------------------------------
    swisstopo_data = f"{tmp_dir}/{SCRIPT_NAME}"
    retval, msg = checkAndMakeDir(swisstopo_data)
    if retval == 0:
        logger.debug(f"{SCRIPT_NAME} - {msg}")
    else:
        error_msg = f"Fehler beim Erstellen des swisstopo-Daten-Verzeichnisses '{swisstopo_data}': {msg}"
        stopExecution( script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn )
        return 1
    
    ##------------------------------------------
    # Create>> Temp path
    ##------------------------------------------
    tmp_path = f"{swisstopo_data}/tmp"
    retval, msg = checkAndMakeDir(tmp_path)
    if retval == 0:
        logger.debug( f"{SCRIPT_NAME} - {msg}" )

    else:
        error_msg = f"Fehler beim Erstellen des swisstopo-tmp-Verzeichnisses '{tmp_path}': {msg}"
        stopExecution( script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn )
        return 1

    ##------------------------------------------
    # Download>> Updated SWISSTOPO File
    ##------------------------------------------
    try:# API request to get the STAC data
        api_url = settings['SWISSTOPO_DOWNLOAD_URL_ROOT']
        response = requests.get(api_url)
        response.raise_for_status()
        stac_data = response.json()
    
        logger.info(f"{SCRIPT_NAME} - Erfolgreicher Abruf der STAC-Daten von der API.")
        
    except Exception as ex:
        msg = str(ex)
        error_msg = f"Fehler beim Abrufen der STAC-Daten von der API'{settings['SWISSTOPO_DOWNLOAD_URL_ROOT']}': {msg}"
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        return 1
    
    ##------------------------------------------
    # Download>> Get the most recent Shapefile href
    ##------------------------------------------
    try:
        # Get the most recent Shapefile href
        most_recent_id, url = get_most_recent_shapefile_href(stac_data, settings)
        
        if not most_recent_id and url:
            raise Exception("Kein geeignetes Shapefile für das aktuelle Jahr gefunden.")
        
        logger.info(f"{SCRIPT_NAME} - Erfolgreiches Abrufen des neuesten Shapefile-Links:'{url}'")
    
    except Exception as ex:
        msg = str(ex)
        error_msg = f" {SCRIPT_NAME} - Fehler beim Abrufen des neuesten Shapefile-Links:{msg}"
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        return 1
    

    ##------------------------------------------
    # Download>> Download Shapefile Zip
    ##------------------------------------------
    try:
        # Download the url (Zip file)
        r = requests.get(url, allow_redirects=True)
        r.raise_for_status()
        zip_path = f"{tmp_path}/{settings['SWISSTOPO_DOWNLOAD_URL_ASSETS']}"
        with open(zip_path, 'wb') as f:
            f.write(r.content)
            
        logger.info(f"{SCRIPT_NAME} - Erfolgreicher Download der Zip-Datei von'{url}'")

    except Exception as ex:
        msg = str(ex)
        error_msg = f"Fehler beim Herunterladen der Zip-Datei von '{settings['SWISSTOPO_DOWNLOAD_URL_ROOT']}': {msg}"
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        return 1
    
    ##------------------------------------------
    # Unzip>> Extract Files from Downloaded ZIP
    ##------------------------------------------
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(tmp_path)
        logger.info(f"{SCRIPT_NAME} - Dateien erfolgreich extrahiert von:{zip_path}")
        
    except Exception as ex:
        msg = str(ex)
        error_msg = f"Fehler beim Extrahieren der Datei'{zip_path}': {msg}"
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        return 1
        
    ##-----------------------------------------------------
    # Process>> Load and Transform the Extracted Shapefiles
    ##-----------------------------------------------------
    try:
        shapefile_path_gebiet = os.path.join(tmp_path, settings['SHAPEFILE_GMD_GEBIET'])
        #shapefile_path_grenze = os.path.join(tmp_path, settings['SHAPEFILE_GMD_GRENZE'])
        
        # Load the shapefiles using GeoPandas
        gdf_gebiet  = gpd.read_file(shapefile_path_gebiet)
        #gdf_grenze  = gpd.read_file(shapefile_path_grenze)
        
        logger.info(f"{SCRIPT_NAME} - Shapefiles erfolgreich geladen")
        
        try:
            geojson_path = f"{shapefile_path_gebiet}.geoJSON"
            gdf_gebiet.to_file(geojson_path, driver='GeoJSON')
            gdf_geojson_gb = gpd.read_file(geojson_path)
            logger.info(f"{SCRIPT_NAME} - GeoJSON-Datei generiert: {geojson_path}")
            
        except Exception as ex:
            msg = str(ex)
            error_msg = f"Fehler beim Generieren der GeoJSON-Datei: {msg}"
            stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
            return 1
        
    except Exception as ex:
        msg = str(ex)
        error_msg = f"Fehler beim Laden der Shapefiles: {msg}"
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        return 1
    
   
    #---------------------------------------------------------
    # S3: new PartII - Check, Create Folder, and Upload to S3
    #---------------------------------------------------------
    # I need to check if I have this file "most_recent_id" in S3 Archive list or not if I have then stop if not I need to upload it in to S3 archive
    # in case there is  a new file so I need to integrate email that we found new file 
    try:
        # Check if the most_recent_id is already in the S3 archive list
        most_recent_s3_file_key = f"{settings['S3_ARCHIVE_PREFIX']}/"
        file_name = f"{most_recent_id}"
        
        if file_name in archiv_list:
            logger.info(f"{SCRIPT_NAME} - Der File '{file_name}' ist bereits im S3-Archiv vorhanden.")
            return 0  # Stop the script, as the folder already exists
        else:
            # logger.info(f"{SCRIPT_NAME} - Neuer Ordner '{file_name}' wird erstellt und Dateien werden hochgeladen.")
            
            # # Create a folder in S3
            # s3_client = boto3.client('s3')
            # s3_client.put_object(Bucket=settings['S3_DATA_EXCHANGE_BUCKET'], Key=most_recent_s3_folder_key)
            # logger.info(f"{SCRIPT_NAME} - Ordner '{file_name}' erfolgreich im S3-Archiv erstellt.")
            
            # Now upload the shapefile data into this new folder
            s3_client = boto3.client('s3')
            archive_file_key = f"{most_recent_s3_file_key}{most_recent_id}.zip"
            s3_client.upload_file(zip_path, settings['S3_DATA_EXCHANGE_BUCKET'], archive_file_key)
            logger.info(f"{SCRIPT_NAME} - Die Datei '{most_recent_id}.zip' wurde erfolgreich nach 's3://{settings['S3_DATA_EXCHANGE_BUCKET']}/{archive_file_key}' hochgeladen.")
            
            # Send an email notification that a new folder and file were uploaded
            email_body = f"""
                            <html>
                                <body>
                                    <p>{SCRIPT_NAME} - Ein neuer Ordner '{most_recent_id}' wurde im S3-Archiv erstellt.</p>
                                    <p>Die Datei wurde erfolgreich nach 's3://{settings['S3_DATA_EXCHANGE_BUCKET']}/{archive_file_key}' hochgeladen.</p>
                                </body>
                            </html>
            """
            sendSESEmail(
                aws_region='eu-central-1',
                sender=emailSettings['email_from'],
                recipients=emailSettings['recipients'],
                subject=f"NEUER ORDNER UND DATEI HOCHGELADEN - {emailSettings['subject']}",
                body_html=email_body
            )
            logger.info(f"{SCRIPT_NAME} - Eine Benachrichtigung über den neuen Ordner und Dateiupload wurde per E-Mail gesendet.")

    except Exception as ex:
        msg = str(ex)
        error_msg = f"Fehler beim Erstellen des Ordners und Hochladen der Datei '{most_recent_id}' in das S3-Archiv: {msg}"
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        raise Exception(error_msg)


    
    
    
    ##-------------------------------------------------------------
    # Data upload into <QUELLTABELLE_GMD> 
    ##-------------------------------------------------------------
    try:
        # Insert the new raw data into our database
        gdf_geojson_gb.to_postgis(settings['QUELLTABELLE_GMD'], conn, schema=settings['GEO_PROD_SCHEMA'], if_exists='replace', index=False)
        #gdf_geojson_gz.to_postgis(settings['QUELLTABELLE_GMD_GRZ'], conn, schema=settings['GEO_PROD_SCHEMA'], if_exists='replace', index=False)
        
        rec_count = pd.read_sql(f"SELECT COUNT(*) FROM {settings['GEO_PROD_SCHEMA']}.{settings['QUELLTABELLE_GMD']}", conn).iloc[0][0]
        logger.info(f"{SCRIPT_NAME} - Gmd-Daten erfolgreich in die Tabelle '{settings['GEO_PROD_SCHEMA']}.{settings['QUELLTABELLE_GMD']}' eingefügt ({rec_count} Records)")
        
    except Exception as ex:
        msg = str(ex)
        error_msg = f"Fehler bei die Tabelle '{settings['GEO_PROD_SCHEMA']}.{settings['QUELLTABELLE_GMD']}' in DB Einzufügen: {msg}"
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        return 1  
     
    ##-------------------------------------------------------------
    # Create a clean Table <IMP_GMD_GEBIET_DB> 
    ##-------------------------------------------------------------
    try:
        query = f"""
                    drop table if exists 
                        {settings['GEO_PROD_SCHEMA']}.{settings['IMP_GMD_GEBIET_DB']}
                    ; 
                    
                    create table 
                        {settings['GEO_PROD_SCHEMA']}.{settings['IMP_GMD_GEBIET_DB']}
                    as
                    select
                        "ICC" as icc
                        ,"BFS_NUMMER" as gmd_nr
                        ,"NAME" as gemeinde
                        ,"KANTONSNUM" as kanton_nr
                        ,NULL as kanton
                        ,"BEZIRKSNUM" as bzr_nr
                        ,"EINWOHNERZ" as einwohnerz
                        ,"HIST_NR" as hist_nr
                        ,"HERKUNFT_J" as herkunft_j
                        ,"OBJEKTART" as objekt_art
                        ,"GEM_FLAECH" as gem_flaech
                        ,ST_Force2D(ST_SetSRID(ST_Multi(geometry), 2056)) as geo_poly_lv95
                        ,ST_Transform(ST_SetSRID(ST_Force2D(ST_Multi(geometry)), 2056), 21781) as geo_poly_lv03  
                        ,ST_Transform(ST_SetSRID(ST_Force2D(ST_Multi(geometry)), 2056), 4326) as geo_poly_wgs84
                    from 
                       {settings['GEO_PROD_SCHEMA']}.{settings['QUELLTABELLE_GMD']}
                    where 
                        "ICC" in ({",".join([f"'{x}'" for x in settings['COUNTRY_CODE_LIST']])}) 
                        and
                        "OBJEKTART" in ({",".join([f"'{x}'" for x in settings['OBJEKTART']])}) 
                    ;
            """
        conn.execute( query )   
        rec_count = pd.read_sql(f"SELECT COUNT(*) FROM {settings['GEO_PROD_SCHEMA']}.{settings['IMP_GMD_GEBIET_DB']}", conn).iloc[0][0]
        n_new = rec_count
        logger.info(f"{SCRIPT_NAME} - GMD Tabelle '{settings['GEO_PROD_SCHEMA']}.{settings['IMP_GMD_GEBIET_DB']}' ist erfolgreich erstellt ({rec_count} Records)")
        
    except Exception as ex:
        msg = str(ex)
        error_msg = f"Fehler beim Aktualisieren der Datenbank mit die Neu Geo Gemeinde Tabelle '{settings['GEO_PROD_SCHEMA']}.{settings['IMP_GMD_GEBIET_DB']}': {msg}"
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        return 1 
        
    # create a temp table `tmp_lay_gmd_geo_hist` for test
    try:    
        query = f"""
                    drop table if exists 
                       {settings['GEO_PROD_SCHEMA']}.{settings['TEST_LAY_GMD_GEO_HIST']}
                    ;
                    create table 
                        {settings['GEO_PROD_SCHEMA']}.{settings['TEST_LAY_GMD_GEO_HIST']}
                    as
                    select
                        t0.*,
                        t1.kantons_nr as kanton_nr
                    from 
                        {settings['GEO_PROD_SCHEMA']}.{settings['LAY_GMD_GEO_HIST']} t0
                    left join
                        {settings['GEO_PROD_SCHEMA']}.{settings['KANTON_MAP']}  t1
                    on
                        t0.kanton = t1.kanton
                    ;
        """
        conn.execute(query)
        
    except Exception as ex:
        msg = str(ex)
        error_msg = f"Fehler beim Erstellung Hist Test Tabelle: {msg}"
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        return 1     
         
    # Run the SQL query to update `tmp_lay_gmd_geo_hist`
    #-----------------------------
    # Delete/deactivate old gmd
    #----------------------------- 
    try:    
        query = f"""  
                    update
                        {settings['GEO_PROD_SCHEMA']}.{settings['TEST_LAY_GMD_GEO_HIST']}
                    set
                        gueltig_bis = current_date,
                        updated_ts = current_timestamp
                    where
                        gmd_nr in(
                                    select
                                        gmd_nr
                                    from
                                        {settings['GEO_PROD_SCHEMA']}.{settings['TEST_LAY_GMD_GEO_HIST']}
                                    where
                                        gmd_nr not in(
                                                    select
                                                        gmd_nr 
                                                    from
                                                        {settings['GEO_PROD_SCHEMA']}.{settings['IMP_GMD_GEBIET_DB']}
                                        )
                                        and 
                                        extract(year from gueltig_bis) = 9999
                        )
                        and
                        extract(year from gueltig_bis) = 9999
                    ;
        """    
        rec_count = conn.execute(query).rowcount
        n_delete = rec_count
        logger.info(f"{SCRIPT_NAME} - veraltete Records in '{settings['GEO_PROD_SCHEMA']}.{settings['TEST_LAY_GMD_GEO_HIST']}' deaktiviert ({rec_count} Records)")
        
    except Exception as ex:
        msg = str(ex)
        error_msg = f"Fehler beim Deaktievieren veralteten Records in '{settings['GEO_PROD_SCHEMA']}.{settings['TEST_LAY_GMD_GEO_HIST']}': {msg}"
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        return 1 
    
    #-----------------------------
    # insert new records
    #-----------------------------
    try:    # add all row for the new gmd 
        query = f"""            
                    insert into 
                        {settings['GEO_PROD_SCHEMA']}.{settings['TEST_LAY_GMD_GEO_HIST']} 
                    (
                        gmd_nr
                        ,gemeinde
                        ,kanton
                        --,kanton_nr
                        ,geo_poly_lv03
                        ,geo_poly_lv95
                        ,geo_poly_wgs84
                        ,gueltig_von
                        ,gueltig_bis
                        ,created_ts
                        ,updated_ts
                    )
                    select  
                        n.gmd_nr
                        ,n.gemeinde
                        ,n.kanton
                        --,n.kanton_nr
                        ,n.geo_poly_lv03   
                        ,n.geo_poly_lv95     
                        ,n.geo_poly_wgs84 
                        ,current_date as gueltig_von
                        ,'9999-12-31' as gueltig_bis
                        ,current_timestamp as created_ts
                        ,current_timestamp as updated_ts                                                                              
                    from  
                        {settings['GEO_PROD_SCHEMA']}.{settings['IMP_GMD_GEBIET_DB']} n
                    where  
                        not exists (
                                    select		
                                        n.gmd_nr 
                                    from
                                        {settings['GEO_PROD_SCHEMA']}.{settings['TEST_LAY_GMD_GEO_HIST']} h
                                    where
                                        h.gmd_nr = n.gmd_nr
                                        and
                                        extract(year from gueltig_bis) = 9999
                        );
        """ 
        rec_count = conn.execute(query).rowcount
        n_update = rec_count
        logger.info(f"{SCRIPT_NAME} - Datenbank wurde erfolgreich mit neuen gmd-Daten aktualisiert ({rec_count} Records)")
        
    except Exception as ex:
        msg = str(ex)
        error_msg = f"Fehler bei den neuen Gemeinde hinzufügen: {msg}"
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        return 1     
    
    #-----------------------------------------------------------------
    # Step 1: Create temp table with proper SRID transformation
    #-----------------------------------------------------------------
    try:
        query = f"""
                    drop table if exists 
                        tmp_perce_overlap
                    ;
                    
                    create temp 
                        table tmp_perce_overlap
                    as
                    select
                        t0.gid,
                        t0.gmd_nr,
                        t0.gemeinde,
                        t0.kanton,
                        t0.kanton_nr,
                        t1.objekt_art,
                        -- Ensure all geometries are in SRID 2056 (or another common SRID)
                        ST_SetSRID(t0.geo_poly_lv95, 2056) as old_poly_lv95,
                        ST_SetSRID(t1.geo_poly_lv95, 2056) as new_poly_lv95,
                        t0.geo_poly_lv03 as old_poly_lv03,
                        ST_Transform(ST_SetSRID(t1.geo_poly_lv95, 2056), 21781) as new_poly_lv03,
                        t0.geo_poly_wgs84 as old_poly_wgs84,
                        ST_Transform(ST_SetSRID(t1.geo_poly_lv95, 2056), 4326) as new_poly_wgs84,
                        t0.gueltig_von,
                        t0.gueltig_bis,
                        t0.created_ts,
                        t0.updated_ts,
                        round(ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056))) as area_old,
                        round(ST_Area(ST_SetSRID(t1.geo_poly_lv95, 2056))) as area_new,
                        round(ST_Area(ST_Intersection(
                                        ST_SetSRID(t0.geo_poly_lv95, 2056),
                                        ST_SetSRID(t1.geo_poly_lv95, 2056)
                                    )
                                )
                            ) as area_overlap,
                        ST_Area(ST_Intersection(ST_SetSRID(t0.geo_poly_lv95, 2056), ST_SetSRID(t1.geo_poly_lv95, 2056))) / ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056)) as percentage_overlap
                    from
                        {settings['GEO_PROD_SCHEMA']}.{settings['TEST_LAY_GMD_GEO_HIST']} t0 
                    join
                        {settings['GEO_PROD_SCHEMA']}.{settings['IMP_GMD_GEBIET_DB']} t1
                    on
                        t0.gmd_nr = t1.gmd_nr
                    where
                        extract(year from t0.gueltig_bis) = 9999
                    ;
        """
        rec_count = conn.execute(query).rowcount
        n_overlap_area = rec_count
        logger.info(f"{SCRIPT_NAME} - Eine temporäre Tabelle für Percentage Overlap erstellen ({rec_count} Records)")

    except Exception as ex:
        msg = str(ex)
        error_msg = f"Fehler beim Erstellen der temporären Tabelle für Percentage Overlap: {msg}"
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        return 1     
        
    #-----------------------------------------------------------------
    # Step 2: Update polygons where overlap is ≥ 0.995
    #-----------------------------------------------------------------            
    try:
        query = f"""
                    update
                        {settings['GEO_PROD_SCHEMA']}.{settings['TEST_LAY_GMD_GEO_HIST']} h
                    set 
                        geo_poly_lv95 = u.new_poly_lv95,
                        geo_poly_lv03 = u.new_poly_lv03,
                        geo_poly_wgs84 = u.new_poly_wgs84,
                        gueltig_von = current_date,
                        updated_ts = current_timestamp 
                    from 
                        tmp_perce_overlap u
                    where 
                        u.percentage_overlap >= 0.995
                        and
                        h.gmd_nr = u.gmd_nr
                        and
                        extract(year from h.gueltig_bis) = 9999
                    ;
        """
        rec_count = conn.execute(query).rowcount
        n_overwritten_poly = rec_count
        logger.info(f"{SCRIPT_NAME} - Polygone werden aktualisiert, bei denen die Überlappung ≥ 0.995 ist ({rec_count} Records)")
    
    except Exception as ex:
        msg = str(ex)
        error_msg = f"Fehler beim Aktualisieren der Polygone mit einer Überlappung von ≥ 0.995:{msg}"
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        return 1
            
    #-----------------------------------------------------------------
    # Step 3: Deactivate polygons where overlap is < 0.995
    #-----------------------------------------------------------------            
    try:
        query = f"""
                    update
                        {settings['GEO_PROD_SCHEMA']}.{settings['TEST_LAY_GMD_GEO_HIST']} h
                    set 
                        gueltig_bis = current_date,
                        updated_ts = current_timestamp 
                    from 
                        tmp_perce_overlap u
                    where 
                        u.percentage_overlap < 0.995
                        and
                        h.gmd_nr = u.gmd_nr
                        and 
                        extract(year from h.gueltig_bis) = 9999
                    ;
        """
        rec_count = conn.execute(query).rowcount
        n_deactivated_poly = rec_count
        logger.info(f"{SCRIPT_NAME} - Polygone erfolgreich deaktiviert, bei denen die Überlappung < 0.995 beträgt ({rec_count} Records)")
    
    except Exception as ex:
        msg = str(ex)
        error_msg = f"Fehler beim Deaktivieren der Polygone, bei denen die Überlappung < 0.995 beträgt:{msg}"
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        return 1
    
    #-----------------------------------------------------------------
    # Step 4: Insert new polygons where overlap < 0.995
    #-----------------------------------------------------------------            
    try:
        query = f"""
                    insert into 
                        {settings['GEO_PROD_SCHEMA']}.{settings['TEST_LAY_GMD_GEO_HIST']}(
                            gmd_nr,
                            gemeinde,
                            kanton,
                            kanton_nr,
                            geo_poly_lv03,
                            geo_poly_lv95,
                            geo_poly_wgs84,
                            gueltig_von,
                            gueltig_bis,
                            created_ts,
                            updated_ts
                        )
                    select  
                        n.gmd_nr,
                        n.gemeinde,
                        n.kanton,
                        n.kanton_nr,
                        n.new_poly_lv03,
                        n.new_poly_lv95,
                        n.new_poly_wgs84,
                        current_date as gueltig_von,
                        '9999-12-31' as gueltig_bis,
                        current_timestamp as created_ts,
                        current_timestamp as updated_ts
                    from  
                        tmp_perce_overlap n
                    where
                        n.percentage_overlap < 0.995
                    ;
        """
        rec_count = conn.execute(query).rowcount
        n_inserted_poly = rec_count
        logger.info(f"{SCRIPT_NAME} - Datenbank wurde erfolgreich mit neuen gmd-polygon aktualisiert ({rec_count} Records)")
    
    except Exception as ex:
        msg = str(ex)
        error_msg = f"Fehler bei Aktualisierung den neuen gmd-polygon:{msg}"
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        return 1
    
    ##----------------------------------------------
    #  Upload processed files to S3  (New: Part III)
    ##----------------------------------------------
    # is this part still needed ??! 
    try:
        s3_client = boto3.client('s3')
        file_list = swisstopo_data # List of files to be uploaded
        for file in file_list:
            try:
                file_name = os.path.basename(file) # or f"{swisstopo_data}/{file}" 
                s3_file_name = f"{settings['S3_TMP_PREFIX']}/{file_name}"
                
                logger.debug( f"{SCRIPT_NAME} - Upload von '{file_name}' nach 's3://{settings['S3_DATA_EXCHANGE_BUCKET']}/{s3_file_name}' gestartet..." )
                response = s3_client.upload_file( file, settings['S3_DATA_EXCHANGE_BUCKET'], s3_file_name)  # I would use the "file_name" if it gives me full path
                logger.debug( f"{SCRIPT_NAME} - Upload von '{file_name}' nach 's3://{settings['S3_DATA_EXCHANGE_BUCKET']}/{s3_file_name}' abgeschlossen" )

            except Exception as ex:
                msg = str(ex)
                error_msg = f"Fehler beim Upload von '{file_name}' nach 's3://{settings['S3_DATA_EXCHANGE_BUCKET']}/{s3_file_name}': {msg}"    
                stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
                raise Exception(error_msg)
                
    except Exception as ex:
        msg = str(ex)
        error_msg = f"Error uploading files to S3: {msg}"
        stopExecution(script=SCRIPT_NAME, status_msg=error_msg, logger=logger, error_flag=True, sendEmailFlag=True, emailSettings=emailSettings, connection=conn)
        raise Exception(error_msg)

    ##------------------------------------------
    # Notify Completion>> Send an Email
    ##------------------------------------------
    try:
        html_table = f"""
        <p>The {SCRIPT_NAME} script ran successfully and updated the database 'tmp_lay_gmd_geo_hist' with the latest SwissTopo data.</p>
        <table>
            <tr><th>Item</th><th>Count</th></tr>
            <tr><td>Deactivated GMD_Nr</td><td>{n_delete}</td></tr>
            <tr><td>Added GMD_Nr</td><td>{n_update}</td></tr>
            <tr><td>Count of Updated GMDs</td><td>{n_new}</td></tr>
            <tr><td>Replaced Polygons</td><td>{n_overwritten_poly}</td></tr>
            <tr><td>Deactivated Polygons</td><td>{n_deactivated_poly}</td></tr>
            <tr><td>Replaced Polygons</td><td>{n_inserted_poly}</td></tr>
        </table>
        """    
        html_table += f"""
            </table>
        """
            
        email_body = f"""
                        <html>
                            <body>
                                <p>{SCRIPT_NAME} - Download und Aufbereitung der neuen Gemeinde Geo-Daten erfolgreich abgeschlossen.</p>
                                {html_table}
                            </body>
                        </html>
        """ 
        sendSESEmail(
            aws_region = 'eu-central-1',
            sender = emailSettings["email_from"],
            recipients = emailSettings["recipients"],
            subject = f"SUCCESS - {emailSettings['subject']}",
            body_html = email_body
        )
        logger.debug( f"Email-Benachrichtigung an {emailSettings['recipients']} gesendet" )

    except Exception as ex:
        logger.warning( f"Fehler beim Versenden der Email-Benachrichtigung an {emailSettings['recipients']}" )

if __name__ == "__main__":
    main()