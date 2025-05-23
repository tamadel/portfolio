
import sqlalchemy as sa
import requests
import json
import os
import shutil
import pandas as pd
import geopandas as gpd
import time
import pdb
from geo_helpers import isochrone_isomin_range
from helpers import getSecrets

from urllib.parse import quote_plus


# #----------------------------------------------------------------------------------------------------
# # getSecrets(): Secrets aus AWS Secrets Manager extrahieren
# #----------------------------------------------------------------------------------------------------
# def getSecrets( aws_region, secret_key_list ):
#     import boto3
#     import base64
    
#     # Secrets Manager Client erstellen
#     try:
#         session = boto3.session.Session()
#         client = session.client(
#             service_name='secretsmanager',
#             region_name = aws_region
#         )
    
#     except Exception as ex:
#         raise Exception(ex)

#     secrets = {}
#     for secret_key in secret_key_list:
#         try:
#             get_secret_value_response = client.get_secret_value(
#                 SecretId = secret_key
#             )
#             if 'SecretString' in get_secret_value_response:
#                 secret = json.loads( get_secret_value_response['SecretString'] ) 
#             else:
#                 secret = json.loads( base64.b64decode(get_secret_value_response['SecretBinary']) )

#             secrets[ secret_key ] = secret
        
#         except Exception as ex:
#             raise Exception(ex)

#     return secrets


def main():
    from datetime import datetime
    from helpers import initLogger
    from helpers import checkAndMakeDir

    ROOT = os.path.dirname( os.path.abspath(__file__) ).replace("\\","/").replace("/src","")
    SCRIPT_NAME = os.path.basename(__file__).split(".")[:-1][0]

    #------------------------------------------------------------------------------------------
    # Manuelles einfügen der Koordinaten (poi_id -> x_koord, y_koord)
    #------------------------------------------------------------------------------------------
    # df = pd.DataFrame()
    # ALDI_STANDORTE = {
    #     # Seengen
    #     "99999999": {
    #         "x_koord": 7.268874994012521,
    #         "y_koord": 47.14203340306385
    #     }
        #,
        #Langnau
        #"99999998": {
        #    "x_koord": 7.7794051996357,
        #    "y_koord": 46.939962469208
        #}

    # }

    #------------------------------------------------------------------------------------------
    # Logger initialisieren
    #------------------------------------------------------------------------------------------
    logger = initLogger(SCRIPT_NAME, stdout=True, log_level="DEBUG")
    logger.info( f"{SCRIPT_NAME} - Logger initialisiert")

    #------------------------------------------------------------------------------------------
    # globale Settings laden
    #------------------------------------------------------------------------------------------
    global_settings_file = f"{ROOT}/config/global_settings.json"
    with open(global_settings_file) as f:
        global_settings = json.load(f)

    #------------------------------------------------------------------------------------------
    # Secrets aus AWS Secret Manager extrahieren
    #------------------------------------------------------------------------------------------
    secrets = getSecrets( global_settings['AWS_REGION'], global_settings['SECRETS'] )

    #------------------------------------------------------------------------------------------
    # Postgres-Settings setzen
    #------------------------------------------------------------------------------------------
    postgres_settings = {
        "host": secrets['WEBGIS_POSTGRES']['HOST'],
        "port": secrets['WEBGIS_POSTGRES']['PORT'],
        "geo_database": secrets['WEBGIS_POSTGRES']['GEO_DATABASE'],
        "webgis_database": secrets['WEBGIS_POSTGRES']['WEBGIS_DATABASE'],
        "user": secrets['WEBGIS_REPORTING']['USER'],
        "pw": secrets['WEBGIS_REPORTING']['PW']
    }

    #------------------------------------------------------------------------------------------
    # DB-Verbindung auf Postgres herstellen
    #------------------------------------------------------------------------------------------
    try:
        postgres_engine = sa.create_engine( 
            f"postgresql+psycopg2://{postgres_settings['user']}:{postgres_settings['pw']}@{postgres_settings['host']}:{postgres_settings['port']}/{postgres_settings['geo_database']}"
        )
        conn = postgres_engine.connect()

    except Exception as ex:
        raise Exception(ex)

    time_range_list = range(1,16)
    
    #------------------------------------------------------------------------------------------
    # Standorte aus Tabelle einlesen, Tabelle vorbereiten
    # Im dBeaver Berechtigungen für webgis_admin der Tabelle und dem Schema freischalten
    #------------------------------------------------------------------------------------------
    try:
        query = f"""
                    select
                        *,
                        geo_point_wgs84 as geom
                    FROM 
                        intervista_modellierung.weitere_food_convenience_pois
                    ;
                """
        df_frame = gpd.read_postgis( query, conn )
        logger.debug( f"{SCRIPT_NAME} -  POIS importiert ({df_frame.shape[0]} Records)")

    except Exception as ex:
        raise Exception(ex)
    
    #------------------------------------------------------------------------------------------
    # Berechnung der Isochronen
    # Der Zieltabelle Berechtigungen für webgis_admin via dBeaver geben
    #------------------------------------------------------------------------------------------
    # Mögliche Settings:
    # target_table: Zieltabelle, Berechtigungen für webgis_admin im dBeaver freigeben
    # iso_time_range: entweder als range(x,y) oder als einzelne Einträge in Liste [x,y,z]
    # unit: "minutes" / "seconds" / "meters"
    # standort_id_col: Spalte, welche die ID beinhaltet, meistens bei uns poi_id, muss in der Input Tabelle vorhanden sein
    # iso_korrektur: Default einfach 1.0 lassen, wird nur für WSM V3 Grundmodell auf 1.07 gestellt, 
    # mode: "car" / "pedestrian" - Auto oder zu Fuss
    # iso_type: "time" (default) in Sekunden / "distance" in metern
    #
    #

    isochrone_isomin_range(
        df_input=df_frame,
        conn=conn,
        target_table = "intervista_modellierung.weitere_food_convenience_pois_iso_2000_diff_liv",
        #iso_time_range = [1, 2, 3],
        iso_time_range = [100,250,500,1000,2000],
        unit = "meters",
        standort_id_col = 'poi_id',
        iso_korrektur = 1.0,
        #iso_korrektur = 1.07,
        mode = 'pedestrian',
        logger = logger,
        here_api_key=secrets['HERE_API_KEY'],
        iso_type = "distance",
        geom_col = 'geom'
    )

if __name__ == '__main__':
    main()
