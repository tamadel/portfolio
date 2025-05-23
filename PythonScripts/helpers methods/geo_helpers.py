from logging import raiseExceptions
import requests
import time
import json
import geopandas as gpd

from urllib.parse import quote_plus

#------------------------------------------------------------------------------------------
# Isochronen pro <ISO_MINUTES_RANGE_1> rechnen
#------------------------------------------------------------------------------------------
def isochrone_isomin_range(
    df_input, 
    conn, 
    target_table, 
    iso_time_range, 
    unit, 
    standort_id_col, 
    iso_korrektur, 
    logger, 
    here_api_key, 
    iso_type, 
    geom_col, 
    szenario_id_col = None,
    mode = "car",
    add_to_table = False,
    geoselection = False
):

    # mode: "car" / "pedestrian" 
    # iso_type: "time" (default) in Sekunden / "distance" in metern
    # unit: "minutes" / "seconds" / "meters"

    SCRIPT_NAME = "isochrone_isomin_range()"
    BASE_SRID =4326
    URL = "https://isoline.route.ls.hereapi.com/routing/7.2/calculateisoline.json"

    # Unit setzen
    if unit.lower() == "minutes":
        time_factor_api = 60
        time_factor_table = 1
    elif unit.lower() == "seconds":
        time_factor_api = 1
        time_factor_table = 60.0
    elif unit.lower() == "meters":
        time_factor_api = 1.0
        time_factor_table = 1.0
    else:
        error_msg = f"ungültige time unit ({unit}), nur 'minutes' oder 'seconds' erlaubt"
        raise (error_msg)

    if add_to_table == False:
        try:
            if szenario_id_col:
                query = f"""
                    DROP TABLE IF EXISTS 
                        {target_table};

                    CREATE TABLE 
                        {target_table}
                    (
                        szenario_id NUMERIC,
                        standort_ID NUMERIC,
                        id_iso NUMERIC,
                        minutes NUMERIC,
                        geom Geometry,
                        geom_diff Geometry
                    )
                """
            else:
                query = f"""
                    DROP TABLE IF EXISTS 
                        {target_table};

                    CREATE TABLE 
                        {target_table}
                    (
                        standort_ID NUMERIC,
                        id_iso NUMERIC,
                        minutes NUMERIC,
                        geom Geometry,
                        geom_diff Geometry
                    )
                """
            conn.execute( query )
            logger.debug( f"{SCRIPT_NAME} - Isochronen-Tabelle '{target_table}' erstellt" )

        except Exception as ex:
            error_msg = f"{SCRIPT_NAME} - Fehler beim Erstellen der Isochronen-Tabelle '{target_table}': {ex}"
            raise(error_msg)


    for index, row in df_input.iterrows():
        try:
            standort_id = row[standort_id_col]

            logger.info(
                f"{SCRIPT_NAME} - Isochronen-Berechnung (1) für Standort {standort_id} gestartet und in Tabelle '{target_table}' eingetragen")
                
            for time_step in iso_time_range:  

            

                i = 0
                success = False
                error_msg = None   
                while i < 10 and success == False:
                    
                    if geoselection == True:
                        x_coord = row['x_koord'] 
                        y_coord = row['y_koord']
                    else:    
                        x_coord = row[geom_col].centroid.x
                        y_coord = row[geom_col].centroid.y
                    iso_seconds = int(round((time_step*iso_korrektur)*time_factor_api,0))

                    if mode == "pedestrian":
                        url = f"{URL}?apiKey={here_api_key}&mode=shortest;pedestrian&start=geo!{y_coord},{x_coord}&departure=2023-03-12T10:00:00&range={iso_seconds}&rangetype={iso_type}"
                    else:
                        url = f"{URL}?apiKey={here_api_key}&mode=shortest;car;traffic:disabled&start=geo!{y_coord},{x_coord}&departure=2023-03-12T10:00:00&range={iso_seconds}&rangetype={iso_type}"

                    r = requests.get(url)

                    if r.status_code == 200:
                        data = json.loads(r.text)
                        logger.debug(f"{SCRIPT_NAME} - Isochronen für time_step={time_step} gerechnet")
                        success = True
                    else:
                        i += 1
                        time.sleep(3 + i)

                if success == False:
                    error_msg = f"{SCRIPT_NAME} - Fehler beim Rechnen der Isochronen für time_step={time_step}: {r.text}"
                    raise(error_msg)

                # ------------------------------------------------------------------------------------------
                # linestring für DB-Tabelle erstellen
                # ------------------------------------------------------------------------------------------
                try:
                    isochrone = data["response"]["isoline"][0]["component"][0]["shape"]
                    linestring = ""
                    for point in isochrone:
                        linestring += f"{point.split(',')[1]} {point.split(',')[0]},"

                    logger.debug(f"{SCRIPT_NAME} - Linestring der Isochronen für time_step={time_step} erstellt")

                except Exception as ex:
                    error_msg = f"{SCRIPT_NAME} - Fehler beim Erstellen des Linestrings der Isochronen für time_step={time_step}: {ex}"
                    raise(error_msg)
                # ------------------------------------------------------------------------------------------
                # Isochronen in DB-Tabelle eintragen
                # ------------------------------------------------------------------------------------------
                try:
                    time_min = time_step/time_factor_table
                    if szenario_id_col:
                        szenario_id = szenario_id_col

                        query = f"""
                            insert into 
                                {target_table}
                            (
                                szenario_id,
                                standort_ID,
                                id_iso,
                                minutes,
                                geom
                            )
                            values
                                (
                                    {szenario_id},
                                    {standort_id},
                                    {time_step}, 
                                    {time_min}, 
                                    ST_SetSRID( ST_MakePolygon( ST_GeomFromText('LINESTRING({linestring[:-1]})') ), {BASE_SRID} )
                                )
                            """
                    else:

                        query = f"""
                            insert into 
                                {target_table}
                            (
                                standort_ID,
                                id_iso,
                                minutes,
                                geom
                            )
                            values
                                (
                                    {standort_id},
                                    {time_step}, 
                                    {time_min}, 
                                    ST_SetSRID( ST_MakePolygon( ST_GeomFromText('LINESTRING({linestring[:-1]})') ), {BASE_SRID} )
                                )
                            """
                    conn.execute(query)
                    logger.debug(
                        f"{SCRIPT_NAME} - Isochronen in Tabelle '{target_table}' eingetragen")

                except Exception as ex:
                    error_msg = f"{SCRIPT_NAME} - Fehler beim Eintragen der Isochronen in der Tabelle '{target_table}': {ex}"
                    raise(error_msg)

            logger.info(
                f"{SCRIPT_NAME} - Isochronen-Berechnung (1) für Standort {standort_id} abgeschlossen und in Tabelle '{target_table}' eingetragen")

        except Exception as ex:
            error_msg = f"{SCRIPT_NAME} - Fehler bei der Isochronen-Berechnung (1) für Standort {standort_id}: {ex}"
            raise(error_msg)     

    #------------------------------------------------------------------------------------------
    # Geom Differenzen
    #------------------------------------------------------------------------------------------
    try:
        query = f"""
            UPDATE
                {target_table} t0
            SET
                geom_diff = u.geom_diff
            FROM
                (
                    SELECT
                        standort_ID,
                        minutes,
                        CASE
                            WHEN standort_ID=lead(standort_ID) over (order by standort_ID, minutes desc) 
                            THEN ST_SetSRID(st_difference(geom, lead(geom) over (order by standort_ID, minutes desc)), 4326)
                            ELSE ST_SetSRID(geom, 4326)
                        END AS geom_diff
                    FROM
                        {target_table}
                ) u
            WHERE
                t0.standort_ID = u.standort_ID
            AND
                t0.minutes = u.minutes
            ;
        """
        conn.execute(query)
        logger.debug(f"{SCRIPT_NAME} - Geom_diff in Tabelle '{target_table}' eingetragen")

    except Exception as ex:
        raise Exception(ex)

    return 0
