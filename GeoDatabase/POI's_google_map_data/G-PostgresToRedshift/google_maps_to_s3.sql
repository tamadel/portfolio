----------------------------------------------------------------------------------------
-- Postgres
-- -> Geometrie muss als Text exportiert werden!!!
----------------------------------------------------------------------------------------

-- (1) TABLE: geo_afo_prod.meta_poi_categories_business_data_aktuell 
select  
	*
from 
    aws_s3.query_export_to_s3('
		select
			category_id
			,category_en 
			,category_de 
			,hauptkategorie_neu 
			,kategorie_neu 
			,poi_typ_neu 
			,kategorie_alt 
			,poi_typ_alt
		from 
			 geo_afo_prod.meta_poi_categories_business_data_aktuell
    ',
    aws_commons.create_s3_uri(
        'webgis-redshift-data-exchange', 
        'geo_afo_prod/meta_poi_categories_business_data_aktuell', 			-- anpassen !
        'eu-central-1'
    ), 
    options := 'format csv, header true, delimiter $$|$$, QUOTE ''"'', FORCE_QUOTE * '
)
;


-- (2) TABLE: geo_afo_prod.meta_poi_google_maps_category
select  
	*
from 
    aws_s3.query_export_to_s3('
		select
			hauptkategorie_neu 
			,kategorie_neu 
			,poi_typ_neu 
			,periodicity 
			,last_run_ts 
			,next_run_date 
			,depth
		from 
			 geo_afo_prod.meta_poi_google_maps_category
    ',
    aws_commons.create_s3_uri(
        'webgis-redshift-data-exchange', 
        'geo_afo_prod/meta_poi_google_maps_category', 			-- anpassen !
        'eu-central-1'
    ), 
    options := 'format csv, header true, delimiter $$|$$, QUOTE ''"'', FORCE_QUOTE * '
)
;


-- (3) TABLE: geo_afo_prod.imp_plz6_geo_neu
select  
	*
from 
    aws_s3.query_export_to_s3('
		select
			gid 
			,plz_id 
			,plz 
			,plz_ort
			,plzzus 
			,plz6 
			,modified 
			,shape_area 
			,qm 
			,qkm 
			,shape_len 
			,ST_AsText(geo_point_lv95) 	as geo_point_lv95
			,ST_AsText(geo_point_lv03) 	as geo_point_lv03
			,ST_AsText(geo_point_wgs84)  as geo_point_wgs84  
			,ST_AsText(geo_poly_lv95)  	as geo_poly_lv95
			,ST_AsText(geo_poly_lv03)  	as geo_poly_lv03
			,ST_AsText(geo_poly_wgs84) 	as geo_poly_wgs84  
		from 
			 geo_afo_prod.imp_plz6_geo_neu
    ',
    aws_commons.create_s3_uri(
        'webgis-redshift-data-exchange', 
        'geo_afo_prod/imp_plz6_geo_neu', 			-- anpassen !
        'eu-central-1'
    ), 
    options := 'format csv, header true, delimiter $$|$$, QUOTE ''"'', FORCE_QUOTE * '
)
;



-- (4) TABLE: geo_afo_prod.imp_gmd_geo_neu
select  
	*
from 
    aws_s3.query_export_to_s3('
		select
			icc
			,gmd_nr 
			,gemeinde 
			,kanton_nr 
			,kanton 
			,bzr_nr 
			,einwohnerz 
			,hist_nr 
			,herkunft_j 
			,objekt_art 
			,gem_flaech 
			,ST_AsText(geo_poly_lv95)  	as geo_poly_lv95
			,ST_AsText(geo_poly_lv03)  	as geo_poly_lv03
			,ST_AsText(geo_poly_wgs84) 	as geo_poly_wgs84  
		from 
			 geo_afo_prod.imp_gmd_geo_neu
    ',
    aws_commons.create_s3_uri(
        'webgis-redshift-data-exchange', 
        'geo_afo_prod/imp_gmd_geo_neu', 			-- anpassen !
        'eu-central-1'
    ), 
    options := 'format csv, header true, delimiter $$|$$, QUOTE ''"'', FORCE_QUOTE * '
)
;



-- (5) TABLE: geo_afo_prod.mv_qu_gbd_gwr_aktuell
select  
	*
from 
    aws_s3.query_export_to_s3('
		select
			eg_ed_id
		    ,egid
		    ,gdekt
		    ,gdenr
		    ,gdename
		    ,gstat
		    ,gstatlab_de
		    ,gkode
		    ,gkodn
		    ,edid
		    ,egaid
		    ,esid
		    ,strname
		    ,deinr
		    ,strsp
		    ,dplz4
		    ,dplzz
		    ,dplzname
		    ,dkode
		    ,dkodn
		    ,doffadr
		    ,strname_std
		    ,deinr_std
		    ,gueltig_von
		    ,gueltig_bis
		    ,created_ts
		    ,updated_ts
			,ST_AsText(geo_point_eg_lv95)  	as geo_point_eg_lv95
			,ST_AsText(geo_point_eg_lv03)  	as geo_point_eg_lv03
			,ST_AsText(geo_point_eg_wgs84) 	as geo_point_eg_wgs84  
			,ST_AsText(geo_point_ed_lv95)  	as geo_point_ed_lv95
			,ST_AsText(geo_point_ed_lv03)  	as geo_point_ed_lv03
			,ST_AsText(geo_point_ed_wgs84) 	as geo_point_ed_wgs84  
		from 
			 geo_afo_prod.mv_qu_gbd_gwr_aktuell
    ',
    aws_commons.create_s3_uri(
        'webgis-redshift-data-exchange', 
        'geo_afo_prod/mv_qu_gbd_gwr_aktuell', 			-- anpassen !
        'eu-central-1'
    ), 
    options := 'format csv, header true, delimiter $$|$$, QUOTE ''"'', FORCE_QUOTE * '
)
;





-- (6) TABLE: google_maps_dev.google_map_hotel_gastronomie
select  
	*
from 
    aws_s3.query_export_to_s3('
		select
			cid
			,exact_match 
			,kw_long 
			,keyword 
			,afo_hauptkategorie 
			,afo_poi_typ 
			,afo_category 
			,category_en_ids 
			,category_de_ids 
			,title 
			,address 
			,strasse_h_no 
			,strasse 
			,hausnummer 
			,plz4 
			,ort 
			,"domain" 
			,url 
			,phone 
			,anz_fotos 
			,google_bewertung 
			,anz_bewertungen 
			,work_hours 
			,status 
			,ST_AsText(geo_point_lv95)  	as geo_poly_lv95
			,category_ids_de 
			,category_ids 
			,longitude 
			,latitude 			
		from 
			 google_maps_dev.google_map_hotel_gastronomie
    ',
    aws_commons.create_s3_uri(
        'webgis-redshift-data-exchange', 
        'google_maps_dev/google_map_hotel_gastronomie', 			-- anpassen !
        'eu-central-1'
    ), 
    options := 'format csv, header true, delimiter $$|$$, QUOTE ''"'', FORCE_QUOTE * '
)
;




 -- (7) TABLE: google_maps_dev_test.google_map_hotel_gastro_rest
select  
	*
from 
    aws_s3.query_export_to_s3('
		select
			cid
			,exact_match 
			,kw_long 
			,keyword 
			,afo_hauptkategorie 
			,afo_poi_typ 
			,afo_category 
			,category_en_ids 
			,category_de_ids 
			,title 
			,address 
			,strasse_h_no 
			,strasse 
			,hausnummer 
			,plz4 
			,ort 
			,"domain" 
			,url 
			,phone 
			,anz_fotos 
			,google_bewertung 
			,anz_bewertungen 
			,work_hours 
			,status 
			,ST_AsText(geo_point_lv95)  	as geo_poly_lv95
			,category_ids_de 
			,category_ids 
			,longitude 
			,latitude
			,opening_times 
			,relevant		
		from 
			 google_maps_dev_test.google_map_hotel_gastro_rest
    ',
    aws_commons.create_s3_uri(
        'webgis-redshift-data-exchange', 
        'google_maps_dev_test/google_map_hotel_gastro_rest', 			-- anpassen !
        'eu-central-1'
    ), 
    options := 'format csv, header true, delimiter $$|$$, QUOTE ''"'', FORCE_QUOTE * '
)
;

--//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


-----------------------------
-- Dienstleistung
-- Google POIs
-----------------------------
select  
	*
from 
    aws_s3.query_export_to_s3('
        select 
			cid 
			,strasse 
			,plz4 
			,ort 
			,address 
			,title 
			,google_strasse
			,google_strasse_std 
			,google_hausnum 
			,google_plz4 
			,google_ort 
			,gwr_strasse 
			,gwr_hausnum 
			,gwr_plz4 
			,gwr_ort
			,plz6
			,gmd_nr
			,gemeinde
			,"domain" 
			,url 
			,google_poi_typ
			,category_ids
			,ST_AsText(geo_point_lv95) as geo_point_lv95		-- Geometrie muss als Text exportiert werden
        from
            google_maps_dev_abgleich.google_abgleich_dienstleistung
    ',
    aws_commons.create_s3_uri(
        'webgis-redshift-data-exchange', 
        'google_maps_dev_abgleich/google_abgleich_dienstleistung', 			-- anpassen !
        'eu-central-1'
    ), 
    options := 'format csv, header true, delimiter $$|$$, QUOTE ''"'', FORCE_QUOTE * '
)
;




-----------------------------
-- Dienstleistung
-- AFO POIs
-----------------------------
select 
	*
from 
    aws_s3.query_export_to_s3('
        select 
			poi_id
			,hauskey
			,poi_typ_id
			,poi_typ
			,company_group_id
			,company_group
			,company_id
			,company
			,company_unit
			,company_brand
			,bezeichnung_lang
			,bezeichnung_kurz
			,adresse
			,plz4
			,ort
			,url
			,ST_AsText(geo_point_lv95) as geo_point_lv95		-- Geometrie muss als Text exportiert werden
        from
            google_maps_dev_abgleich.afo_poi_typ_dienstleistung 
    ',
    aws_commons.create_s3_uri(
        'webgis-redshift-data-exchange', 
        'google_maps_dev_abgleich/afo_poi_typ_dienstleistung', 			-- anpassen !
        'eu-central-1'
    ), 
    options := 'format csv, header true, delimiter $$|$$, QUOTE ''"'', FORCE_QUOTE * '
)
;

--////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
-----------------------------
-- Einkaufszentrum
-- Google POIs
-----------------------------
select  
	*
from 
    aws_s3.query_export_to_s3('
        select 
			cid 
			,strasse 
			,plz4 
			,ort 
			,address 
			,title 
			,google_strasse
			,google_strasse_std 
			,google_hausnum 
			,google_plz4 
			,google_ort 
			,gwr_strasse 
			,gwr_hausnum 
			,gwr_plz4 
			,gwr_ort
			,plz6
			,gmd_nr
			,gemeinde
			,"domain" 
			,url 
			,google_poi_typ
			,category_ids
			,ST_AsText(geo_point_lv95) as geo_point_lv95		-- Geometrie muss als Text exportiert werden
        from
            google_maps_dev_abgleich.google_abgleich_einkaufszentrum
    ',
    aws_commons.create_s3_uri(
        'webgis-redshift-data-exchange', 
        'google_maps_dev_abgleich/google_abgleich_einkaufszentrum', 			-- anpassen !
        'eu-central-1'
    ), 
    options := 'format csv, header true, delimiter $$|$$, QUOTE ''"'', FORCE_QUOTE * '
)
;




-----------------------------
-- Einkaufszentrum
-- AFO POIs
-----------------------------
select 
	*
from 
    aws_s3.query_export_to_s3('
        select 
			poi_id
			,hauskey
			,poi_typ_id
			,poi_typ
			,company_group_id
			,company_group
			,company_id
			,company
			,company_unit
			,company_brand
			,bezeichnung_lang
			,bezeichnung_kurz
			,adresse
			,plz4
			,ort
			,url
			,ST_AsText(geo_point_lv95) as geo_point_lv95		-- Geometrie muss als Text exportiert werden
        from
            google_maps_dev_abgleich.afo_poi_typ_einkaufszentrum 
    ',
    aws_commons.create_s3_uri(
        'webgis-redshift-data-exchange', 
        'google_maps_dev_abgleich/afo_poi_typ_einkaufszentrum', 			-- anpassen !
        'eu-central-1'
    ), 
    options := 'format csv, header true, delimiter $$|$$, QUOTE ''"'', FORCE_QUOTE * '
)
;








































