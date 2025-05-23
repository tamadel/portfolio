----------------------------------------------------------------------------------------
-- Redshift GOOGLE MAP PROJECT
----------------------------------------------------------------------------------------

-- (1) TABLE: geo_afo_prod.meta_poi_categories_business_data_aktuell 

drop table if exists
	google_maps_dev.meta_poi_categories_business_data_aktuell;

CREATE TABLE IF NOT EXISTS 
	google_maps_dev.meta_poi_categories_business_data_aktuell
(
	category_id varchar(50) NULL,
	category_en varchar(50) NULL,
	category_de varchar(50) NULL,
	hauptkategorie_neu varchar(50) NULL,
	kategorie_neu varchar(50) NULL,
	poi_typ_neu varchar(50) NULL,
	kategorie_alt varchar(50) NULL,
	poi_typ_alt varchar(50) NULL
)
;

copy
    google_maps_dev.meta_poi_categories_business_data_aktuell 
from
    's3://webgis-redshift-data-exchange/geo_afo_prod/meta_poi_categories_business_data_aktuell'
iam_role
    'arn:aws:iam::878087211183:role/WebGISRedshiftCopyUnloadRole,arn:aws:iam::043826194356:role/043826194356-afo-marketingxaccount-redshift'
FORMAT
	csv
delimiter
    '|'
IGNOREHEADER
    1
quote
	'"'
;

/*
-- Errors
select 
	*
from 
	stl_load_errors
	;
	

select 
	st_setsrid(geo_point_lv95, 2056) 
from
	google_maps_dev.tmp_afo_pois_hotel_restaurant
;
*/




-- (2) TABLE: geo_afo_prod.meta_poi_google_maps_category

drop table if exists
	google_maps_dev.meta_poi_google_maps_category;

CREATE TABLE IF NOT EXISTS 
	google_maps_dev.meta_poi_google_maps_category
(
	hauptkategorie_neu varchar(50) NULL,
	kategorie_neu varchar(50) NULL,
	poi_typ_neu varchar(50) NULL,
	periodicity text NULL,
	last_run_ts timestamp NULL,
	next_run_date date NULL,
	"depth" numeric NULL
)
;

copy
    google_maps_dev.meta_poi_google_maps_category 
from
    's3://webgis-redshift-data-exchange/geo_afo_prod/meta_poi_google_maps_category'
iam_role
    'arn:aws:iam::878087211183:role/WebGISRedshiftCopyUnloadRole,arn:aws:iam::043826194356:role/043826194356-afo-marketingxaccount-redshift'
FORMAT
	csv
delimiter
    '|'
IGNOREHEADER
    1
quote
	'"'
;


-- (3) TABLE: geo_afo_prod.imp_plz6_geo_neu

drop table if exists
	google_maps_dev.imp_plz6_geo_neu;

CREATE TABLE IF NOT EXISTS 
	google_maps_dev.imp_plz6_geo_neu
(
	gid text NULL,
	plz_id text NULL,
	plz numeric NULL,
	plz_ort text null,
	plzzus text NULL,
	plz6 numeric NULL,
	modified text NULL,
	shape_area text NULL,
	qm float8 NULL,
	qkm float8 NULL,
	shape_len float8 NULL,
	geo_poly_lv95 geometry NULL,
	geo_poly_lv03 geometry NULL,
	geo_poly_wgs84 geometry NULL,
	geo_point_lv95 geometry NULL,
	geo_point_lv03 geometry NULL,
	geo_point_wgs84 geometry NULL
)
;

copy
    google_maps_dev.imp_plz6_geo_neu 
from
    's3://webgis-redshift-data-exchange/geo_afo_prod/imp_plz6_geo_neu'
iam_role
    'arn:aws:iam::878087211183:role/WebGISRedshiftCopyUnloadRole,arn:aws:iam::043826194356:role/043826194356-afo-marketingxaccount-redshift'
FORMAT
	csv
delimiter
    '|'
IGNOREHEADER
    1
quote
	'"'
;


select 
	*
from 
	stl_load_errors
	;



-- (4) TABLE: geo_afo_prod.imp_gmd_geo_neu
drop table if exists
	google_maps_dev.imp_gmd_geo_neu;

CREATE TABLE IF NOT EXISTS 
	google_maps_dev.imp_gmd_geo_neu
(
	icc text NULL,
	gmd_nr int8 NULL,
	gemeinde text NULL,
	kanton_nr float8 NULL,
	kanton text NULL,
	bzr_nr float8 NULL,
	einwohnerz int8 NULL,
	hist_nr float8 NULL,
	herkunft_j int8 NULL,
	objekt_art text NULL,
	gem_flaech float8 NULL,
	geo_poly_lv95 geometry NULL,
	geo_poly_lv03 geometry NULL,
	geo_poly_wgs84 geometry NULL
)
;

copy
    google_maps_dev.imp_gmd_geo_neu 
from
    's3://webgis-redshift-data-exchange/geo_afo_prod/imp_gmd_geo_neu'
iam_role
    'arn:aws:iam::878087211183:role/WebGISRedshiftCopyUnloadRole,arn:aws:iam::043826194356:role/043826194356-afo-marketingxaccount-redshift'
FORMAT
	csv
delimiter
    '|'
IGNOREHEADER
    1
quote
	'"'
;


select 
	*
from 
	stl_load_errors
	;



-- (5) TABLE: geo_afo_prod.mv_qu_gbd_gwr_aktuell
drop table if exists
	google_maps_dev.mv_qu_gbd_gwr_aktuell;

CREATE TABLE IF NOT EXISTS 
	google_maps_dev.mv_qu_gbd_gwr_aktuell
(
	eg_ed_id TEXT,
    egid TEXT,
    gdekt TEXT,
    gdenr TEXT,
    gdename TEXT,
    gstat TEXT,
    gstatlab_de TEXT,
    gkode TEXT,
    gkodn TEXT,
    edid TEXT,
    egaid TEXT,
    esid TEXT,
    strname TEXT,
    deinr TEXT,
    strsp TEXT,
    dplz4 TEXT,
    dplzz TEXT,
    dplzname TEXT,
    dkode TEXT,
    dkodn TEXT,
    doffadr TEXT,
    strname_std TEXT,
    deinr_std TEXT,
    gueltig_von TIMESTAMP,
    gueltig_bis TIMESTAMP,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    geo_point_eg_lv95 GEOMETRY,
    geo_point_eg_lv03 GEOMETRY,
    geo_point_eg_wgs84 GEOMETRY,
    geo_point_ed_lv95 GEOMETRY,
    geo_point_ed_lv03 GEOMETRY,
    geo_point_ed_wgs84 GEOMETRY
)
;

copy
    google_maps_dev.mv_qu_gbd_gwr_aktuell 
from
    's3://webgis-redshift-data-exchange/geo_afo_prod/mv_qu_gbd_gwr_aktuell'
iam_role
    'arn:aws:iam::878087211183:role/WebGISRedshiftCopyUnloadRole,arn:aws:iam::043826194356:role/043826194356-afo-marketingxaccount-redshift'
FORMAT
	csv
delimiter
    '|'
IGNOREHEADER
    1
quote
	'"'
;


-- (6) TABLE: google_maps_dev.google_map_hotel_gastronomie
drop table if exists
	google_maps_dev.google_map_hotel_gastronomie;

CREATE TABLE IF NOT EXISTS 
	google_maps_dev.google_map_hotel_gastronomie
(
	cid text NULL,
	exact_match int4 NULL,
	kw_long text NULL,
	keyword text NULL,
	afo_hauptkategorie text NULL,
	afo_poi_typ text NULL,
	afo_category text NULL,
	category_en_ids varchar(10000) NULL,
	category_de_ids varchar(10000) NULL,
	title text NULL,
	address text NULL,
	strasse_h_no text NULL,
	strasse text NULL,
	hausnummer text NULL,
	plz4 text NULL,
	ort text NULL,
	"domain" text NULL,
	url varchar(10000) NULL,
	phone text NULL,
	anz_fotos int4 NULL,
	google_bewertung text NULL,
	anz_bewertungen text NULL,
	work_hours super NULL,
	status text NULL,
	geo_point_lv95 geometry NULL,
	category_ids_de super NULL,
	category_ids super NULL,
	longitude float8 NULL,
	latitude float8 NULL
)
;

copy
    google_maps_dev.google_map_hotel_gastronomie 
from
    's3://webgis-redshift-data-exchange/google_maps_dev/google_map_hotel_gastronomie'
iam_role
    'arn:aws:iam::878087211183:role/WebGISRedshiftCopyUnloadRole,arn:aws:iam::043826194356:role/043826194356-afo-marketingxaccount-redshift'
FORMAT
	csv
delimiter
    '|'
IGNOREHEADER
    1
quote
	'"'
;


-- (7) TABLE: google_maps_dev_test.google_map_hotel_gastro_rest
drop table if exists
	google_maps_dev.google_map_hotel_gastro_rest;

CREATE TABLE IF NOT EXISTS 
	google_maps_dev.google_map_hotel_gastro_rest
(
	cid text NULL,
	exact_match int4 NULL,
	kw_long text NULL,
	keyword text NULL,
	afo_hauptkategorie text NULL,
	afo_poi_typ text NULL,
	afo_category text NULL,
	category_en_ids varchar(10000) NULL,
	category_de_ids varchar(10000) NULL,
	title text NULL,
	address text NULL,
	strasse_h_no text NULL,
	strasse text NULL,
	hausnummer text NULL,
	plz4 text NULL,
	ort text NULL,
	"domain" text NULL,
	url varchar(10000) NULL,
	phone text NULL,
	anz_fotos int4 NULL,
	google_bewertung text NULL,
	anz_bewertungen text NULL,
	work_hours super NULL,
	status text NULL,
	geo_point_lv95 geometry NULL,
	category_ids_de super NULL,
	category_ids super NULL,
	longitude float8 NULL,
	latitude float8 null,
	opening_times varchar(10000) NULL,
	relevant numeric NULL
)
;

copy
    google_maps_dev.google_map_hotel_gastro_rest 
from
    's3://webgis-redshift-data-exchange/google_maps_dev_test/google_map_hotel_gastro_rest'
iam_role
    'arn:aws:iam::878087211183:role/WebGISRedshiftCopyUnloadRole,arn:aws:iam::043826194356:role/043826194356-afo-marketingxaccount-redshift'
FORMAT
	csv
delimiter
    '|'
IGNOREHEADER
    1
quote
	'"'
;



--////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

-------------------------------
-- Dienstleistung
-- Google POIs to Redshift
------------------------------
drop table if exists
	geo_afo_prod.google_abgleich_dienstleistung;

CREATE TABLE IF NOT EXISTS 
	geo_afo_prod.google_abgleich_dienstleistung
(
	cid varchar(225) null
	,strasse varchar(1000) null
	,plz4 numeric NULL
	,ort varchar(225) null
	,address varchar(1000) null
	,title varchar(1000) null
	,google_strasse varchar(1000) null
	,google_strasse_std varchar(1000) null
	,google_hausnum varchar(225) null
	,google_plz4 varchar(225) null
	,google_ort varchar(1000) null
	,gwr_strasse varchar(1000) null
	,gwr_hausnum varchar(225) null
	,gwr_plz4 int4 NULL
	,gwr_ort varchar(1000) null
	,plz6 varchar(225) null
	,gmd_nr numeric NULL
	,gemeinde varchar(1000) null
	,"domain" varchar(1000) null
	,url varchar(10000) null
	,google_poi_typ varchar(1000) null
	,category_ids varchar(10000) null
	,geo_point_lv95 geometry NULL
)
;

copy
    geo_afo_prod.google_abgleich_dienstleistung 
from
    's3://webgis-redshift-data-exchange/google_maps_dev_abgleich/google_abgleich_dienstleistung'
iam_role
    'arn:aws:iam::878087211183:role/WebGISRedshiftCopyUnloadRole,arn:aws:iam::043826194356:role/043826194356-afo-marketingxaccount-redshift'
FORMAT
	csv
delimiter
    '|'
IGNOREHEADER
    1
quote
	'"'
;



select 
	*
from 
	stl_load_errors
	;

-------------------------------
-- Dienstleistung
-- AFO POIs to Redshift
------------------------------
drop table if exists
	geo_afo_prod.afo_poi_typ_dienstleistung;

CREATE TABLE IF NOT EXISTS 
	geo_afo_prod.afo_poi_typ_dienstleistung
(
	poi_id numeric NULL
	,hauskey numeric NULL
	,poi_typ_id numeric NULL
	,poi_typ varchar(1000) null
	,company_group_id numeric NULL
	,company_group varchar(1000) null
	,company_id numeric NULL
	,company varchar(1000) null
	,company_unit varchar(1000) null
	,company_brand varchar(1000) null
	,bezeichnung_lang varchar(1000) null
	,bezeichnung_kurz varchar(1000) null
	,adresse varchar(1000) null
	,plz4 numeric NULL
	,ort varchar(1000) null
	,url varchar(10000) NULL
	,geo_point_lv95 geometry NULL
)
;

copy
    geo_afo_prod.afo_poi_typ_dienstleistung 
from
    's3://webgis-redshift-data-exchange/google_maps_dev_abgleich/afo_poi_typ_dienstleistung'
iam_role
    'arn:aws:iam::878087211183:role/WebGISRedshiftCopyUnloadRole,arn:aws:iam::043826194356:role/043826194356-afo-marketingxaccount-redshift'
FORMAT
	csv
delimiter
    '|'
IGNOREHEADER
    1
quote
	'"'
;

--////////////////////////////////////////////////////////////////////////////////////
-------------------------------
-- Einkaufszentrum
-- Google POIs to Redshift
------------------------------
drop table if exists
	geo_afo_prod.google_abgleich_einkaufszentrum;

CREATE TABLE IF NOT EXISTS 
	geo_afo_prod.google_abgleich_einkaufszentrum
(
	cid varchar(225) null
	,strasse varchar(1000) null
	,plz4 numeric NULL
	,ort varchar(225) null
	,address varchar(1000) null
	,title varchar(1000) null
	,google_strasse varchar(1000) null
	,google_strasse_std varchar(1000) null
	,google_hausnum varchar(225) null
	,google_plz4 varchar(225) null
	,google_ort varchar(1000) null
	,gwr_strasse varchar(1000) null
	,gwr_hausnum varchar(225) null
	,gwr_plz4 int4 NULL
	,gwr_ort varchar(1000) null
	,plz6 varchar(225) null
	,gmd_nr numeric NULL
	,gemeinde varchar(1000) null
	,"domain" varchar(1000) null
	,url varchar(10000) null
	,google_poi_typ varchar(1000) null
	,category_ids varchar(10000) null
	,geo_point_lv95 geometry NULL
)
;

copy
    geo_afo_prod.google_abgleich_einkaufszentrum 
from
    's3://webgis-redshift-data-exchange/google_maps_dev_abgleich/google_abgleich_einkaufszentrum'
iam_role
    'arn:aws:iam::878087211183:role/WebGISRedshiftCopyUnloadRole,arn:aws:iam::043826194356:role/043826194356-afo-marketingxaccount-redshift'
FORMAT
	csv
delimiter
    '|'
IGNOREHEADER
    1
quote
	'"'
;



select 
	*
from 
	stl_load_errors
	;

-------------------------------
-- Einkaufszentrum
-- AFO POIs to Redshift
------------------------------
drop table if exists
	geo_afo_prod.afo_poi_typ_einkaufszentrum ;

CREATE TABLE IF NOT EXISTS 
	geo_afo_prod.afo_poi_typ_einkaufszentrum 
(
	poi_id numeric NULL
	,hauskey numeric NULL
	,poi_typ_id numeric NULL
	,poi_typ varchar(1000) null
	,company_group_id numeric NULL
	,company_group varchar(1000) null
	,company_id numeric NULL
	,company varchar(1000) null
	,company_unit varchar(1000) null
	,company_brand varchar(1000) null
	,bezeichnung_lang varchar(1000) null
	,bezeichnung_kurz varchar(1000) null
	,adresse varchar(1000) null
	,plz4 numeric NULL
	,ort varchar(1000) null
	,url varchar(10000) NULL
	,geo_point_lv95 geometry NULL
)
;

copy
    geo_afo_prod.afo_poi_typ_einkaufszentrum  
from
    's3://webgis-redshift-data-exchange/google_maps_dev_abgleich/afo_poi_typ_einkaufszentrum'
iam_role
    'arn:aws:iam::878087211183:role/WebGISRedshiftCopyUnloadRole,arn:aws:iam::043826194356:role/043826194356-afo-marketingxaccount-redshift'
FORMAT
	csv
delimiter
    '|'
IGNOREHEADER
    1
quote
	'"'
;









































