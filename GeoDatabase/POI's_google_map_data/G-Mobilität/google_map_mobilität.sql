--==================================================================================
-- DFSEO: Hauptkategorie >> "Mobilität"
--        kategory >> "Fahrzeughandel und -werkstatt" und "Fahrzeugvermietung"
--        poi_typ >> alle 9 poi_typ
--Datum: 23.09.2024
--==================================================================================
----------------------------------------------
-- Schritt 1: Auswahl der Point of Interest (POI)-Kategorie
----------------------------------------------
select 
	*
from 
	geo_afo_prod.meta_poi_google_maps_category
where 
	hauptkategorie_neu = 'Mobilität'
	and 
	kategorie_neu in ('Fahrzeughandel und -werkstatt', 'Fahrzeugvermietung')
;




-------------------------------------------------
--Schritt 2: Erstellung der Metadatentabelle (T1) - POST-Anfrage
-------------------------------------------------
select 
	*
from 
	google_maps_dev.google_map_metadata_mobilität
where 
	hauptkategorie_neu = 'Mobilität'
	and 
	kategorie_neu in ('Fahrzeughandel und -werkstatt', 'Fahrzeugvermietung')
;




alter table google_maps_dev.google_map_results_mobilität
add column poi_typ text
;

update google_maps_dev.google_map_results_mobilität
set 
	poi_typ = TRIM(poi_typ)
;

-------------------------------------------------
--Schritt 3: Abrufen der Rohdatentabelle (T2) - GET-Anfrage
-------------------------------------------------
select 
	*
from 
	google_maps_dev.google_map_results_mobilität 
where 
	main_category = 'Mobilität'
	and 
	category in ('Fahrzeughandel und -werkstatt', 'Fahrzeugvermietung')
;

select 
	*
from 
	google_maps_dev.google_map_results_mobilität
where 
	category_ids = '["auto_body_shop"]'
;


------------------------------------------------------------------
-- Schritt 4: Erstellung der Tabelle für eindeutige Einträge (T3)
------------------------------------------------------------------
DROP TABLE IF EXISTS 
          google_maps_dev.google_map_items_mobilität;

CREATE TABLE 
          google_maps_dev.google_map_items_mobilität 
AS
SELECT
    t0.cid,
    t0.rank_absolute,
    t0.keyword,
    t0.poi_typ,
    t0.exact_match,
    t0.address_info->>'zip' AS plz4,
    t0.address_info->>'city' AS ort,
    t0.address_info->>'address' AS strasse,
    t0.address_info->>'country_code' AS country_code,
    t0.address,
    t0.title,
    t0.phone,
    t0.domain,
    t0.url,
    t0.rating,
    t0.total_photos,
    t0.hotel_rating,
    t0.category,
    t0.additional_categories,
    t0.category_ids,
    t0.work_hours,
    ST_Transform(ST_SetSRID(ST_MakePoint(t0.longitude, t0.latitude), 4326), 2056) AS geo_point_lv95,
    t0.longitude,
    t0.latitude
FROM
    (
        SELECT
            t1.cid,
            t1.rank_absolute,
            t1.address_info,
            t1.keyword,
            t1.poi_typ,
            t1.exact_match,
            t1.address,
            t1.title,
            t1.phone,
            t1.domain,
            t1.url,
            t1.rating,
            t1.hotel_rating,
            t1.total_photos,
            t1.category,
            t1.additional_categories,
            t1.category_ids,
            t1.work_hours,
            t1.longitude,
            t1.latitude,
            ROW_NUMBER() OVER (PARTITION BY t1.cid ORDER BY t1.rank_absolute, 
            RANDOM()) AS row_num
        FROM
            google_maps_dev.google_map_results_mobilität  t1
        WHERE
            t1.address_info->>'country_code' = 'CH'
            and 
            t1.poi_typ in (
            				'Occasionshändler'
							,'Lieferwagenvermietung'
							,'Reifengeschäft'
							,'Autohändler'
							,'Motorscooterhändler'
							,'Motorradhändler'
							,'Autovermietung'
							,'Auto-Ersatzteilgeschäft'
							,'Autowerkstatt'
            )
    ) t0
WHERE
    t0.row_num = 1
;


select
    cid
    ,COUNT(*)
FROM 
    google_maps_dev.google_map_items_mobilität
GROUP BY 
    cid
HAVING  
    COUNT(*) > 1
;


------------------------------------------------------------------
-- Schritt 5: Filtern und Anreichern der Daten (T4)
----------------------------------------------------------------

-- add a new column "category_ids_de"
alter table  
	google_maps_dev.google_map_items_mobilität
add column  
	category_ids_de jsonb
;

-- update items table with "category_ids_de" values (apply this for pi and pii)
update
	google_maps_dev.google_map_items_mobilität
set
	category_ids_de = CASE
				        -- If `additional_categories` is NULL, insert only `category` into a JSONB array
				        when 
				        	additional_categories is null or additional_categories::text = 'null'
				        then
				        	to_jsonb(array[category])  -- Convert category to JSONB array
				        -- If `additional_categories` is a JSONB array, append `category` to the start of the array (Order here is important/ Reihnfolge hier ist wichtig) 
				        when
				        	jsonb_typeof(additional_categories::jsonb) = 'array'
				        then
				        	jsonb_insert(additional_categories::jsonb, '{0}', to_jsonb(category))  -- Insert category at the beginning of the JSONB array
				        -- If `additional_categories` is not an array, convert it and `category` to a JSONB array
				        else
				        	to_jsonb(array[category] || additional_categories::text)  -- Combine category and non-array additional_categories
				    end
;



-- Tabelle zur Entfaltung der Kategorien erstellen:
DROP TABLE IF EXISTS 
         google_maps_dev.google_map_kateg_mobilität;

CREATE TABLE 
        google_maps_dev.google_map_kateg_mobilität 
AS
SELECT 
    *
FROM (
    SELECT 
        cid,
        split_part(keyword, ' ', 1) AS keyword,
        exact_match,
        category,
        additional_categories,
        category_ids_de,
        category_ids,
        jsonb_array_elements_text(category_ids_de::jsonb) AS category_de,
        jsonb_array_elements_text(category_ids::jsonb) AS category_en,
        plz4,
        ort,
        strasse,
        address,
        title,
        phone,
        work_hours,
        work_hours->>'current_status' AS current_status,
        domain,
        url,
        rating->>'value' AS google_bewertung,
        rating->>'votes_count' AS anz_bewertungen,
        total_photos AS anz_fotos,
        geo_point_lv95,
        longitude,
        latitude 
    FROM 
        google_maps_dev.google_map_items_mobilität 
    WHERE 
        jsonb_typeof(category_ids_de::jsonb) = 'array'
        AND 
       jsonb_typeof(category_ids::jsonb) = 'array'
) t
WHERE 
    category_en IN (
               SELECT 
                  category_en
               FROM 
                  google_maps_dev.google_map_category_hierarchy
               WHERE 
                  hauptkategorie_neu = 'Mobilität'
				  and 
				  kategorie_neu in ('Fahrzeughandel und -werkstatt', 'Fahrzeugvermietung')
   )
;


---------------------------------------------------------------------------------------
-- Schritt 6: Hinzufügen neuer Attribute und Vervollständigung der fehlenden Daten (T5)
---------------------------------------------------------------------------------------
drop table if exists  
    google_maps_dev.google_map_mobilität;

create table  
    google_maps_dev.google_map_mobilität
as 
select  
    cid,
    keyword,
    category_ids_de,
    category_ids,
    STRING_AGG(distinct category_de, '/ ') as categories_de,
    STRING_AGG(distinct category_en, '/ ') as categories_en,
    title,
    address,
    strasse as str_hausnummer,
    REGEXP_REPLACE(strasse, '[0-9]+[A-Za-z]*$', '') as strasse,
    (REGEXP_MATCH(strasse, '[0-9]+[A-Za-z]*$'))[1] as hausnummer,
    plz4,
    ort,
    phone,
    domain,
    url,
    google_bewertung,
    anz_bewertungen,
    anz_fotos,
    geo_point_lv95,
    longitude,
    latitude
from 
    google_maps_dev.google_map_kateg_mobilität
group by 
    cid,
    title,
    plz4,
    ort,
    strasse,
    address,
    domain,
    url,
    phone,
    google_bewertung,
    anz_bewertungen,
    anz_fotos,
    geo_point_lv95,
    longitude,
    latitude,
    category_ids_de,
    category_ids,
    keyword
;


SELECT
    *
FROM
    google_maps_dev.google_map_mobilität
;

-------------------
-- Normalisierung
-------------------
-- Get plz4 from address if exists
select 
	*
from
	google_maps_dev.google_map_mobilität
where 
	plz4 NOT SIMILAR TO '[0-9]{4}'
;


update google_maps_dev.google_map_mobilität
set 
	plz4 = regexp_replace(address, '([^0-9]*)([0-9]{4,6})([^0-9]*)', '\2')
where 
	plz4 is null
	and 
	plz4 NOT SIMILAR TO '[0-9]{4}'
;


-- Fehlende PLZ4 
select 
	t0.*
	,t1.plz4
from
	google_maps_dev.google_map_mobilität t0
left join
	geo_afo_prod.mv_lay_plz4_aktuell t1
on
	ST_Contains(t1.geo_poly_lv95 , t0.geo_point_lv95)
where 
	t0.plz4 NOT SIMILAR TO '[0-9]{4}'
	or 
	t0.plz4 is null
;


update google_maps_dev.google_map_mobilität t0
set
	plz4 = t1.plz4
from
	geo_afo_prod.mv_lay_plz4_aktuell t1
where  
	t0.plz4 NOT SIMILAR TO '[0-9]{4}'
	or
    t0.plz4 is null
    and
    ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;


-- Get hausenummer from address if exists
select   
    address,
    strasse,
    TRIM(REGEXP_REPLACE(address, '^([^,0-9]+).*', '\1')) AS strasse_neu,
    case  
        when
        TRIM(REGEXP_REPLACE(address, '^.*? ([0-9]+)(,| ).*$', '\1')) = plz4 
        then	
        	null	
        when
        TRIM(REGEXP_REPLACE(address, '^.*? ([0-9]+)(,| ).*$', '\1')) ~ '^[0-9]+$' 
        then
         TRIM(REGEXP_REPLACE(address, '^.*? ([0-9]+)(,| ).*$', '\1'))
        else 
           null 
    end as hausnummer_neu,
    plz4
from 
    google_maps_dev.google_map_mobilität
where  
    hausnummer is null  
    and
   	not address ~ '^[0-9]{4} [A-Za-zäöüÄÖÜß\s-]+$'
;

   

update google_maps_dev.google_map_mobilität
set 
hausnummer = 
        case  
	       when
	           TRIM(REGEXP_REPLACE(address, '^.*? ([0-9]+)(,| ).*$', '\1')) = plz4 
	       then	
	           null	
	       when
	          TRIM(REGEXP_REPLACE(address, '^.*? ([0-9]+)(,| ).*$', '\1')) ~ '^[0-9]+$' 
	       then
	           TRIM(REGEXP_REPLACE(address, '^.*? ([0-9]+)(,| ).*$', '\1'))
	       else 
	           null 
	end,
	strasse = TRIM(REGEXP_REPLACE(address, '^([^,0-9]+).*', '\1'))
where  
    hausnummer is null  
    and
    not address ~ '^[0-9]{4} [A-Za-zäöüÄÖÜß\s-]+$'
;


--Hinzufügen von PLZ6
alter table
	google_maps_dev.google_map_mobilität
add column 
	plz6 text,
add column
	plz text
add column 
	plz6_geo_poly_lv95 geometry
;


update
	google_maps_dev.google_map_mobilität t0
set
	plz = t1.plz, 
	plz6 = t1.plz6,
	plz6_geo_poly_lv95 = t1.geo_poly_lv95 
from
	geo_afo_prod.imp_plz6_geo_neu t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;


-- Hinzufügen von Gemeinde
alter table
	google_maps_dev.google_map_mobilität
add column 
	gemeinde text,
add column
	gmd_nr numeric,
add column
	gmd_geo_poly_lv95 geometry
;

update
	google_maps_dev.google_map_mobilität t0
set
	gemeinde = t1.gemeinde
	,gmd_nr = t1.gmd_nr
	,gmd_geo_poly_lv95 = t1.geo_poly_lv95 
from
	geo_afo_prod.imp_gmd_geo_neu t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;


select 
	title
	,address
	,hausnummer
	,plz4
	,plz
	,plz6
	,geo_point_lv95
	,plz6_geo_poly_lv95
	,gmd_geo_poly_lv95
from 
	google_maps_dev.google_map_mobilität 
where
	plz4 <> SUBSTRING(plz6 , 1, 4)
;

select 
	*
from 
	geo_afo_prod.imp_plz6_geo_neu 
where 
	plz6 = '743000'
;
---------------------------------------------------
--Schritt 8: Erstellung der Vergleichstabelle (T6)
---------------------------------------------------
DROP TABLE IF EXISTS 
    google_maps_dev.google_map_abgleich_mobilität ;

CREATE TABLE 
    google_maps_dev.google_map_abgleich_mobilität  
AS
SELECT 
    cid,
    categories_de,
    categories_en,
    title,
    address,
    strasse,
    hausnummer,
    plz4,
    plz6,
    gemeinde,
    gmd_nr,
    ort,
    phone,
    domain,
    url,
    google_bewertung,
    anz_bewertungen,
    anz_fotos,
    geo_point_lv95,
    longitude,
    latitude
FROM 
    google_maps_dev.google_map_mobilität 
;


select 
	*
from 
	google_maps_dev.google_map_abgleich_mobilität
where
	plz4 <> SUBSTRING(plz6 , 1, 4)
;



-- Test der Eindeutigkeit bevor Abgleich
SELECT 
    cid
    , COUNT(*)
FROM 
    google_maps_dev.google_map_abgleich_mobilität
GROUP BY 
    cid
HAVING  
    COUNT(*) > 1
;

















--///////////////////////////////////// OLD Version////////////////////////////////////////
----------------------------------------------------------------------------------------------------------------
-- Step(6): add PLZ6 and Gemeinde 
--							>> ST_Intersects or ST_Contains (geo_poly_lv95"PLZ6", geo_point_lv95"google_maps")
--        : fill the missing hausenummer 
--							>> ST_Intersects or ST_Contains (geo_poly_lv95"gbd", geo_point_lv95"google_maps")
----------------------------------------------------------------------------------------------------------------


geo_afo_prod.imp_gmd_geo_neu

geo_afo_prod.imp_plz6_geo_neu

geo_afo_prod.mv_lay_gbd_aktuell

geo_afo_prod.mv_qu_gbd_gwr_aktuell








-----------------------------------------------------------
--Step(7): T5 Create table for comparison (Abgleich) Table
-----------------------------------------------------------
drop table if exists 
	google_maps_dev.google_map_mobilität
;

create table 
	google_maps_dev.google_map_mobilität
as
select distinct
	cid
	,category_ids_de
	,category_ids
	,STRING_AGG(DISTINCT category_de, '/ ') as categories_de
	,STRING_AGG(DISTINCT category_en, '/ ') as categories_en
	,title
	,address
	,plz4
	,ort
	,strasse as str_hausnummer
	,REGEXP_REPLACE(strasse, '[0-9]+[A-Za-z]*$', '') as strasse
    ,(REGEXP_MATCH(strasse, '[0-9]+[A-Za-z]*$'))[1] as hausnummer
	--,current_status
	,domain
	,url
	,geo_point_lv95
	,longitude
	,latitude
from
	google_maps_dev.google_map_mobilität_kateg
group by
	cid
	,title
	,plz4
	,ort
	,strasse
	,address
	--,current_status
	,domain
	,url
	,geo_point_lv95
	,longitude
	,latitude
	,category_ids_de
	,category_ids
;







--///////////////////////TEST //////////////////////////

select 
	*
from 
	google_maps_dev.google_map_mobilität
where 
	category_ids = '["auto_body_shop"]'
;




-- Mit Stefan darüber diskutieren 
select 
	*
from 
	google_maps_dev.google_map_results_mobilität
where 
	category_ids = '["auto_body_shop"]'
;


10329199793541200000





select 
	count(distinct title )
	,count(*)
from 
	google_maps_dev.google_map_mobilität
	
group by 
	title
having 
	count(*) > 1
;



select 
	title
	,plz4
from 
	google_maps_dev.google_map_mobilität
group by
	title
	,plz4
having 
	count(*) > 1
;






















--////////////////////////// OLD VERSION ////////////////////////////////

/*
-- create table to unfold the categories and be able to choose the most relevant categories to Restaurants and hotels
drop table if exists 
	google_maps_dev.google_map_mobilität_kateg
;

create table 
	google_maps_dev.google_map_mobilität_kateg
as
select
	*
from(
		select 
			cid,
			split_part(keyword,' ',1) as keyword,
			jsonb_array_elements_text(category_ids_de::jsonb) as category_de,
			jsonb_array_elements_text(category_ids::jsonb) as category_en,
			plz4,
			ort,
			strasse,
			address,
			title,
			current_status,
			domain,
			url,
			geo_point_lv95,
			longitude,
			latitude 
		from 
			google_maps_dev.google_map_items_mobilität
		where 
			jsonb_typeof(category_ids_de::jsonb) = 'array'
			and 
			jsonb_typeof(category_ids::jsonb) = 'array'
	) t
where
category_en in (
				select
					category_en
			    from
			    	google_maps_dev.google_map_category_hierarchy
			    where
			    hauptkategorie_neu = 'Mobilität'
				and 
				kategorie_neu in ('Fahrzeughandel und -werkstatt', 'Fahrzeugvermietung')
)
;

select 
	*
from 
	google_maps_dev.google_map_mobilität_kateg
	
;
--==============================================================
-- final table from google maps filtered with chossen category from Peter 
-- vollständige Tabelle von google mit category filter von Peter
--==============================================================
-->> count 30,680
drop table if exists 
	google_maps_dev.google_map_mobilität
;

create table 
	google_maps_dev.google_map_mobilität
as
select distinct
	cid
	,STRING_AGG(DISTINCT category_de, '/ ') as category_ids_de
	,STRING_AGG(DISTINCT category_en, '/ ') as category_ids_en
	,title
	,address
	,plz4
	,ort
	,strasse as str_hausnummer
	,REGEXP_REPLACE(strasse, '[0-9]+[A-Za-z]*$', '') as strasse
    ,(REGEXP_MATCH(strasse, '[0-9]+[A-Za-z]*$'))[1] as hausnummer
	--,current_status
	,domain
	,url
	,geo_point_lv95
	,longitude
	,latitude
from
	google_maps_dev.google_map_mobilität_kateg
group by
	cid
	,title
	,plz4
	,ort
	,strasse
	,address
	--,current_status
	,domain
	,url
	,geo_point_lv95
	,longitude
	,latitude
;	


-- get rid from plz4 is null
update google_maps_dev.google_map_mobilität
set 
	plz4 = regexp_replace(address, '([^0-9]*)([0-9]{4,6})([^0-9]*)', '\2')
where 
	plz4 is null
	and 
	let(address, 1) in 0,1,2,3
;

select 
	*
from 
	google_maps_dev.google_map_mobilität
where 
	plz4 is null
;



select
	address
	,regexp_replace(address, '([^0-9]*)([0-9]{4,6})([^0-9]*)', '\2') as plz
	,ort
    --,trim(both ' ' from regexp_replace(address, '([0-9]{4,6})\s*(.*)', '\2')) as ort_add
from 
	google_maps_dev.google_map_mobilität
where 
	plz4 is null
;













-- Afo Tabelle zum Abgleich 
drop table if exists google_maps_dev.google_afo_mobilität;
create table google_maps_dev.google_afo_mobilität
as
select 
	*
from 
	geo_afo_prod.mv_lay_poi_aktuell
where 
	poi_typ_id in (
					1311 --Autovermietung
					,516 --Autohändler (non-Food Kategorie ???!)
	)
;


--================ TEST =====================
select
	*
	--cid
	--,count(*)
from
	google_maps_dev.google_map_mobilität
group by
	cid
having 
	count(*) > 1
;

-- Google Tabelle zum Abgleich
select 
	*
from 
	google_maps_dev.google_map_mobilität
where 
	--title like '%Stanco%'
	--address like '%St. Gallerstrasse 126%'
	address like '%Route des Acacias 23%'
	--and
	--title like '%Emil Frey AG%'
;	


--1306 --car-sharing station
--,1309 --Tankstelle
--,1312 --grenzübergang
--,523 --veloladen (non-Food Kategorie ???!)
--,1307 --Veloverleih
--,107 --Autowäsche (Dienestleistung ???!)
--,1308 --Parkhause	
--,1310 --Ladestation Elektroauto	


/*
Occasionshändler
Lieferwagenvermietung
Reifengeschäft
Autohändler
Motorscooterhändler
Motorradhändler
Autovermietung
Auto-Ersatzteilgeschäft
Autowerkstatt
 */


select distinct 
	poi_typ
from
	geo_afo_prod.mv_lay_poi_aktuell
;



























--=============================
-- Tabelle zum Abgleich
-- google_maps_dev.google_map_mobilität
-- google_maps_dev.google_afo_mobilität










--///////////////////////////////// DRAFT /////////////////////////////////////////////

/*
 * 
-- add a new column "current_status" to items table
alter table  
	google_maps_dev.google_map_items_mobilität
add column  
	current_status text
;

-- update items table with "current_status" values
update
	google_maps_dev.google_map_items_mobilität
set
	current_status = work_hours->>'current_status'
;





select distinct
	cid,
	category,
	add_category,
	plz4,
	ort,
	strasse,
	address,
	title,
	domain,
	url,
	geo_point_lv95,
	longitude,
	latitude
from
	google_maps_dev.google_mobilität_kateg 
where 
	category not in (
				'Occasionshändler'
				,'Lieferwagenvermietung'
				,'Reifengeschäft'
				,'Autohändler'
				,'Motorscooterhändler'
				,'Motorradhändler'
				,'Autovermietung'
				,'Auto-Ersatzteilgeschäft'
				,'Autowerkstatt'
	)
	or 
	add_category not in (
				'Occasionshändler'
				,'Lieferwagenvermietung'
				,'Reifengeschäft'
				,'Autohändler'
				,'Motorscooterhändler'
				,'Motorradhändler'
				,'Autovermietung'
				,'Auto-Ersatzteilgeschäft'
				,'Autowerkstatt'
			)
;


	categories in (
						'Occasionshändler','Lieferwagenvermietung','Parkplatz','Reifengeschäft'
						,'Bootswerkstatt','Bushaltestelle','Raststätte','Tankstelle für alternative Kraftstoffe'
						,'Autohändler','Motorscooterhändler','Carsharing-Stellplatz','Autowaschanlage'
						,'Motorradhändler'
						,'Fährterminal'
						,'Haltestelle'
						,'Autovermietung'
						,'Bootsverleih'
						,'Auto-Ersatzteilgeschäft'
						,'Bergbahn'
						,'Tankstelle'
						,'Rastplatz'
						,'Bootshändler'
						,'Ladestation für Elektrofahrzeuge'
						,'Tramhaltestelle'
						,'Bahnhof'
						,'Parkhaus'
						,'Bootslagerung'
						,'Autowerkstatt'
						,'Park & Ride'
						,'Grenzübergangsstelle'
	)		
;



elect 
    g.cid,
    split_part(g.keyword, ' ', 1) as keyword,
    g.category,
    coalesce(ac.add_category, null) as add_category,
    g.plz4,
    g.ort,
    g.strasse,
    g.address,
    g.title,
    g.current_status,
    g.domain,
    g.url,
    g.geo_point_lv95,
    g.longitude,
    g.latitude
from 
    google_maps_dev.google_map_items_mobilität g
left join lateral (
    select
    	jsonb_array_elements_text(g.additional_categories::jsonb) as add_category
    where
    	jsonb_typeof(g.additional_categories::jsonb) = 'array'
) ac on true	
;




select distinct 
    cid,
    category_value as categories,
    plz4,
    ort,
    strasse,
    address,
    title,
    domain,
    url,
    geo_point_lv95,
    longitude,
    latitude
from (
    select 
        cid,
        plz4,
        ort,
        strasse,
        address,
        title,
        domain,
        url,
        geo_point_lv95,
        longitude,
        latitude,
        unnest(string_to_array(
            case 
                when 
                	add_category is null 
                then 
                	category
                else 
                	category || '/' || add_category
            end, '/')
        ) as category_value
    from 
        google_maps_dev.google_mobilität_kateg
) mt
where 
    category_value in (
        select  
            poi_typ_neu
        from  
            geo_afo_prod.meta_poi_google_maps_category
        where  
            hauptkategorie_neu = 'Mobilität'
    );






where
	category_en in (
				select
					category_en
			    from
			    	google_maps_dev.google_map_category_hierarchy
			    where
			    hauptkategorie_neu = 'Mobilität'
				and 
				kategorie_neu in ('Fahrzeughandel und -werkstatt', 'Fahrzeugvermietung')
	)
;


select 
	*
from
	google_maps_dev.google_map_metadata 
;



*/

