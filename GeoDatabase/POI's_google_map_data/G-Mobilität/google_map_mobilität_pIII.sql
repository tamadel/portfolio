--==================================================================================
-- DFSEO: Hauptkategorie >> "Mobilität_p3"
--        kategory >> 'Bootshandel und -werkstatt'
--        poi_typ >> alle 11 poi_typ
--Datum: 03.10.2024
--==================================================================================

--First: select the poi category to use them as keyword in DataForSEO 
----------------------------------------------
--Step(1): Selected Category for Mobilität_P2
----------------------------------------------
select 
	*
from 
	geo_afo_prod.meta_poi_google_maps_category
where 
	hauptkategorie_neu = 'Mobilität'
	and 
	kategorie_neu = 'Bootshandel und -werkstatt' 
;
	
'Bootswerkstatt'
'Bootsverleih'
'Bootshändler'
'Bootslagerung'

/*
update
	geo_afo_prod.meta_poi_google_maps_category
set
	last_run_ts = default 
where
    hauptkategorie_neu = 'Mobilität'
	and 
	kategorie_neu not in ('Fahrzeughandel und -werkstatt', 'Fahrzeugvermietung') 
;
*/


--second: create the first table whiches the metadata table 
-------------------------------------------------
--Step(2): T1 google_map_metadata (Metadata) "POST"
-------------------------------------------------
select 
	*
from 
	google_maps_dev.google_map_metadata_mobilität
where 
	hauptkategorie_neu = 'Mobilität'
	and 
	kategorie_neu = 'Bootshandel und -werkstatt'
;



--third: create the second table which is the retrieaved data from DataForSEO it's a row data 
-------------------------------------------------
--Step(3): T2 google_map_results (Results) "GET"
-------------------------------------------------
select 
	*
from 
	google_maps_dev.google_map_results_mobilität 
where
	poi_typ in (
				'Bootswerkstatt'
				,'Bootsverleih'
				,'Bootshändler'
				,'Bootslagerung'
    )
;


-- adding a new column called "poi_typ" where we split the poi type from the keyword to filter the table with poi_type in case the table contains lots of different poi types 
alter table google_maps_dev.google_map_results_mobilität
add column poi_typ text
;

update google_maps_dev.google_map_results_mobilität
set 
	poi_typ = TRIM(SUBSTRING(keyword FROM '^[^0-9]+'))
where 
	poi_typ is null 
;


--fourth: create the third table wich is called items in this table we filter "rank_absoult" attribute and select the one has the min(rank_absolute) and like this we get rid of duplicated cid and we have at the end a table with unique "Cid"
-------------------------------------------------------------------------------------
--Step(4): T3 google_map_items (Items) unique cid and filitered by "rank_abslout"
-- created by Py script
-------------------------------------------------------------------------------------

select 
	*
from 
	google_maps_dev.google_map_items_mobilität_piii
where 
	cid = '7264126841512861687'
;

select 
	cid
	,count(*)
from
	google_maps_dev.google_map_items_mobilität_piii
group by
	cid
having 
	count(*) > 1
;



--fifth: we update items table with a new column "category_ids_de" where we concatnate "category" with "additional_categories" in the same order as "category_id's"  then "creating the fourth table where we unfold the "category_ids" attribute because it is a jsonb file and
-- after we unfold it we filter the whole table by pre-defind category in this table "google_maps_dev.google_map_category_hierarchy"         
-------------------------------------------
--Step(5): T4 Filter by defined Category
-------------------------------------------
-- update items table with "category_ids_de"
-- add a new column "category_ids_de"
alter table  
	google_maps_dev.google_map_items_mobilität_piii
add column  
	category_ids_de jsonb
;



-- update items table with "category_ids_de" values (apply this for pi and pii)
update
	google_maps_dev.google_map_items_mobilität_piii
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





-- create table to unfold the categories and be able to choose the most relevant categories to Restaurants and hotels
drop table if exists 
	google_maps_dev.google_map_mobilität_piii_kateg
;

create table 
	google_maps_dev.google_map_mobilität_piii_kateg
as
select
	*
from(
		select 
			cid,
			split_part(keyword,' ',1) as keyword,
			category,
			additional_categories,
			category_ids_de,
			category_ids,
			jsonb_array_elements_text(category_ids_de::jsonb) as category_de,
			jsonb_array_elements_text(category_ids::jsonb) as category_en,
			plz4,
			ort,
			strasse,
			address,
			title,
			work_hours->>'current_status' as current_status,
			domain,
			url,
			rating->>'value' AS google_bewertung,
	        rating->>'votes_count' AS anz_bewertungen,
	        total_photos AS anz_fotos,
			geo_point_lv95,
			longitude,
			latitude 
		from 
			google_maps_dev.google_map_items_mobilität_piii
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
						kategorie_neu = 'Bootshandel und -werkstatt'
	)
;




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



--sixth: creating the fivth table where we use it to compare the content with our AFO Poi's in our database in this table we need to: 
-- clean address attribute by fill the missing "plz4" and split the address in to "street_name", "building number", "plz4", "gemeinde", "ort" and to do so we need to intersect the polygone of the whole table with the polygone of our "plz6_layer"    
-- add 2 new columns one for "plz6" and one for "Gemeinde"
-- make sure that all the category are the relevant categories für the poi 
-- get rid of duplicated poi in google map data 
-- adding a predefine form for a new atrribute called "Relevance" and it has to be scores for a good quilty data 











-----------------------------------------------------------
--Step(7): T5 Create table for comparison (Abgleich) Table
-----------------------------------------------------------
drop table if exists 
	google_maps_dev.google_map_mobilität_piii
;

create table 
	google_maps_dev.google_map_mobilität_piii
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
	google_maps_dev.google_map_mobilität_piii_kateg
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


--TEST
DROP TABLE IF EXISTS 
    google_maps_dev.google_map_<category_name>_comparison;

CREATE TABLE 
    google_maps_dev.google_map_<category_name>_comparison AS
SELECT 
    cid,
    keyword,
    category_ids_de,
    category_ids,
    STRING_AGG(DISTINCT category_de, '/ ') AS categories_de,
    STRING_AGG(DISTINCT category_en, '/ ') AS categories_en,
    title,
    address,
    strasse AS str_hausnummer,
    REGEXP_REPLACE(strasse, '[0-9]+[A-Za-z]*$', '') as strasse,
    (REGEXP_MATCH(strasse, '[0-9]+[A-Za-z]*$'))[1] as hausnummer,
    plz4,
    --plz6,
    --gemeinde,
    --gmd_nr,
    ort,
    domain,
    url,
    --google_bewertung,
    --anz_bewertungen,
    --anz_fotos,
    geo_point_lv95,
    longitude,
    latitude
FROM 
    google_maps_dev.google_map_mobilität_piii_kateg
GROUP BY
    cid,
    title,
    plz4,
    ort,
    strasse,
    address,
    domain,
    url,
    --google_bewertung,
    --anz_bewertungen,
    --anz_fotos,
    geo_point_lv95,
    longitude,
    latitude,
    category_ids_de,
    category_ids,
    keyword
    --gemeinde,
    --gmd_nr,
    --plz6
   ;











-- 7264126841512861687 interesting case to test 
select 
	*
from 
	google_maps_dev.google_map_mobilität_piii
where 
	cid = '7264126841512861687'
;












--///////////////////// DRAFT ///////////////////////////////////////////

/*
 * -- update items table with "category_ids_de" values
update
	google_maps_dev.google_map_items_mobilität_piii
set
	category_ids_de = CASE
				        -- If `additional_categories` is NULL, insert only `category` into a JSONB array
				        when 
				        	additional_categories is null or additional_categories::text = 'null'
				        then
				        	to_jsonb(array[category])  -- Convert category to JSONB array
				        -- If `additional_categories` is a JSONB array, append `category` to the array
				        when
				        	jsonb_typeof(additional_categories::jsonb) = 'array'
				        then
				        	jsonb_insert(additional_categories::jsonb, '{0}', to_jsonb(category), true)  -- Append category to the JSONB array
				        -- If `additional_categories` is not an array, convert it and `category` to a JSONB array
				        else
				        	to_jsonb(array[category] || additional_categories::text)  -- Combine category and non-array additional_categories
				    end
;
 */




