--==================================================================================
-- DFSEO: Hauptkategorie >> "Mobilität_p2"
--        kategory >> 'Fahrzeugparking', 'Fahrzeugpflege', 'Ladestation Elektrofahrzeuge'
--                    'Raststätte', 'Tankstelle', 'Grenzübergang', 'Car-sharing'
--        poi_typ >> alle 11 poi_typ
--Datum: 02.10.2024
--==================================================================================
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
	kategorie_neu in (
						'Fahrzeugparking'
						,'Fahrzeugpflege'
						,'Ladestation Elektrofahrzeuge'
						,'Raststätte'
						,'Tankstelle'
						,'Grenzübergang'
						,'Car-sharing' 
	)
;



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
	kategorie_neu in (
						'Fahrzeugparking'
						,'Fahrzeugpflege'
						,'Ladestation Elektrofahrzeuge'
						,'Raststätte'
						,'Tankstelle'
						,'Grenzübergang'
						,'Car-sharing' 
	)
;





-------------------------------------------------
--Step(3): T2 google_map_results (Results) "GET"
-------------------------------------------------
select 
	*
from 
	google_maps_dev.google_map_results_mobilität 
where 
    trim(poi_typ) in (
						'Parkplatz'
						,'Raststätte'
						,'Tankstelle für alternative Kraftstoffe'
						,'Carsharing-Stellplatz'
						,'Autowaschanlage'
						,'Tankstelle'
						,'Rastplatz'
						,'Ladestation für Elektrofahrzeuge'
						,'Parkhaus'
						,'Park & Ride'
						,'Grenzübergangsstelle'
    )
;


alter table google_maps_dev.google_map_results_mobilität
add column poi_typ text
;

update google_maps_dev.google_map_results_mobilität
set 
	poi_typ = TRIM(poi_typ)
;



-------------------------------------------------------------------------------------
--Step(4): T3 google_map_items (Items) unique cid and filitered by "rank_abslout"
-- created by Py script
-------------------------------------------------------------------------------------
-- n_count 75'333
select 
	*
from 
	google_maps_dev.google_map_items_mobilität_pii
;

select 
	cid
	,count(*)
from
	google_maps_dev.google_map_items_mobilität_pii
group by
	cid
having 
	count(*) > 1
;

-------------------------------------------
--Step(5): T4 Filter by defined Category
-------------------------------------------
-- update items table with "category_ids_de"
-- add a new column "category_ids_de"
alter table  
	google_maps_dev.google_map_items_mobilität_pii
add column  
	category_ids_de jsonb
;

-- update items table with "category_ids_de" values (apply this for pi and pii)
update
	google_maps_dev.google_map_items_mobilität_pii
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
	google_maps_dev.google_map_mobilität_pii_kateg
;

create table 
	google_maps_dev.google_map_mobilität_pii_kateg
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
			geo_point_lv95,
			longitude,
			latitude 
		from 
			google_maps_dev.google_map_items_mobilität_pii
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
					kategorie_neu in (
										'Fahrzeugparking'
										,'Fahrzeugpflege'
										,'Ladestation Elektrofahrzeuge'
										,'Raststätte'
										,'Tankstelle'
										,'Grenzübergang'
										,'Car-sharing' 
					)
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



















-----------------------------------------------------------
--Step(7): T5 Create table for comparison (Abgleich) Table
-----------------------------------------------------------
drop table if exists 
	google_maps_dev.google_map_mobilität_pii
;

create table 
	google_maps_dev.google_map_mobilität_pii
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
	google_maps_dev.google_map_mobilität_pii_kateg
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





-- get rid from plz4 is null
select 
	*
from 
	google_maps_dev.google_map_mobilität_pii
where 
	title like '%Moor Dach GmbH%'
;




update google_maps_dev.google_map_mobilität_pii
set 
	plz4 = regexp_replace(address, '([^0-9]*)([0-9]{4,6})([^0-9]*)', '\2')
where 
	plz4 is null
;





