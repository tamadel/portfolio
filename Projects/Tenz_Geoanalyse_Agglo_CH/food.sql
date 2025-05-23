--============================
-- Project: POIs aktulisierung
-- Task: Find scrap and identify any pattern
-- Date: 23.01.2025
--============================
DROP table if exists
	google_maps_dev.google_abgleich_food;

create table
	google_maps_dev.google_abgleich_food
as
SELECT 
	*
FROM 
	dblink(
		'geo_database',
		$POSTGRES$
			select
				* 
			from
				google_maps_dev.google_abgleich_food 
		$POSTGRES$
	) AS google_abgleich_food (
			cid text 
			,strasse text 
			,plz4 numeric 
			,ort text 
			,address text 
			,title text 
			,google_strasse text 
			,google_strasse_std text 
			,google_hausnum text 
			,google_plz4 text 
			,google_ort text 
			,gwr_strasse varchar(60) 
			,gwr_hausnum text 
			,gwr_plz4 int4 
			,gwr_ort varchar(100) 
			,plz6 text 
			,gmd_nr numeric 
			,gemeinde text 
			,"domain" text 
			,url text 
			,google_poi_typ text 
			,category_ids text 
			,geo_point_lv95 public.geometry
	)
;


select * from google_maps_dev.google_abgleich_food;
--select * from google_maps_dev.google_map_food_v1



/*
Zigarrengeschäft
Bagelshop
Gemischtwarenladen
Kiosk
Sandwichladen


cigar_shop
bagel_shop
convenience_store
kiosk
sandwich_shop
*/

select 
	*
from 
	google_maps_dev.google_abgleich_food
where 
	gmd_nr = 2701
	and 
	category_ids ilike '%cigar_shop%'
	or
	category_ids ilike '%convenience_store%'
	or
	category_ids ilike '%kiosk%'
	or 
	category_ids ilike '%sandwich_shop%'
	or 
	category_ids ilike '%bagel_shop%'
;

