--=====================================
-- Hauptkategory = Bildung
-- Poi_typ = alle
-- 27.11.2024
--=====================================
update 
	geo_afo_prod.meta_poi_google_maps_category
set next_run_date  = current_date  
where 
	hauptkategorie_neu  = 'Bildung'
;


select
	*
from 
	geo_afo_prod.meta_poi_google_maps_category
where
	hauptkategorie_neu  = 'Bildung'

/*
 'Kindertagesstätte'
, 'Vorschule'
, 'Sekundarstufe 1'
, 'Zweitausbildung nicht-tertiäre Stufe'
, 'Primarstufe'
, 'Sekundarstufe 2'
, 'Tertiäre Stufe'
*/



	
select
	* 
from 
	geo_afo_prod.meta_poi_categories_business_data_aktuell
where 
	hauptkategorie_neu = 'Bildung'
;		
	
	

--(1) METADATA

select 
	*
from
	google_maps_dev.google_map_metadata_bildung
where 
	n_result is not null
;

alter table  
	google_maps_dev.google_map_metadata_bildung
add column if not exists datetime DATE,
add column if not exists item_type TEXT,
add column if not exists n_result NUMERIC,
add column if not exists status_message TEXT,
add column if not exists status_code TEXT
;




--(2) RESULTS

select 
	*
from
	google_maps_dev.google_map_results_bildung
;



--(3) ITEMS
select 
	*
from 
	google_maps_dev.google_map_items_bildung
;


--check duplicates in item table 
select 
	cid
	,count(*)
from
	google_maps_dev.google_map_items_bildung
group by
	cid 
having 
	count(*) > 1
;


--(4) CATEGORY
select 
	*
from 
	google_maps_dev.google_map_bildung
;
	

-- the categories that dropped in the filiter 
select 
	*
from 
	google_maps_dev.google_map_items_bildung
where 
	cid not in (
				select 
					cid
				from
					google_maps_dev.google_map_bildung
	)
;





