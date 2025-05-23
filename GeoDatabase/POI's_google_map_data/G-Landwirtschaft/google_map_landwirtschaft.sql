--=====================================
-- Hauptkategory = Landwirtschaft
-- Poi_typ = alle
-- 27.11.2024
--=====================================
update 
	geo_afo_prod.meta_poi_google_maps_category
set next_run_date  = current_date  
where 
	hauptkategorie_neu  = 'Landwirtschaft'
;


select
	*
from 
	geo_afo_prod.meta_poi_google_maps_category
where
	hauptkategorie_neu  = 'Landwirtschaft'


	
select
	* 
from 
	geo_afo_prod.meta_poi_categories_business_data_aktuell
where 
	--category_id in ( 'wine_wholesaler' , 'vineyard',  'wine_cellar' , 'winery')
	hauptkategorie_neu = 'Landwirtschaft'
;	
	



--(1) METADATA

select 
	*
from
	google_maps_dev.google_map_metadata_landwirtschaft
;

alter table  
	google_maps_dev.google_map_metadata_landwirtschaft
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
	google_maps_dev.google_map_results_landwirtschaft
;



--(3) ITEMS
select 
	*
from 
	google_maps_dev.google_map_items_landwirtschaft
;

--check duplicates in item table 
select 
	cid
	,count(*)
from
	google_maps_dev.google_map_items_landwirtschaft
group by
	cid 
having 
	count(*) > 1
;


--(4) CATEGORY
--drop table if exists google_maps_dev.google_map_landwirtschaft; 
select 
	*
from 
	google_maps_dev.google_map_landwirtschaft
;
	

-- the categories that dropped in the filiter 
-- we need to have a closer look at this categories 
select 
	*
from 
	google_maps_dev.google_map_items_landwirtschaft
where 
	cid not in (
				select 
					cid
				from
					google_maps_dev.google_map_landwirtschaft
	)
;






