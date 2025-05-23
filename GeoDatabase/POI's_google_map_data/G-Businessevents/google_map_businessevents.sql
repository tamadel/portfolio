--===========================
-- Hauptkategorie = 'Businessevents'
-- 04.11.2024
--==========================
select 
	distinct hauptkategorie_neu 
	,kategorie_neu
	,poi_typ_neu 
from
	geo_afo_prod.meta_poi_categories_business_data_aktuell
where
	hauptkategorie_neu = 'Businessevents'
;
	
	
--	'Businessevents', 'Einkaufszentrum', 'Landwirtschaft', 'Non-Food Geschäft','Parkanlagen', 'Religiöse Einrichtungen'
	

select 
	*
from 
	geo_afo_prod.meta_poi_google_maps_category
where 
	hauptkategorie_neu = 'Businessevents'
;



update geo_afo_prod.meta_poi_google_maps_category
set 
	next_run_date = current_date 
where 
	hauptkategorie_neu = 'Businessevents'
;	

	
--===============
-- Metadata
--===============
select 
	*
from 
	google_maps_dev.google_map_metadata_businessevents
where 
	n_result is null
;


alter table  
	google_maps_dev.google_map_metadata_businessevents
ADD COLUMN IF NOT EXISTS datetime DATE,
ADD COLUMN IF NOT EXISTS item_type TEXT,
ADD COLUMN IF NOT EXISTS n_result NUMERIC,
ADD COLUMN IF NOT EXISTS status_message TEXT,
ADD COLUMN IF NOT EXISTS status_code TEXT
;



select 
	*
from 
	google_maps_dev.google_map_results_businessevents
;




select 
	*
from 
	google_maps_dev.google_map_items_businessevents
;







