--============================================
-- Hauptkategorie: 'Parkanlagen'
-- 07.11.2024
--============================================
select 
	*
from 
	geo_afo_prod.meta_poi_google_maps_category
where 
	hauptkategorie_neu = 'Parkanlagen'
;


update geo_afo_prod.meta_poi_google_maps_category
set 
	next_run_date = current_date 
where 
	hauptkategorie_neu = 'Parkanlagen'
;


--Metedata
alter table  
	google_maps_dev.google_map_metadata_parkanlagen
add column if not exists datetime DATE,
add column if not exists item_type text,
add column if not exists n_result numeric,
add column if not exists status_message text,
add column if not exists status_code text
;


select 
	*
from 
	google_maps_dev.google_map_metadata_parkanlagen
;



--Results 
select 
	*
from 
	google_maps_dev.google_map_results_parkanlagen
;


--Items
select 
	*
from 
	google_maps_dev.google_map_items_parkanlagen
;



select 
	count(*)
from 
	geo_afo_prod.mv_lay_plz4_aktuell 



