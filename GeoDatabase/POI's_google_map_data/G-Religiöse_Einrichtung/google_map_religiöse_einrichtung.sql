--============================
--HauptKategorie: Religiöse Einrichtungen
--Datum: 20.11.2024
--============================
select 
	*
from 
	geo_afo_prod.meta_poi_google_maps_category
where 
	hauptkategorie_neu = 'Religiöse Einrichtungen'
	and 
	kategorie_neu in ('Kirchen/Tempel/Moscheen')
;

update geo_afo_prod.meta_poi_google_maps_category
set poi_typ_neu = 'Synagogen'
where 
	hauptkategorie_neu = 'Religiöse Einrichtungen'
	and 
	kategorie_neu in ('Kirchen/Tempel/Moscheen')
	and 
	poi_typ_neu = ''
;


update geo_afo_prod.meta_poi_google_maps_category
set next_run_date = current_date
where 
	hauptkategorie_neu = 'Religiöse Einrichtungen'
	and 
	kategorie_neu in ('Kirchen/Tempel/Moscheen')
;



select * from geo_afo_prod.meta_poi_categories_business_data_aktuell where category_id like '%hous%'; 

-------------------------
--Metadata
------------------------
select 
	*
from 
	google_maps_dev.google_map_metadata_religiöse_einrichtung
where 
	n_result is not null
;


alter table  
	google_maps_dev.google_map_metadata_religiöse_einrichtung
add column if not exists datetime DATE,
add column if not exists item_type TEXT,
add column if not exists n_result NUMERIC,
add column if not exists status_message TEXT,
add column if not exists status_code TEXT
;







--------------------
--Results
--------------------
select 
	*
from 
	google_maps_dev.google_map_results_religiöse_einrichtung
;



--------------------
--Items
--------------------
select 
	*
from 
	google_maps_dev.google_map_items_religiöse_einrichtung
;









