--===========================
-- Food Geschäft (Cleaning)
-- 01.11.2024
--===========================
-- data ansicht
create table google_maps_dev.google_map_metadata_food_geschäft_v1
as
select 
	*
from 
	google_maps_dev.google_map_metadata_food_geschäft
where  
	cost <> 0
;

select 
	*
from 
	google_maps_dev.google_map_metadata_food_geschäft_v1
;

update 
	google_maps_dev.google_map_metadata_food_geschäft_v1
set datetime = default,
	item_type = default,
	n_result = default, 
	status_message = default,
	status_code = default 
;

-- query for python script (get data) 
select 
	keyword
	,poi_typ
	,id
from 
	google_maps_dev.google_map_metadata_food_geschäft_v1
where  
	cost <> 0
	and 
	poi_typ in (
				select  
					poi_typ_neu 
				from  
					geo_afo_prod.meta_poi_google_maps_category
				where  
					hauptkategorie_neu in ('Food-Geschäft', 'Food Geschäft')
					and 
					kategorie_neu in ('Lebensmittel', 'Lebensmittelgeschäft', 'Lebensmittelhändler')  
	)
;	


select 
	*
from 
	google_maps_dev.google_map_metadata_food_geschäft_v1
where
	kategorie_neu in ('Supermarkt', 'Convenience-Geschäft') 
;	


	
--('Lebensmittel', 'Lebensmittelgeschäft', 'Lebensmittelhändler')	>> Done
--('Supermarkt', 'Convenience-Geschäft') >> 
--('Bäckerei & Konditorei', 'Getränkegeschäft') >> 

SELECT 
	poi_typ_neu 
FROM 
	geo_afo_prod.meta_poi_google_maps_category
WHERE 
	hauptkategorie_neu in ('Food-Geschäft', 'Food Geschäft')
	and 
	kategorie_neu in ('Lebensmittel', 'Lebensmittelgeschäft', 'Lebensmittelhändler')
;
	

--========================================================================================================
--------------------
-- (1): metedata PI
--------------------
select 
	*
from 
	google_maps_dev.google_map_metadata_food_geschäft_v1
where
	kategorie_neu in ('Lebensmittel', 'Lebensmittelgeschäft', 'Lebensmittelhändler')
	and 
	n_result is null
;




---------------------
-- Results PI
---------------------
select 
	*
from 
	google_maps_dev.google_map_results_food_geschäft_pi
;




----------------------
-- Items PI 
----------------------
select 
	*
from 
	google_maps_dev.google_map_items_food_geschäft_pi
;

--===========================================================================
--------------------
-- (2): metedata PII
--------------------
select 
	*
from 
	google_maps_dev.google_map_metadata_food_geschäft_v1
where
	kategorie_neu in ('Supermarkt', 'Convenience-Geschäft') 
	and 
	n_result is null
;





---------------------
-- Results PII
---------------------
select 
	*
from 
	google_maps_dev.google_map_results_food_geschäft_pii
;




----------------------
-- Items PII 
----------------------
select 
	*
from 
	google_maps_dev.google_map_items_food_geschäft_pii
;


--===========================================================================
--------------------
-- (3): metedata PIII
--------------------
select 
	*
from 
	google_maps_dev.google_map_metadata_food_geschäft_v1
where
	kategorie_neu in ('Bäckerei & Konditorei', 'Getränkegeschäft')
	and 
	n_result is null
;




---------------------
-- Results PIII
---------------------
select 
	*
from 
	google_maps_dev.google_map_results_food_geschäft_piii
;




----------------------
-- Items PIII 
----------------------
select 
	*
from 
	google_maps_dev.google_map_items_food_geschäft_piii
;












	