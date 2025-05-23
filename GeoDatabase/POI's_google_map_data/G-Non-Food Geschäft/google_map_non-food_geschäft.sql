--============================
--HauptKategorie: Non-Food Geschäft
--Datum: 08.11.2024
--============================

select 
	*
from 
	geo_afo_prod.meta_poi_google_maps_category
where 
	hauptkategorie_neu = 'Non-Food Geschäft'
	and 
	kategorie_neu in ('Bücher & Musik', 'Multimedia Geschäft', 'Computergeschäft')
;

	--kategorie_neu in ('Bücher & Musik', 'Multimedia Geschäft', 'Computergeschäft' ) >> 14
	--kategorie_neu in ('Beauty Geschäft', 'Schmuck- und Uhrengeschäft', 'Modegeschäft', 'Kleidergeschäft') >> 10
	--kategorie_neu in ('Sportgeschäft', 'Spielwarengeschäft', 'Geschenkhandel', 'Outlet') >> 10
	--kategorie_neu in ('Baumarkt', 'Blumengeschäft', 'Tierhandlung') >> 8
	--kategorie_neu in ('Haushaltsgeschäft', 'Haushaltswarengeschäft', 'Möbelgeschäft', 'Warenhaus', 'Bürobedarf') >> 10

select distinct
	hauptkategorie_neu 
	,kategorie_neu 
	,poi_typ_neu 
from
	geo_afo_prod.meta_poi_categories_business_data_aktuell as t2
where
	hauptkategorie_neu = 'Non-Food Geschäft'
	
	
select
	*
from
	geo_afo_prod.meta_poi_categories_business_data_aktuell	
;
	





	
alter table  
	google_maps_dev.google_map_metadata_non_food_geschäft
add column if not exists datetime DATE,
add column if not exists item_type TEXT,
add column if not exists n_result NUMERIC,
add column if not exists status_message TEXT,
add column if not exists status_code TEXT
;

	


select 
	poi_typ
	,kategorie_neu
	,count(*)
from
	google_maps_dev.google_map_metadata_non_food_geschäft
group by
	poi_typ,
	kategorie_neu

	
	
SELECT  
    poi_typ_neu
    ,hauptkategorie_neu
    ,kategorie_neu
    ,next_run_date
FROM 
    geo_afo_prod.meta_poi_google_maps_category
WHERE 
    hauptkategorie_neu = 'Non-Food Geschäft'
    and 
    poi_typ_neu not in(
    					select 
							poi_typ
						from
							google_maps_dev.google_map_metadata_non_food_geschäft
    );  
  
update geo_afo_prod.meta_poi_google_maps_category
set next_run_date = current_date 
WHERE 
    hauptkategorie_neu = 'Non-Food Geschäft'
    and 
    poi_typ_neu not in(
    					select 
							poi_typ
						from
							google_maps_dev.google_map_metadata_non_food_geschäft
    );  

	
	
	
--(1) METADATA
select 
	*
from 
	google_maps_dev.google_map_metadata_non_food_geschäft
where 
	n_result is not null
;	
	
	

--(2) RESULTS

select 
	*
from
	google_maps_dev.google_map_results_non_food_geschäft
;




--(3) ITEMS
select 
	*
from 
	google_maps_dev.google_map_items_non_food_geschäft
;




--check duplicates in item table 
select 
	cid
	,count(*)
from
	google_maps_dev.google_map_items_non_food_geschäft
group by
	cid 
having 
	count(*) > 1
;




--(4) CATEGORY
--drop table if exists google_maps_dev.google_map_non_food_geschäft; 
select 
	*
from 
	google_maps_dev.google_map_non_food_geschäft
;
	

-- the categories that dropped in the filiter 
 
where 
	cid not in (
				select 
					cid
				from
					google_maps_dev.google_map_non_food_geschäft
	)
;

	
	