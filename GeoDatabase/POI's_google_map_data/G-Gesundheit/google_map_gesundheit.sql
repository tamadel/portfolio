--===========================
-- GESUNDHEIT
--===========================
--stage1
select  distinct 
	hauptkategorie_neu 
	,kategorie_neu 
	,poi_typ_neu 
	,next_run_date 
from  
	geo_afo_prod.meta_poi_google_maps_category
where 
	hauptkategorie_neu = 'Gesundheit'
	and
	--next_run_date = current_date
	kategorie_neu in ('Gesundheitsdienstleistung', 'Hörgeräte-Dienstleistungen', 'Optiker', 'Zahnarzt', 'Ärztehaus/Praxis', 'Spital/Klinik')
;


update geo_afo_prod.meta_poi_google_maps_category
set next_run_date = current_date 
where 
	hauptkategorie_neu = 'Gesundheit'
	and 
	kategorie_neu in ('Gesundheitsdienstleistung', 'Hörgeräte-Dienstleistungen', 'Optiker', 'Zahnarzt', 'Ärztehaus/Praxis', 'Spital/Klinik')
;

select 
	count(*)
from 
	geo_afo_prod.imp_plz6_geo_neu  
;
--stages2
select  
	hauptkategorie_neu 
	,kategorie_neu 
	,poi_typ_neu 
	,next_run_date 
from  
	geo_afo_prod.meta_poi_google_maps_category
where 
	hauptkategorie_neu = 'Gesundheit'
	and
	kategorie_neu = 'Arzt'
	and 
	next_run_date = current_date
;


update geo_afo_prod.meta_poi_google_maps_category
set next_run_date = current_date 
where 
	hauptkategorie_neu = 'Gesundheit'
	and 
	kategorie_neu = 'Arzt'
;

---------------------------------
-- Metadata for all "Gesundheit"
---------------------------------
/*
ALTER TABLE google_maps_dev.google_map_metadata_gesundheit
ADD COLUMN IF NOT EXISTS datetime DATE,
ADD COLUMN IF NOT EXISTS item_type TEXT,                   
ADD COLUMN IF NOT EXISTS n_result NUMERIC,
ADD COLUMN IF NOT EXISTS status_message TEXT,
ADD COLUMN IF NOT EXISTS status_code TEXT
;
*/
select 
	id
	,count(*)
from 
	google_maps_dev.google_map_metadata_gesundheit
group by 
	id 
having 
	count(*) > 1
;




select 
	*
from 
	google_maps_dev.google_map_metadata_gesundheit
where 
	kategorie_neu = 'Arzt'
	and 
	n_r
;



/*
create table google_maps_dev.google_map_metadata_gesundheit_v1
as
select 
	*
from 
	google_maps_dev.google_map_metadata_gesundheit
union all
select 
	*
from 
	google_maps_dev.google_map_metadata_gesundheit_piii
;


select 
	id 
	,count(*)
from
	google_maps_dev.google_map_metadata_gesundheit_v1
group by
	id 
having 
  count(*) > 1
;
*/
------------------------------------------------------------------
--Stage(1) 
--('Alters- und Pflegeheim', 'Apotheke', 'Apotheke & Drogerie')
------------------------------------------------------------------
--Results
select 
	*
from 
	google_maps_dev.google_map_results_gesundheit_pi
;

--Items
select 
	*
from 
	google_maps_dev.google_map_items_gesundheit_pi
;


------------------------------------------------------------------
--Stage(2) its 2 parts pii and pii_v1
--('Arzt') -- n_results are missed
------------------------------------------------------------------
--Results
select 
	*
from 
	google_maps_dev.google_map_results_gesundheit_pii
union all
select 
	*
from 
	google_maps_dev.google_map_results_gesundheit_pii_v1
;

-- Items
select 
	*
from 
	google_maps_dev.google_map_items_gesundheit_pii
;



/*
drop table if exists google_maps_dev.google_map_items_gesundheit_pii_1;
create table google_maps_dev.google_map_items_gesundheit_pii_1
as
select 
	*
from 
	google_maps_dev.google_map_items_gesundheit_pii
UNION all
select 
	*
from 
	google_maps_dev.google_map_items_gesundheit_pii_v1
where 
	cid not in(
				select 
					cid
				from 
					google_maps_dev.google_map_items_gesundheit_pii
	)
;

select 
	cid
	,count(*)
from
	google_maps_dev.google_map_items_gesundheit_pii_1
group by
 cid
 having 
 count(*) > 1
 ;
*/
---------------------------------------------------------------------------------------------------------
--Stage(3) 
--('Gesundheitsdienstleistung', 'Hörgeräte-Dienstleistungen', 'Optiker', 'Zahnarzt', 'Ärztehaus/Praxis', 'Spital/Klinik')
---------------------------------------------------------------------------------------------------------
/*
ALTER TABLE 
    google_maps_dev.google_map_metadata_gesundheit_piii
	ADD COLUMN IF NOT EXISTS datetime DATE,
	ADD COLUMN IF NOT EXISTS item_type TEXT,
	ADD COLUMN IF NOT EXISTS n_result NUMERIC,
	ADD COLUMN IF NOT EXISTS status_message TEXT,
	ADD COLUMN IF NOT EXISTS status_code TEXT
;
--metedata
select 
	*
from 
	google_maps_dev.google_map_metadata_gesundheit_piii
;	

*/

--Results
select 
	*
from 
	google_maps_dev.google_map_results_gesundheit_piii
;


--items
select 
	*
from 
	google_maps_dev.google_map_items_gesundheit_piii
;









