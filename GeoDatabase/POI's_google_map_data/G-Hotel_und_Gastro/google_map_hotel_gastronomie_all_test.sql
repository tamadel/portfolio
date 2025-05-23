--=====================================
-- Hauptkategory = Hotel & Gastronomie
-- Poi_typ = alle
-- 21.11.2024
--=====================================
update 
	geo_afo_prod.meta_poi_google_maps_category
set next_run_date  = current_date  
where 
	hauptkategorie_neu  = 'Einkaufszentrum'
;


select
	*
from 
	geo_afo_prod.meta_poi_google_maps_category
where
	hauptkategorie_neu  = 'Einkaufszentrum'
	and 
	kategorie_neu in (
					'Bar/Pub',
			        'Café',
			        'Nachtclub'        
	)
;

"'Restaurant'",
"'Takeaway'"
"'Caterer'",
"'Catering'",
"'Food Zustellservice'",
"'Food-Court'",
"'Hotel'",


'Caterer',
'Catering',
'Food Zustellservice',
'Food-Court',

'Hotel'
'Restaurant'
'Takeaway'


--============OLD PYTHON SCRIPT=======================

create table google_maps_dev.google_map_metadata_hotel_gastronomie_test
as
select 
	*
from
	google_maps_dev.google_map_metadata_hotel_gastronomie
;


update
	google_maps_dev.google_map_metadata_hotel_gastronomie_test
set datetime = default,
	item_type = default,
	n_result = default,
	status_message = default,
	status_code= default
;

--================================
-- Hotel/Gastro Part(1)
-- 'Bar/Pub', 'Café', 'Nachtclub'
--================================
------------------
--(1) METADATA PI
------------------
select 
	*
from
	google_maps_dev.google_map_metadata_hotel_gastronomie_test
where
	n_result is not null
	and
	kategorie_neu in (
					'Bar/Pub',
			        'Café',
			        'Nachtclub'        
	)
;

----------------
--(2) RESULTS PI
----------------
select 
	*
from 
	google_maps_dev.google_map_results_hotel_gastronomie_v1
;


----------------
--(3) ITEMS PI
----------------

--//////////////////////////// New Version (Mit philipp)//////////////////////////////////////
/*
drop table if exists google_maps_dev.google_map_results_hotel_gastronomie_test;
drop table if exists google_maps_dev.google_map_items_hotel_gastronomie_test;
drop table if exists geo_afo_tmp.tmp_results;
select * from geo_afo_tmp.tmp_results;  
update
	google_maps_dev.google_map_metadata_hotel_gastronomie_test
set datetime = default,
	item_type = default,
	n_result = default,
	status_message = default,
	status_code= default
;
*/


select
	* 
from 
	google_maps_dev.google_map_metadata_hotel_gastronomie_test
where 
	n_result is null
;


create table google_maps_dev.google_map_metadata_hotel_gastronomie_test1
as
select
	* 
from 
	google_maps_dev.google_map_metadata_hotel_gastronomie_test
where 
	n_result is null
;

select
	* 
from 
	google_maps_dev.google_map_metadata_hotel_gastronomie_test1
where 
	n_result is not null
;



select
	* 
from 
	google_maps_dev.google_map_results_hotel_gastronomie_test
;


select
	* 
from 
	google_maps_dev.google_map_items_hotel_gastronomie_test
;


select 
	cid
	,count(*)
from
	google_maps_dev.google_map_items_hotel_gastronomie_test
group by
	cid 
having 
	count(*) > 1
;








--//////////////////////////////////////////////////////////////////////////////////////
                                              









--///////////////////////////////////////////////////////////////////////////////////////////////////////////
--------------------
--(1) Metadata
--------------------
--drop table if exists google_maps_dev.google_map_metadata_hotel_gastronomie;
select 
	*
from
	google_maps_dev.google_map_metadata_hotel_gastronomie
where 
	n_result is not null
;



alter table  
	google_maps_dev.google_map_metadata_hotel_gastronomie
add column if not exists datetime DATE,
add column if not exists item_type TEXT,
add column if not exists n_result NUMERIC,
add column if not exists status_message TEXT,
add column if not exists status_code TEXT
;


create table google_maps_dev.google_map_metadata_hotel_gastronomie_v1 
as
select 
	*
from
	google_maps_dev.google_map_metadata_hotel_gastronomie
;




select 
	*
from 
	google_maps_dev.google_map_metadata_hotel_gastronomie_v1
;

--------------------
--(1) Results
--------------------
create temp table tmp_hotel_gastro_pi
as
select 
	count(*)
from
	google_maps_dev.google_map_results_hotel_gastronomie
where  
	poi_typ in (
				'Lounge'
				,'Bar'
				,'Brauhaus'
				,'Pub'
				,'Biergarten'
				,'Kindercafé'
				,'Frühstückslokal'
				,'Espressobar'
				,'Café'
				,'Coffeeshop'
	)
;


/*
update
	google_maps_dev.google_map_metadata_hotel_gastronomie_v1
set datetime = default,
	item_type = default,
	n_result = default,
	status_message = default,
	status_code= default
;

drop table if exists google_maps_dev.google_map_results_hotel_gastronomie_v1;
drop table if exists google_maps_dev.google_map_items_hotel_gastronomie;
drop table if exists google_maps_dev.google_map_hotel_gastronomie;
*/


--(1) METADATA
select 
	*
from 
	google_maps_dev.google_map_metadata_hotel_gastronomie_v1
where 
	n_result is not null
;

--================================================================


--(2) RESULTS
select 
	*
from 
	google_maps_dev.google_map_results_hotel_gastronomie_v2_test
where 
	cid in (
			select 
				cid
			from 
				google_maps_dev.google_map_results_hotel_gastronomie_v3_test
	)
;


select 
	*
from 
	google_maps_dev.google_map_results_hotel_gastronomie_v3_test
where 
	cid = '13533188480348067917'
;










--================================================================================
--(3) ITEMS
select 
	*
from 
	google_maps_dev.google_map_items_hotel_gastronomie
;

select 
	cid
	,count(*)
from
	google_maps_dev.google_map_items_hotel_gastronomie
group by
	cid 
having 
	count(*) > 1
;


--(4) CATEGORY
select 
	*
from 
	google_maps_dev.google_map_hotel_gastronomie
;
	



select 
	*
from 
	google_maps_dev.google_map_items_hotel_gastronomie
where 
	cid not in (
				select 
					cid
				from
					google_maps_dev.google_map_hotel_gastronomie
	)
;











                        















































 
	google_maps_dev.google_map_metadata_hotel_gastronomie
;

select 
	distinct kategorie_neu
	,poi_typ 
from
	google_maps_dev_test.google_map_metadata_hotel_gastronomie_depth_test
;
--==========================================================================================
