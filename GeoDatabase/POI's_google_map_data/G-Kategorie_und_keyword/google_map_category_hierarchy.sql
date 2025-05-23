--=================================
-- Categorie/POI Hierarchy
--=================================
select 
	*
from 
	google_maps_dev.google_map_category_hierarchy
order by
	keyword,
	n_count desc
; 

--==============================================================
-- Drei Tabelle für die Historische category erstellen
--==============================================================
--Tabelle(1): meta_poi_main_category_new_hist

drop table if exists 
	geo_afo_prod.meta_poi_main_category_new_hist
;

create table 
	geo_afo_prod.meta_poi_main_category_new_hist(
		hauptkategorie_id int generated always as identity,
		hauptkategorie_neu text,
		gueltig_von date default current_date,
    	gueltig_bis date default current_date,
    	created_ts timestamp default current_timestamp,
    	updated_ts timestamp default current_timestamp
	);


insert into geo_afo_prod.meta_poi_main_category_new_hist (hauptkategorie_neu)
select distinct  
	hauptkategorie_neu
from
	google_maps_dev.google_map_category_hierarchy
order by
	hauptkategorie_neu
;

select 
	*
from 
	geo_afo_prod.meta_poi_main_category_new_hist
;



--Tabel(2): meta_poi_category_new_hist
drop table if exists 
	tmp_meta_poi_category_new_hist
;

create temp table 
	tmp_meta_poi_category_new_hist(
		--hauptkategorie_id int,
		hauptkategorie_neu text
		,kategorie_id int generated always as identity
		,kategorie_neu text
		,gueltig_von date default current_date
    	,gueltig_bis date default current_date
    	,created_ts timestamp default current_timestamp
    	,updated_ts timestamp default current_timestamp
	);


insert into tmp_meta_poi_category_new_hist (hauptkategorie_neu, kategorie_neu)
select distinct  
	hauptkategorie_neu
	,kategorie_neu
from
	google_maps_dev.google_map_category_hierarchy
order by
	hauptkategorie_neu
	,kategorie_neu
;


select 
	*
from 
	tmp_meta_poi_category_new_hist
;


--Tabel(3): meta_poi_poi_typ_new_hist
drop table if exists 
	tmp_meta_poi_poi_typ_new_hist
;

create temp table 
	tmp_meta_poi_poi_typ_new_hist(
		hauptkategorie_neu text
		,kategorie_neu text
		,poi_id int generated always as identity
		,poi_typ_neu text
		,gueltig_von date default current_date
    	,gueltig_bis date default current_date
    	,created_ts timestamp default current_timestamp
    	,updated_ts timestamp default current_timestamp
	);


insert into tmp_meta_poi_poi_typ_new_hist (hauptkategorie_neu, kategorie_neu, poi_typ_neu)
select distinct  
	hauptkategorie_neu,
	kategorie_neu,
	poi_typ_neu
from
	google_maps_dev.google_map_category_hierarchy
order by
	hauptkategorie_neu
	,kategorie_neu
	,poi_typ_neu
;


select 
	*
from 
	tmp_meta_poi_poi_typ_new_hist
;

--==================================
--  Final 3 tables with id's
--==================================
-------------
--Table(1)
-------------
select 
	*
from 
	geo_afo_prod.meta_poi_main_category_new_hist
;

-------------
--Table(2)
-------------
drop table if exists 
	 geo_afo_prod.meta_poi_category_new_hist;
	
create table geo_afo_prod.meta_poi_category_new_hist
as
select
	t0.hauptkategorie_id
	,t1.*
from 
	tmp_meta_poi_category_new_hist t1
left join
	geo_afo_prod.meta_poi_main_category_new_hist t0
on
	t0.hauptkategorie_neu = t1.hauptkategorie_neu 
order by 
	t0.hauptkategorie_id,
	t1.kategorie_id
;

select 
	*
from 
	geo_afo_prod.meta_poi_category_new_hist
;

-------------
--Table(3)
------------
select 
	*
from 
	geo_afo_prod.meta_poi_poi_typ_new_hist
;

drop table if exists 
	 geo_afo_prod.meta_poi_typ_new_hist;
	
create table geo_afo_prod.meta_poi_typ_new_hist
as
select 
	t0.hauptkategorie_id
	,t0.hauptkategorie_neu
	,t0.kategorie_id
	,t0.kategorie_neu
	,t1.poi_id as poi_typ_id
	,t1.poi_typ_neu
	,t1.gueltig_von
	,t1.gueltig_bis
	,t1.created_ts
	,t1.updated_ts
from 
	geo_afo_prod.meta_poi_category_new_hist t0
left join
	tmp_meta_poi_poi_typ_new_hist t1
on
	t0.kategorie_neu = t1.kategorie_neu 
order by 
	t0.hauptkategorie_id
	,t0.kategorie_id
	,t1.poi_id
;


--=======================================
--(I)
select 
	*
from 
	geo_afo_prod.meta_poi_main_category_new_hist
;

update geo_afo_prod.meta_poi_main_category_new_hist
set gueltig_bis = '9999-12-31'
;


--(II)
select 
	*
from 
	geo_afo_prod.meta_poi_category_new_hist
;

update geo_afo_prod.meta_poi_category_new_hist
set gueltig_bis = '9999-12-31'
;

--(III)
select 
	*
from 
	geo_afo_prod.meta_poi_typ_new_hist
;
	

update geo_afo_prod.meta_poi_typ_new_hist
set gueltig_bis = '9999-12-31'
;


alter table geo_afo_prod.meta_poi_typ_new_hist
drop column hauptkategorie_id,
drop column hauptkategorie_neu
;

























--/////////////////////////////////// Experiment ////////////////////////////////////
/*
select 
	count(distinct poi_typ)
from
	geo_afo_prod.meta_poi_google_maps_category 
; 

select 
	*
from 
	geo_afo_prod.meta_poi_google_maps_category
;
	

-- T2: neu Hauptkategorie, Kategorie und Poi_ 
create table 

select distinct
	hauptkategorie_neu,
	kategorie_neu,
	poi_typ_neu
from 
	google_maps_dev.google_map_category_hierarchy
order by
	hauptkategorie_neu 
;


-- T1: Poi_typ_alt zu Poi_typ_neu
create table 

select distinct
	 poi_typ_alt
	,poi_typ_neu 
from 
	google_maps_dev.google_map_category_hierarchy
order by
	poi_typ_alt desc
;

select 
	*
from 
	google_maps_dev.google_map_category_hierarchy;



select
	hauptkategorie_neu,
	kategorie_neu,
	poi_typ_neu
from 
	google_maps_dev.google_map_category_hierarchy
order by
	keyword,
	n_count desc
;


select 
	count(distinct keyword)
from
	google_maps_dev.google_map_category_hierarchy
;

*/