--=================================
--Project: 
-- Date: 04.03.2025
-- Update 
--==================================
-- geo_afo_prod.lay_poi_geo_2024_hist nach serverless
-- geo_afo_prod.meta_company_hist  
-- geo_afo_prod.meta_company_group_hist
-- Tabelle auf Serverless migrieren
drop table if exists
	geo_afo_prod.meta_company_group_hist;
create table
	geo_afo_prod.meta_company_group_hist   
as
select  
	*
from  
	dblink(
		'geo_database',
		$POSTGRES$
			select
				* 
			from
				geo_afo_prod.meta_company_group_hist
		$POSTGRES$
	) as meta_company_group_hist (
			id numeric
			,company_group_id numeric
			,company_group text 
			,gueltig_von date 
			,gueltig_bis date 
			,created_ts timestamp 
			,updated_ts timestamp 
	)
;


--=======================================================================================================
-- With helep of "geo_afo_prod.lay_poi_geo_2024_hist" create the table 
select 
	*
from
	geo_afo_prod.lay_poi_geo_2024_hist
where 
	'6002002' = any(poi_typ_id_list)
;


--bem: there is differents poi_ids with same name and address but different company_unit
--------------------------------------
-- create table from poi_geo_2024_hist
--------------------------------------
drop table if exists tmp_versicherung;
create temp table tmp_versicherung
as
select 
	poi_id 
	,cid 
	,bezeichnung_lang
	,bezeichnung_kurz
	,adresse 
	,plz4 
	,ort 
	,url
	,geo_point_lv95 
	,company_group_id
	,company_id
	,company_unit
from
	geo_afo_prod.lay_poi_geo_2024_hist
where 
	'6002002' = any(poi_typ_id_list)
;

select 
	poi_id
	,count(*)
from 
	tmp_versicherung
group by 
	poi_id
having 
	count(*) > 1
;
	

---------------
-- add company
--------------- 
drop table if exists tmp_versicherung_comp;
create temp table 
		tmp_versicherung_comp
as
select 
	t1.*
	,t2.company
from 
	tmp_versicherung t1
left join
	geo_afo_prod.meta_company_hist t2
on 
t1.company_id = t2.company_id
and 
t1.company_group_id = t2.company_group_id 
;

select 
	poi_id
	,count(*)
from 
	tmp_versicherung_comp
group by 
	poi_id
having 
	count(*) > 1
;


-----------------------
-- add company_group 
-----------------------
drop table if exists tmp_versicherung_comp_group;
create temp table 
		tmp_versicherung_comp_group
as
select 
	t1.*
	,t2.company_group
from 
	tmp_versicherung_comp t1
left join
	 geo_afo_prod.meta_company_group_hist t2
on 
t1.company_group_id = t2.company_group_id 
;

select 
	poi_id
	,count(*)
from 
	tmp_versicherung_comp_group
group by 
	poi_id
having 
	count(*) > 1
;

select * from tmp_versicherung_comp_group;

--------------------------------------
-- create first table 
--------------------------------------- 
drop table if exists allianz.tmp_versicherung_tot;
create table 
	allianz.tmp_versicherung_tot
as
select 
	poi_id
	,cid
	,company
	,adresse
	,plz4
	,ort
	,bezeichnung_lang as bezeichnung
	,company_group
	,company_unit
	,url
	,geo_point_lv95
from 
	tmp_versicherung_comp_group
;

select * from allianz.tmp_versicherung_tot where lower(adresse) like '%industriestrasse 14%';

select 
	poi_id
	,count(*)
from 
	allianz.tmp_versicherung_tot
group by 
	poi_id
having 
	count(*) > 1
;

----------------------------------------------------------------------
-- Data von Google that not exist in "allianz.tmp_versicherung_tot"
----------------------------------------------------------------------

select 
	*
from 
	google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot
where
	google_poi_typ <> '[Versicherungsagentur]'
	--or 
	--poi_typ_id = 105
	and
	(
	lower(company) like '%allianz suisse%'
	or
	lower(company) like '%axa winterthur%'
	or 
	lower(company) like '%bâloise%'
	or 
	lower(company) like '%mobiliar%'
	or 
	lower(company) like '%generali%'
	or
	lower(company) like '%helvetia versicherungen%'
	or 
	lower(company) like '%swiss life%'
	or
	lower(company) like '%vaudoise versicherungen%'
	or  
	lower(company) like '%zurich%'
	)
	--and
	--quelle = 'GOOGLE'
	--and 
	--dubletten_nr is null
	and 
	poi_id not in (
					select 
						--poi_id::text
						cid 
					from 
						allianz.tmp_versicherung_tot
			)
;


-- Note: there are 23 poi_id from afo that have match with google data and they are not exists in the table from 
-- "geo_afo_prod.lay_poi_geo_2024_hist"


select 
	*
from 
	allianz.tmp_versicherung_tot
where 
	cid not in (
				select 
					poi_id
				from 
					google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot
				where
					( 
					google_poi_typ = '[Versicherungsagentur]'
					--or 
					--poi_typ_id = 105
					)
					and
					(
					lower(company) like '%allianz suisse%'
					or
					lower(company) like '%axa winterthur%'
					or 
					lower(company) like '%bâloise%'
					or 
					lower(company) like '%die mobiliar%'
					or 
					lower(company) like '%generali%'
					or
					lower(company) like '%helvetia versicherungen%'
					or 
					lower(company) like '%swiss life%'
					or
					lower(company) like '%vaudoise versicherungen%'
					or  
					lower(company) like '%zurich%'
					)
	);















----------------------------------------------------------------------------------------------------------------------


--table from allianz 2024
select * from allianz.allianz_konkurrenten;



---------------------------------------------------
-- add the number of employes from allianz 2024
---------------------------------------------------
-- 1st approach
select 
	t1.company
	,t2.company as original_company
	,t1.adresse
	,t2.original_strasse
	,t1.plz4
	,t2.original_plz 
	,t1.ort
	,t2.original_ort 
	,t1.bezeichnung
	,t2.bezeichnung as original_bezeichnung
	,t2."zugehrigkeit" 
	,t2."zugehrigkeit_id" 
	,t2.x_wgs 
	,t2.y_wgs 
	,t2.ma_total 
	,t2.aussendienst 
	,t2.innendienst 
from 
	allianz.tmp_versicherung_tot t1
join 
	allianz.allianz_konkurrenten t2
on 
	ST_X(t1.geo_point_lv95) = ST_X(ST_Transform(ST_SetSRID(ST_MakePoint(t2.x_wgs, t2.y_wgs), 4326), 2056)) 
	and  
	ST_Y(t1.geo_point_lv95) = ST_Y(ST_Transform(ST_SetSRID(ST_MakePoint(t2.x_wgs, t2.y_wgs), 4326), 2056))
;
	
	

-- 2nd approch
select  
	t1.poi_id, 
    t1.company,
    t2.company as original_company,
    t1.adresse,
    t2.original_strasse,
    t1.plz4,
    t2.original_plz,
    t1.ort,
    t2.original_ort,
    t1.bezeichnung,
    t2.bezeichnung as original_bezeichnung,
    t1.url,
    --t2.zugehörigkeit, 
    --t2.zugehörigkeit_id, 
    t1.geo_point_lv95, 
    t2.x_wgs, 
    t2.y_wgs, 
    t2.ma_total, 
    t2.aussendienst, 
    t2.innendienst 
from  
    allianz.tmp_versicherung_tot t1
left join  
    allianz.allianz_konkurrenten t2
on  
    ST_DWithin(
        t1.geo_point_lv95, 
        ST_Transform(ST_SetSRID(ST_MakePoint(t2.x_wgs, t2.y_wgs), 4326), 2056),
        10  -- Distance threshold in meters
    )
    and 
    t1.company = t2.company
;
    

-- continue with the 2nd approach
drop table if exists tmp_allianz_konkurrenten_2025;
create temp table tmp_allianz_konkurrenten_2025
as
select distinct on(t1.poi_id)
	t1.poi_id 
	,t1.cid
	,t1.company 
	,t1.adresse 
	,t1.plz4 
	,t1.ort 
	,t1.bezeichnung 
	,t1.company_group 
	,t1.company_unit 
	,t1.url 
	,t2."zugehrigkeit"
	,t1.geo_point_lv95
	,ST_Transform(ST_SetSRID(ST_MakePoint(t2.x_wgs, t2.y_wgs), 4326), 2056) as geo_point_allianz
	,ST_Distance(
        ST_Transform(t1.geo_point_lv95, 2056),  
        ST_Transform(ST_SetSRID(ST_MakePoint(t2.x_wgs, t2.y_wgs), 4326), 2056)  
    ) as distance_meters
	,ST_X(ST_Transform(t1.geo_point_lv95, 4326)) as x_wgs
	,ST_Y(ST_Transform(t1.geo_point_lv95, 4326)) as y_wgs
    ,t2.ma_total
    ,t2.aussendienst
    ,t2.innendienst 
from  
    allianz.tmp_versicherung_tot t1
left join  
    allianz.allianz_konkurrenten t2
on  
    ST_DWithin(
        t1.geo_point_lv95, 
        ST_Transform(ST_SetSRID(ST_MakePoint(t2.x_wgs, t2.y_wgs), 4326), 2056),
        50  -- Distance threshold in meters
    )
    and 
    t1.company = t2.company
    and 
    t1.company_unit = t2.company_unit
where 
	t1.company <> 'Allianz Suisse'
order by  
    t1.poi_id 
   	,distance_meters ASC
;



--------------------
-- check duplicates 
--------------------
select * from tmp_allianz_konkurrenten_2025;
select distinct poi_id from tmp_allianz_konkurrenten_2025;

select 
	poi_id
	,count(*)
from 
	tmp_allianz_konkurrenten_2025
group by 
	poi_id
having 
	count(*) > 1
;



select 
	*
from 
	tmp_allianz_konkurrenten_2025
where  
	poi_id in (
				select 
					poi_id
				from 
					tmp_allianz_konkurrenten_2025
				group by 
					poi_id
				having 
					count(*) > 1
	)
	
;


-- duplicates in Allianz table 2024 because of the company unit
select 
	*
from 	
	allianz.allianz_konkurrenten
where 
	original_plz in (
						select 
							plz4
						from 
							tmp_allianz_konkurrenten_2025
						where  
							poi_id in (
										select 
											poi_id
										from 
											tmp_allianz_konkurrenten_2025
										group by 
											poi_id
										having 
											count(*) > 1
	))
	and	
	original_ort in (
						select 
							ort
						from 
							tmp_allianz_konkurrenten_2025
						where  
							poi_id in (
										select 
											poi_id
										from 
											tmp_allianz_konkurrenten_2025
										group by 
											poi_id
										having 
											count(*) > 1
	))
;

--------------------------------
-- create table with distinct poi_id
--------------------------------


drop table if exists allianz.allianz_konkurrenten_2025;
create table allianz.allianz_konkurrenten_2025
as
select distinct
	poi_id 
	,cid
	,company 
	,adresse 
  	,plz4 
	,ort 
	,bezeichnung 
	,company_group 
	,company_unit 
	,url 
	--,geo_point_lv95
	--,geo_point_allianz
	--,distance_meters
	,x_wgs
	,y_wgs
    ,ma_total
    ,aussendienst 
    ,innendienst
from 
	tmp_allianz_konkurrenten_2025
;


select distinct company from allianz.allianz_konkurrenten_2025;

--AXA Winterthur  			ok			>> 340 	API: https://www.axa.ch/servlets/external/exportagentdata.de.json
--Bâloise  					ok			>> 94 	API: https://www.baloise.ch/de/privatkunden/kontakt-services/berater-standorte/main/content/0.json
--Die Mobiliar 				ok			>> 250 	API: https://www.mobiliar.ch/api/v1/map/agencies
--Generali 					ok			>> 57 	API: https://www.generali.ch/content/dam/generali/common/data/agentur/agencies.xml
--Helvetia Versicherungen  	ok			>> 115 	API: https://www.helvetia.com/ch/web/de/_jcr_content.agencies-map-all.json
--Swiss Life  				ok			>> 61 	API: https://api.eventually.cloud/api/v1/beraterseite/agencies 
--Vaudoise Versicherungen  	ok			>> 114 	API: https://www.vaudoise.ch/widget-api/agences?lang=de
--Zurich (137 Agentur 28 Help point)   	>> 138 	API: https://www.zurich.ch/de/api/finder/v1/locations/Agency,Helppoint?locationRootOverride= 
-- 1168 Agenutre 





select 
	poi_id
	,count(*)
from 
	allianz.allianz_konkurrenten_2025
group by
	poi_id
having
	count(*) > 1
;






select * from allianz.allianz_konkurrenten_2025 where cid is null; 	--1089
select * from allianz.allianz_konkurrenten;			--1057



--===============================
-- inspect Peter's Feedback 
--===============================
-- cid = '9928875062173662917'  Zurich, Generalagentur Howald & Scheidegger AG  Rötistrasse 6, 4500 Solothurn
-- poi_id = 48990 				Zurich 											Rötistrasse 6, 4501 solothurn

























--/////////////////////////////////////
-- DRAFT
--////////////////////////////////////
/*
-- Data 
select * from google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot;

-----------------
-- Versicherung
-----------------
-- google data 3335
-- No Match with category_ids '[insurance_agency]' 2189
select 
	*
from 
	google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot
where 
	google_poi_typ = '[Versicherungsagentur]'
	and 
	quelle = 'GOOGLE' 
	and 
	dubletten_nr is not null 
	and 
	category_ids = '[insurance_agency]'
	and 
	lower(company) like '%axa winterthur%'
;

select 
	*
from
	geo_afo_prod.lay_poi_geo_2024_hist
where 
	'6002002' = any(poi_typ_id_list)
;
	
select 
	*
from 
	geo_afo_prod.meta_poi_typ_2024_hist
;
	
-- AfO data  2645
-- No Match 590 from this there are 316 company = 'AXA Winterthur'
select 
	*
from 
	google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot
where 
	quelle = 'AFO'
	and 
	poi_typ_id = 105
	and
	dubletten_nr is null
;



-- match with url
select distinct 
	t1.url 		as google_url 
	,t1.company as google_company
	,t2.company as afo_company
	,t1.adresse as google_adresse
	,t2.adresse as afo_adresse
	,t2.url 	as afo_url
	,t1.poi_id  as google_cid
	,t2.poi_id 	as afo_id
from(
		select 
			*
		from 
			google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot
		where 
			google_poi_typ = '[Versicherungsagentur]'
			and
		 	dubletten_nr is null
		 	and
			quelle = 'GOOGLE'
			and 
			poi_typ_id <> 105
) t1
join (
		select 
			*
		from 
			google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot
		where 
		 	dubletten_nr is null
		 	and
			quelle = 'AFO' --'GOOGLE'
			and 
			poi_typ_id = 105
) t2
on 
	t1.url ilike '%' || t2.url || '%' 
	--and 
	--t1.adresse ilike '%' || t2.adresse || '%'
; 


select 
	*
from (
		select distinct 
			poi_id 
			,company 
			,adresse 
		  	,plz4 
			,ort 
			,bezeichnung 
			,company_group 
			,company_unit 
			,url 
			--,geo_point_lv95
			--,geo_point_allianz
			--,distance_meters
			,x_wgs
			,y_wgs
		    ,ma_total
		    ,aussendienst 
		    ,innendienst
		    ,row_number() over(
		    					partition by 
		    						poi_id
		    						,company_unit
		    					order by 
		    						ma_total desc
		    						--,aussendienst desc
		    						--,innendienst desc
		    			) as rn
		from
			tmp_allianz_konkurrenten_2025
	)
where 
	rn = 1
;

*/
