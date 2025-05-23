--==============================
-- Vesricherung Daten durch API
--
--==============================
-- display data sources 
select * from allianz.die_mobiliar;
select * from allianz.axa_winterthur;
select * from allianz.baloise;
select * from allianz.swisslife_agencies;
select * from allianz.vaudoise_versicherungen;
select * from allianz.zurich_agencies;
select * from allianz.helvetia_versicherungen;
select * from allianz.generali_agencies;




-- 1) Die Mobiliar  250
drop table if exists tmp_mobiliar;
create temp table tmp_mobiliar
as
select 
	nid::text  												as id
	--,title  												as title
	,coalesce('Die Mobiliar' || ' ' || "name", null)  		as company
	,case 
		when ismain = 'FALSCH' 	then 'Agentur/Büro'
		when ismain = 'WAHR' 	then 'Generalagentur'
		else null
	end 													as company_unit	
	,address 												as adresse
	,postalcode  											as plz4
	,locality 												as ort
	,phonenumber::text 										as telefon
	,email 													as email 
	,url 
	,"coordinates/lat" 										as lat 
	,"coordinates/lng" 										as lng
	,ST_Transform(ST_SetSRID(ST_MakePoint("coordinates/lng", "coordinates/lat"), 4326), 2056) 	    as geo_point_lv95
	,ST_Transform(ST_SetSRID(ST_MakePoint("coordinates/lng", "coordinates/lat"), 2056), 4326) 		as geo_point_wgs84
	,ST_Transform(ST_SetSRID(ST_MakePoint("coordinates/lng", "coordinates/lat"), 4326), 21781) 	as geo_point_lv03
from 
	allianz.die_mobiliar
;

-- 2) AXA Winterthur 336
/*
drop table if exists tmp_axa_winterthur;
create table geo_afo_tmp.tmp_axa_winterthur
as
select distinct 
	agency_type::text 	as id
	,coalesce('AXA Winterthur'|| ' ' || agency_name, null)   as company
	,split_part(agency_name)  		as company_unit
	,agency_street 		as adresse
	,agency_zip 		as plz4
	,agency_city 		as ort
	,agency_phone::text as telefon
	,agency_mail  		as email
	,agency_link  		as url
	,agency_latitude   	as lat
	,agency_longitude 	as lng
	,ST_Transform(ST_SetSRID(ST_MakePoint(agency_longitude, agency_latitude), 4326), 2056) 	    as geo_point_lv95
	,ST_Transform(ST_SetSRID(ST_MakePoint(agency_longitude, agency_latitude), 2056), 4326) 		as geo_point_wgs84
	,ST_Transform(ST_SetSRID(ST_MakePoint(agency_longitude, agency_latitude), 4326), 21781) 	as geo_point_lv03
from 
	allianz.axa_winterthur
;
*/
select * from geo_afo_tmp.tmp_axa_winterthur;

drop table if exists tmp_axa_winterthur;
create temp table tmp_axa_winterthur
as
select 
	id
	,coalesce('AXA Winterthur'|| ' ' || company_unit, null)   	as company
	,split_part(company_unit,' ', 1)  							as company_unit
	,adresse
	,plz4
	,ort
	,telefon
	,email
	,url
	,lat
	,lng
	,geo_point_lv95
	,geo_point_wgs84
	,geo_point_lv03
from
	geo_afo_tmp.tmp_axa_winterthur
;


-- 3) Baloise 95
drop table if exists tmp_baloise;
create temp table tmp_baloise
as
select  
	agencyid::text 			as id
	,title 					as company
	,null 					as company_unit
	,"address.street" 		as adresse
	,"address.zipcode"  	as plz4
	,"address.location"  	as ort
	,phone 					as telefon
	,mail  					as email
	,"link.href" 			as url
	,"coords.lat" 			as lat 
	,"coords.lng" 			as lng
	,ST_Transform(ST_SetSRID(ST_MakePoint("coords.lng", "coords.lat"), 4326), 2056) 	    as geo_point_lv95
	,ST_Transform(ST_SetSRID(ST_MakePoint("coords.lng", "coords.lat"), 2056), 4326) 		as geo_point_wgs84
	,ST_Transform(ST_SetSRID(ST_MakePoint("coords.lng", "coords.lat"), 4326), 21781) 	    as geo_point_lv03
from 
	allianz.baloise
;

-- 4) Generali 54
drop table if exists tmp_generali;
create temp table tmp_generali
as
select distinct
	row_number() over (order by agencytitle1) 		as id
	,agencytitle1 									as company
	, null 											as company_unit
	,coalesce(street || ' ' || housenumber, null) 	as adresse
	,zipcode 										as plz4
	,town 											as ort
	,phone::text 											as telefon
	,email 											as email
	,website 										as url
	,latitude 										as lat
	,longitude 										as lng
	,ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), 2056) 	    as geo_point_lv95
	,ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 2056), 4326) 		as geo_point_wgs84
	,ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), 21781) 	    as geo_point_lv03
from (
    select distinct
    	agencytitle1
    	, street
    	, housenumber
    	, zipcode
    	, town
    	, phone
    	, email
    	, website
    	, latitude
    	, longitude
    from
    	allianz.generali_agencies
) as unique_agencies
;


-- 5) Helvetia 116
drop table if exists tmp_helvetia;
create temp table tmp_helvetia
as
select 
	id::text 													as id
	,coalesce("name" || ' '|| split_part(agencyname, ' ', -1)) 	as company
	--,agencyname
	,split_part(agencyname, ' ', 1) 							as company_unit
	,address 													as adresse
	,zip 														as plz4
	,city 														as ort
	,phone 														as telefon
	,email 														as email
	,url 														as url
	,"location.lat" 											as lat
	,"location.lng" 											as lng
	,ST_Transform(ST_SetSRID(ST_MakePoint("location.lng", "location.lat"), 4326), 2056) 	    as geo_point_lv95
	,ST_Transform(ST_SetSRID(ST_MakePoint("location.lng", "location.lat"), 2056),4326) 			as geo_point_wgs84
	,ST_Transform(ST_SetSRID(ST_MakePoint("location.lng", "location.lat"), 4326), 21781) 	    as geo_point_lv03
from 
	allianz.helvetia_versicherungen
;

-- 6) Swisslife  61
drop table if exists tmp_swisslife;
create temp table tmp_swisslife
as
select distinct
	id::text 											as id 
	,"name"												as company
	,case 
		when "type" like 'AGENCY' 			then 'Generalagentur'
		when "type" like 'BRANCH_OFFICE' 	then 'Geschäftsstelle'
		else null
	end													as company_unit
	,address_line1 										as adresse
	,address_postalcode 								as plz4
	,address_city 										as ort
	,phonenumber 										as telefon
	, null 												as email
	,url 												as url 
	,coordinates_latitude 								as lat
	,coordinates_longitude 								as lng
	,ST_Transform(ST_SetSRID(ST_MakePoint(coordinates_longitude, coordinates_latitude), 4326), 2056) 	    as geo_point_lv95
	,ST_Transform(ST_SetSRID(ST_MakePoint(coordinates_longitude, coordinates_latitude), 2056),4326) 		as geo_point_wgs84
	,ST_Transform(ST_SetSRID(ST_MakePoint(coordinates_longitude, coordinates_latitude), 4326), 21781) 	    as geo_point_lv03
from 
	allianz.swisslife_agencies
;


-- 7) Vaudoise 115
drop table if exists tmp_vaudoise;
create temp table tmp_vaudoise
as
select 
	id::text 									as id 
	,coalesce('Vaudoise' || ' ' || title, null)  	as company
	,split_part(title, ' ', 1) 					as company_unit
	,address 									as adresse
	,postalcode 								as plz4 
	,locality 									as ort
	,phone 										as telefon
	,email 										as email
	,null 										as url 
	,latitude 									as lat
	,longitude 									as lng
	,ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), 2056) 	    as geo_point_lv95
	,ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 2056),4326) 		as geo_point_wgs84
	,ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), 21781) 	    as geo_point_lv03
from
	allianz.vaudoise_versicherungen
; 

-- 8) Zurich 138
drop table if exists tmp_zurich;
create temp table tmp_zurich
as
select 
	id::text 												as id
	,"name" 												as company
	,split_part(trim(split_part("name", ',', -1)), ' ', 1)	as company_unit
	,street 												as adresse
	,zip    												as plz4 
	,place  												as ort
	,phone  												as telefon
	,email  												as email
	,agencylink  											as url 
	,latitude    											as lat
	,longitude   											as lng
	,ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), 2056) 	    as geo_point_lv95
	,ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 2056),4326) 		as geo_point_wgs84
	,ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), 21781) 	    as geo_point_lv03
from 
	allianz.zurich_agencies
;


--------------------
-- verification
--------------------
select 
	company_unit
	,adresse
	,count(*)
from
	--tmp_mobiliar
	--tmp_zurich
	--tmp_vaudoise
	--tmp_swisslife
	--tmp_helvetia   -- using here company_unit
	--tmp_generali
	--tmp_baloise
							tmp_axa_winterthur
group by 
	company_unit
	,adresse
having 
	count(*) > 1
;

--select * from tmp_axa_winterthur;
--delete from tmp_swisslife where id = 'gs-liestal' and telefon = '+41 61 227 88 33';
	
	 

---------------------------------------------
-- create table for all Versicherung Comapny
---------------------------------------------
drop table if exists allianz.allianz_konkurrenten_api;
create table 
		allianz.allianz_konkurrenten_api
as
select * from tmp_mobiliar  --
union all 
select * from tmp_zurich
union all 
select * from tmp_vaudoise
union all 
select * from tmp_swisslife
union all 
select * from tmp_helvetia
union all 
select * from tmp_generali
union all 
select * from tmp_baloise
union all 
select * from tmp_axa_winterthur
;

select * from allianz.allianz_konkurrenten_api; --1158

select            
	company
	,adresse
	,plz4
	,ort
	,count(*)
from
	allianz.allianz_konkurrenten_api
group by 
	company
	,adresse
	,plz4 
	,ort
having 
	count(*) > 1
;


---------------------------------
-- Add anzahl MA 
---------------------------------
drop table if exists tmp_allianz_konkurrenten_2025;
create temp table tmp_allianz_konkurrenten_2025
as
select 
	t1.company
	,t2.company
	,t2.bezeichnung
	,t1.adresse
	,t2.original_strasse
	,t2."zugehrigkeit"
	
	,ST_Distance(
        ST_Transform(t1.geo_point_lv95, 2056),  
        ST_Transform(ST_SetSRID(ST_MakePoint(t2.x_wgs, t2.y_wgs), 4326), 2056)  
    ) as distance_meters
    ,t1.geo_point_lv95
    ,ST_Transform(ST_SetSRID(ST_MakePoint(t2.x_wgs, t2.y_wgs), 4326), 2056) as geo_point_allianz
    ,t2.ma_total
    ,t2.aussendienst
    ,t2.innendienst 
from  
    allianz.allianz_konkurrenten_api t1
left join  
    allianz.allianz_konkurrenten t2
on  
    ST_DWithin(
        t1.geo_point_lv95, 
        ST_Transform(ST_SetSRID(ST_MakePoint(t2.x_wgs, t2.y_wgs), 4326), 2056),
        50  -- Distance threshold in meters
    )
    and 
    lower(t1.company) ~ lower(t2.company)
    and 
    t1.adresse = t2.original_strasse
;

-- match with allianz_konkurrenten 2024 in >> 644






--////////////////////////////////////////////////////////////////////////////////////////////////////
---------------------------------------------------
-- vergleich mit google 
---------------------------------------------------
select * from google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot;

select 
	*
from 
	google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot
where
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
	and
	quelle = 'GOOGLE'
	--and
	--google_poi_typ <> '[Versicherungsagentur]'
	--or 
	--poi_typ_id = 105
	--and 
	--dubletten_nr is null
	
	

----------------------
-- Die Mobiliar
----------------------
-- 222 google von 250 Mobiliar: Match 169

-- api Mobiliar data 250
select * from tmp_mobiliar;

-- google Mobiliar data 222
create temp table tmp_mobiliar_google
as
select 
	* 
from 
	google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot
where 
	(
	lower(company) like '%mobiliar%'
	or 
	lower(url) like '%mobiliar%'
	)
	and 
	quelle = 'GOOGLE'
;


-- match data 169
drop table if exists tmp_mobiliar_match;
create temp table tmp_mobiliar_match
as
select
	t1.id 				as api_id
	,t2.poi_id 			as go_id
	,t1.company  		as api_company
	,t2.company 		as go_company
	,t1.company_unit
	,t1.adresse			as api_adresse
	,t2.adresse 		as go_adresse
	,t1.plz4 			as api_plz4
	,t2.plz4 			as go_plz4
	,t1.ort 			as api_ort
	,t2.ort 			as go_ort
	,t1.telefon
	,t1.email
	,t1.url 			as api_url
	,t2.url 			as go_url
	,t2.domain 
	,t1.lat
	,t1.lng
	,t1.geo_point_lv95 	as api_geo_point_lv95
	,t2.geo_point_lv95 	as go_geo_point_lv95 	
	,ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) as distance
	,row_number() over(partition by poi_id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95)) as rn
from 
	 tmp_mobiliar t1
left join 
	 tmp_mobiliar_google  t2
on
	ST_DWithin(t1.geo_point_lv95, t2.geo_point_lv95, 100)
;


select * from tmp_mobiliar_match where rn = 1;

-- duplicates 
--google: 9488677529382990948 and 10894407165163755056
--api: 6843 6796 6776 6885
select 
	--go_id
	api_id
	,count(*)
from
	tmp_mobiliar_match
--where 
 	--rn = 1
group by 
	--go_id
	api_id
having 
	count(*) > 1
;
	

-- 85 in mobiliar not in google match
select 
	*
from 
	tmp_mobiliar
where 
	id not in (select api_id from tmp_mobiliar_match)
;


-- 55 in google not in mobiliar
select 
	*
from 
	tmp_mobiliar_google
where 
	poi_id not in (select go_id from tmp_mobiliar_match)
; 
	


----------------------
-- tmp_zurich
----------------------
-- 233 google und 138 zurich: Match 148 

-- api zurich data 138
select * from tmp_zurich;

-- google zurich data 233
create temp table tmp_zurich_google
as
select 
	* 
from 
	google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot
where 
	(
	lower(company) like 'zurich%'
	or 
	lower(url) like '%.zurich.ch%'
	)
	and 
	(
	google_poi_typ not like '%Bank%'
	or 
	google_poi_typ not like '%Bankautomat%'
	)
	and 
	quelle = 'GOOGLE'
;


-- match data 148  
drop table if exists tmp_zurich_match;
create temp table tmp_zurich_match
as
select 
	t1.id 				as api_id
	,t2.poi_id 			as go_id
	,t1.company  		as api_company
	,t2.company 		as go_company
	,t1.company_unit
	,t1.adresse			as api_adresse
	,t2.adresse 		as go_adresse
	,t1.plz4 			as api_plz4
	,t2.plz4 			as go_plz4
	,t1.ort 			as api_ort
	,t2.ort 			as go_ort
	,t1.telefon
	,t1.email
	,t2.domain 
	,t1.lat
	,t1.lng
	,t1.geo_point_lv95 	as api_geo_point_lv95
	,t2.geo_point_lv95 	as go_geo_point_lv95 	
	,ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) as distance
	,row_number() over(partition by poi_id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95)) as rn
from 
	 tmp_zurich t1
left join 
	 tmp_zurich_google  t2
on 
	ST_DWithin(t1.geo_point_lv95, t2.geo_point_lv95, 100)
;

select * from tmp_zurich_match where rn = 1;

-- duplicates 
-- google:	11
-- api:	  	30 
select 
	api_id
	--go_id
	,count(*)
from
	tmp_zurich_match
--where 
 	--rn = 1
group by 
	api_id
	--go_id
having 
	count(*) > 1
;




-- 26 in zurich not in google match
select 
	*
from 
	tmp_zurich
where 
	id not in (select api_id from tmp_zurich_match)
;


-- 97 in google not in zurich
select 
	*
from 
	tmp_zurich_google
where 
	poi_id not in (select go_id from tmp_zurich_match)
; 
	



----------------------
-- tmp_vaudoise
----------------------
-- 127 google und 115 vaudoise: Match 102 

-- api vaudoise data 115
select * from tmp_vaudoise;

-- google vaudoise data 127
create temp table tmp_vaudoise_google
as
select 
	* 
from 
	google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot
where 
	(
	lower(company) like '%vaudoise%'
	or 
	lower(url) like '%vaudoise%'
	)
	and 
	(
	google_poi_typ not like '%Bank%'
	or 
	google_poi_typ not like '%Bankautomat%'
	)
	and 
	quelle = 'GOOGLE'
;


-- match data  102
drop table if exists tmp_vaudoise_match;
create temp table tmp_vaudoise_match
as
select 
	t1.id 				as api_id
	,t2.poi_id 			as go_id
	,t1.company  		as api_company
	,t2.company 		as go_company
	,t1.company_unit
	,t1.adresse			as api_adresse
	,t2.adresse 		as go_adresse
	,t1.plz4 			as api_plz4
	,t2.plz4 			as go_plz4
	,t1.ort 			as api_ort
	,t2.ort 			as go_ort
	,t1.telefon
	,t1.email
	,t1.url 			as api_url
	,t2.url 			as go_url
	,t2.domain 
	,t1.lat
	,t1.lng
	,t1.geo_point_lv95 	as api_geo_point_lv95
	,t2.geo_point_lv95 	as go_geo_point_lv95 	
	,ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) as distance
	,row_number() over(partition by poi_id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95)) as rn
from 
	 tmp_vaudoise t1
left join 
	 tmp_vaudoise_google  t2
on 
	ST_DWithin(t1.geo_point_lv95, t2.geo_point_lv95, 100)
;


select * from tmp_vaudoise_match where rn = 1;

-- duplicates 
-- Google: 1985407966996723260  and 10411623974544922990
-- api:    33_6
select 
	api_id
	--go_id
	,count(*)
from
	tmp_vaudoise_match
--where 
 	--rn = 1
group by
	api_id
	--go_id
having 
	count(*) > 1
;


-- match address
select 
	*
from 
	tmp_vaudoise_match
where
	lower(api_adresse) <> lower(go_adresse) 
;
-- 2709285141323089361 Vaudoise Agentur Meilen Seestrasse 941 (Eingang via, Alte Landstrasse 154, 8706 Meilen



-- 14 in vaudoise not in google match
select 
	*
from 
	tmp_vaudoise
where 
	id not in (select api_id from tmp_vaudoise_match)
;


-- 27 in google not in vaudoise
select 
	*
from 
	tmp_vaudoise_google
where 
	poi_id not in (select go_id from tmp_vaudoise_match)
; 




----------------------
-- tmp_swisslife
----------------------
--  google und 61 swisslife: Match 

-- api swisslife data 61
select * from tmp_swisslife;

-- google swisslife data 89
create temp table tmp_swisslife_google
as
select 
	* 
from 
	google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot
where 
	(
	lower(company) like 'swisslife%'
	or 
	lower(company) like 'swiss life%'
	)
	and 
--	(
--	google_poi_typ not like '%Bank%'
--	or 
--	google_poi_typ not like '%Bankautomat%'
--	)
--	and 
	quelle = 'GOOGLE'
;


-- match data 60 
drop table if exists tmp_swisslife_match;
create temp table tmp_swisslife_match
as
select 
	t1.id 				as api_id
	,t2.poi_id 			as go_id
	,t1.company  		as api_company
	,t2.company 		as go_company
	,t1.company_unit
	,t1.adresse			as api_adresse
	,t2.adresse 		as go_adresse
	,t1.plz4 			as api_plz4
	,t2.plz4 			as go_plz4
	,t1.ort 			as api_ort
	,t1.telefon
	,t1.email
	,t1.url 			as api_url
	,t2.url 			as go_url
	,t2.domain 
	,t1.lat
	,t1.lng
	,t1.geo_point_lv95 	as api_geo_point_lv95
	,t2.geo_point_lv95 	as go_geo_point_lv95 	
	,ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) as distance
	,row_number() over(partition by poi_id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95)) as rn
from 
	 tmp_swisslife t1
left join 
	 tmp_swisslife_google  t2
on 
	ST_DWithin(t1.geo_point_lv95, t2.geo_point_lv95, 100)
;




select * from tmp_swisslife_match where rn = 1;

-- duplicates 
-- 15570231327195679521 and 16431848166065878070 and 886452969831043045
-- 80A , gs-liestal,  gs-zuerich-binz,  81C
select 
	api_id
	--go_id
	,count(*)
from
	tmp_swisslife_match
--where 
 	--rn = 1
group by 
	api_id
	--go_id
having 
	count(*) > 1
;


-- match address
select 
	*
from 
	tmp_swisslife_match
where
	lower(api_adresse) <> lower(go_adresse) 
;



-- 1 in swisslife not in google match
select 
	*
from 
	tmp_swisslife
where 
	id not in (select api_id from tmp_swisslife_match)
;


-- 29 in google not in swisslife
select 
	*
from 
	tmp_swisslife_google
where 
	poi_id not in (select go_id from tmp_swisslife_match)
; 


--


----------------------
-- tmp_helvetia
----------------------
--  google und 61 helvetia: Match 

-- api helvetia data 116
select * from tmp_helvetia;

-- ?? 	Helvetia Versicherungen für-Hypotheken-und-Immobilien-flagship-geneve
--		Helvetia Versicherungen für-Hypotheken-und-Immobilien-flagship-lausanne
--		Helvetia Versicherungen für-Hypotheken-und-Immobilien-flagship-zurich


-- google helvetia data 139
create temp table tmp_helvetia_google
as
select 
	* 
from 
	google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot
where 
	(
	lower(company) like '%helvetia%'
	or 
	lower(url) like '%helvetia%'
	)
	and 
	quelle = 'GOOGLE'
;


-- match data  99
drop table if exists tmp_helvetia_match;
create temp table tmp_helvetia_match
as
select 
	t1.id 				as api_id
	,t2.poi_id 			as go_id
	,t1.company  		as api_company
	,t2.company 		as go_company
	,t1.company_unit
	,t1.adresse			as api_adresse
	,t2.adresse 		as go_adresse
	,t1.plz4 			as api_plz4
	,t2.plz4 			as go_plz4
	,t1.ort 			as api_ort
	,t1.telefon
	,t1.email
	,t1.url 			as api_url
	,t2.url 			as go_url
	,t2.domain 
	,t1.lat
	,t1.lng
	,t1.geo_point_lv95 	as api_geo_point_lv95
	,t2.geo_point_lv95 	as go_geo_point_lv95 	
	,ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) as distance
	,row_number() over(partition by poi_id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95)) as rn
from 
	 tmp_helvetia t1
left join 
	 tmp_helvetia_google  t2
on 
	ST_DWithin(t1.geo_point_lv95, t2.geo_point_lv95, 100)
;




select * from tmp_helvetia_match where rn = 1;

-- duplicates 
-- 60000 70020
select
	api_id
	--go_id
	,count(*)
from
	tmp_helvetia_match
--where 
 	--rn = 1
group by 
	api_id
	--go_id
having 
	count(*) > 1
;


-- match address
select 
	*
from 
	tmp_helvetia_match
where
	lower(api_adresse) <> lower(go_adresse) 
;
-- correction required >> 9489663507969269203	Helvetia Versicherungen Rheintal	Helvetia Versicherungen Generalagentur Rheintal	Generalagentur	ri.nova impulszentrum	alte Landstrasse 106


-- 17 in helvetia not in google match  ?? I can not get this 17 from the following query what could be possiable worng 
select 
	id
from 
	tmp_helvetia
where 
	id not in (select api_id from tmp_helvetia_match)
;
-- 78001 --60010 --65001 --66050 --51010 --57010 --58002


-- 40 in google not in helvetia
select 
	*
from 
	tmp_helvetia_google
where 
	poi_id not in (select go_id from tmp_helvetia_match)
; 




----------------------
-- tmp_generali
----------------------
--  google von  generali: Match 

-- api generali data 54
select * from tmp_generali;

-- google generali data 75
create temp table tmp_generali_google
as
select 
	* 
from 
	google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot
where 
	(
	lower(company) like '%generali%'
	or 
	lower(url) like '%generali.ch%'
	)
	and 
	quelle = 'GOOGLE'
;


-- match data 51
drop table if exists tmp_generali_match;
create temp table tmp_generali_match
as
select
	t1.id 				as api_id
	,t2.poi_id 			as go_id
	,t1.company  		as api_company
	,t2.company 		as go_company
	,t1.company_unit
	,t1.adresse			as api_adresse
	,t2.adresse 		as go_adresse
	,t1.plz4 			as api_plz4
	,t2.plz4 			as go_plz4
	,t1.ort 			as api_ort
	,t2.ort 			as go_ort
	,t1.telefon
	,t1.email
	,t1.url 			as api_url
	,t2.url 			as go_url
	,t2.domain 
	,t1.lat
	,t1.lng
	,t1.geo_point_lv95 	as api_geo_point_lv95
	,t2.geo_point_lv95 	as go_geo_point_lv95 	
	,ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) as distance
	,row_number() over(partition by poi_id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95)) as rn
from 
	 tmp_generali t1
left join 
	 tmp_generali_google  t2
on
	ST_DWithin(t1.geo_point_lv95, t2.geo_point_lv95, 100)
;


select * from tmp_generali_match where rn = 1;

-- duplicates 
--google: 
--api: 39 and 3 it comes five times 
select 
	--go_id
	api_id
	,count(*)
from
	tmp_generali_match
--where 
 	--rn = 1
group by 
	--go_id
	api_id
having 
	count(*) > 1
;


-- match address
select 
	*
from 
	tmp_generali_match
where
	lower(api_adresse) <> lower(go_adresse) 
;
	


-- 8 in generali not in google match
select 
	id
from 
	tmp_generali
where 
	id not in (select api_id from tmp_generali_match)
;


-- 24 in google not in generali
select 
	*
from 
	tmp_generali_google
where 
	poi_id not in (select go_id from tmp_generali_match)
; 
	


--

----------------------
-- tmp_baloise
----------------------
--  google von  baloise: Match 

-- api baloise data 95
select * from tmp_baloise;
select id, count(*) from tmp_baloise group by id having count(*) > 1;

-- google baloise data 115
create temp table tmp_baloise_google
as
select 
	* 
from 
	google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot
where 
	(
		google_poi_typ not like '[Bank]'
		and 
		google_poi_typ not like '[Bankautomat]'
	)
	and
	(
		lower(company) like '%baloise%'
		or 
		lower(company) like '%bâloise%' 
		or
		lower(url) like '%baloise.ch%'
	)
	and 
	quelle = 'GOOGLE'
;

select poi_id, count(*) from tmp_baloise_google group by poi_id having count(*) > 1;


-- match data 82
drop table if exists tmp_baloise_match;
create temp table tmp_baloise_match
as
select
	t1.id 				as api_id
	,t2.poi_id 			as go_id
	,t1.company  		as api_company
	,t2.company 		as go_company
	,t1.company_unit
	,t1.adresse			as api_adresse
	,t2.adresse 		as go_adresse
	,t1.plz4 			as api_plz4
	,t2.plz4 			as go_plz4
	,t1.ort 			as api_ort
	,t2.ort 			as go_ort
	,t1.telefon
	,t1.email
	,t1.url 			as api_url
	,t2.url 			as go_url
	,t2.domain 
	,t1.lat
	,t1.lng
	,t1.geo_point_lv95 	as api_geo_point_lv95
	,t2.geo_point_lv95 	as go_geo_point_lv95 	
	,ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) as distance
	,row_number() over(partition by poi_id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95)) as rn
from 
	 tmp_baloise t1
left join 
	 tmp_baloise_google  t2
on
	ST_DWithin(t1.geo_point_lv95, t2.geo_point_lv95, 100)
;


select * from tmp_baloise_match where rn = 1;

-- duplicates 
--google: 5764609638885703361 and 12734960499776718777
--api: 39 and 3 it comes five times 
select 
	go_id
	--api_id
	,count(*)
from
	tmp_baloise_match
--where 
 	--rn = 1
group by 
	go_id
	--api_id
having 
	count(*) > 1
;


-- match address
select 
	*
from 
	tmp_baloise_match
where
	lower(api_adresse) <> lower(go_adresse) 
;
	


-- 0 in baloise not in google match
select 
	id
from 
	tmp_baloise
where 
	id not in (select api_id from tmp_baloise_match)
;


-- 0 in google not in baloise
select 
	*
from 
	tmp_baloise_google
where 
	poi_id not in (select go_id from tmp_baloise_match)
; 


-- 
----------------------
-- tmp_axa_winterthur
----------------------
--  google von  axa_winterthur: Match 

-- api axa_winterthur data 336
select * from tmp_axa_winterthur;
select company, adresse, count(*) from tmp_axa_winterthur group by company, adresse having count(*) > 1;

-- google axa_winterthur data 363
create temp table tmp_axa_winterthur_google
as
select 
	* 
from 
	google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot
where 
	--(
	--	google_poi_typ not like '[Bank]'
	--	and 
	--	google_poi_typ not like '[Bankautomat]'
	--)
	--and
	(
		lower(company) like '%axa winterthur%'
		or
		lower(url) like '%axa.ch%'
	)
	and 
	quelle = 'GOOGLE'
;

select poi_id, count(*) from tmp_axa_winterthur_google group by poi_id having count(*) > 1;


-- match data 82
drop table if exists tmp_axa_winterthur_match;
create temp table tmp_axa_winterthur_match
as
select distinct 
	t1.id 				as api_id
	,t2.poi_id 			as go_id
	,t1.company  		as api_company
	,t2.company 		as go_company
	,t1.company_unit
	,t1.adresse			as api_adresse
	,t2.adresse 		as go_adresse
	,t1.plz4 			as api_plz4
	,t2.plz4 			as go_plz4
	,t1.ort 			as api_ort
	,t2.ort 			as go_ort
	,t1.telefon
	,t1.email
	,t1.url 			as api_url
	,t2.url 			as go_url
	,t2.domain 
	,t1.lat
	,t1.lng
	,t1.geo_point_lv95 	as api_geo_point_lv95
	,t2.geo_point_lv95 	as go_geo_point_lv95 	
	,ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) as distance
	,row_number() over(partition by poi_id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95)) as rn
from 
	 tmp_axa_winterthur t1
left join 
	 tmp_axa_winterthur_google  t2
on
	ST_DWithin(t1.geo_point_lv95, t2.geo_point_lv95, 100)
;


select * from tmp_axa_winterthur_match where rn = 1;

-- duplicates 
--google: 5764609638885703361 and 12734960499776718777
--api: 39 and 3 it comes five times 
select 
	go_id
	--api_id
	,count(*)
from
	tmp_axa_winterthur_match
--where 
 	--rn = 1
group by 
	go_id
	--api_id
having 
	count(*) > 1
;


-- match address
select 
	*
from 
	tmp_axa_winterthur_match
where
	lower(api_adresse) <> lower(go_adresse) 
;
	


-- 0 in axa_winterthur not in google match
select 
	id
from 
	tmp_axa_winterthur
where 
	id not in (select api_id from tmp_axa_winterthur_match)
;


-- 0 in google not in axa_winterthur
select 
	*
from 
	tmp_axa_winterthur_google
where 
	poi_id not in (select go_id from tmp_axa_winterthur_match)
; 










-------------------------
-- All
-------------------------
drop table if exists tmp_google_match;
create temp table tmp_google_match
as
select 
	t1.id 				as api_id
	,t2.poi_id 			as go_id
	,t1.company  		as api_company
	,t2.company 		as go_company
	,t1.adresse			as api_adresse
	,t2.adresse 		as go_adresse
	,t1.plz4 			as api_plz4
	,t2.plz4 			as go_plz4
	,t1.ort 			as api_ort
	,t2.ort 			as go_ort
	,t1.url 			as api_url
	,t2.url 			as go_url
	,t1.geo_point_lv95 	as api_geo_point_lv95
	,t2.geo_point_lv95 	as go_geo_point_lv95 	
	,ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) as distance 
from 
	allianz.allianz_konkurrenten_api t1
left join 
	google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot t2
on
	ST_DWithin(t1.geo_point_lv95, t2.geo_point_lv95, 50)
	and 
	lower(t1.adresse) ilike '%' || lower(t2.adresse) || '%' 
where
	t2.quelle = 'GOOGLE'
	and
	(
	lower(t2.company) like '%allianz suisse%'
	or
	lower(t2.company) like '%axa%'
	or 
	lower(t2.company) like '%bâloise%'
	or 
	lower(t2.company) like '%baloise%'
	or 
	lower(t2.company) like '%mobiliar%'
	or 
	lower(t2.company) like '%generali%'
	or
	lower(t2.company) like '%helvetia versicherungen%'
	or 
	lower(t2.company) like '%swiss life%'
	or
	lower(t2.company) like '%swisslife%'
	or
	lower(t2.company) like '%vaudoise%'
	or  
	lower(t2.company) like '%zurich%'
	)
	--and 
	--lower(t1.company) ~ lower(t2.company)
;



select 
	*
from (
		select 
			*
			,row_number() over(partition by poi_id order by distance) as rn
		from 
			tmp_google_match
) as t
where 
	rn = 1
	and 
	lower(adresse) ilike '%' || lower(go_adresse) || '%'
;


select 
	*
from 
	tmp_google_match
where 
	poi_id in (
				select 
					poi_id
					--,count(*)
				from 
					tmp_google_match
				group by 
					poi_id
				having 
					count(*) > 1
	)
;

--t2.adresse like '%Via Cantonale 18%'














	