----------------------
-- helvetia
----------------------
--  google und 61 helvetia: Match 
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


-- api helvetia data 116 with "moneypark.ch" and 113 without
select * from tmp_helvetia;

-- The following three Standorte belong to "moneypark.ch" which is part of Helvetia. They are not a Versicherung, which is why they have been eliminated.
--  	Helvetia Versicherungen für-Hypotheken-und-Immobilien-flagship-geneve
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
	,row_number() over(partition by  t1.id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) asc, (t1.company = t2.company)::int desc ) 		as api_rn
	,row_number() over(partition by  t2.poi_id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) asc, (t1.company = t2.company)::int desc ) 	as go_rn
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


-- 16 in helvetia not in google match  ?? I can not get this 17 from the following query what could be possiable worng 
select 
	*
from 
	tmp_helvetia_match
where 
	go_id is null
;
-- 78001 --60010 --65001 --66050 --51010 --57010 --58002


-- 40 in google not in helvetia
select 
	*
from 
	tmp_helvetia_google
where 
	poi_id not in (select go_id from tmp_helvetia_match where go_id is not null)
; 


-- create helvetia table for final stage 
drop table if exists geo_afo_tmp.tmp_helvetia_final;
create table 
		geo_afo_tmp.tmp_helvetia_final
as
select 
	*
from 
	tmp_helvetia_match
;

select * from geo_afo_tmp.tmp_helvetia_final;



select   
	--api_id
	go_id
	,count(*)
from
	geo_afo_tmp.tmp_helvetia_final
group by 
	--api_id
	go_id
having 
	count(*) > 1
;












