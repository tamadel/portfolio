----------------------
-- swisslife
----------------------
-- Source
select * from allianz.swisslife_agencies;

--  google und 61 swisslife: Match 
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
	,row_number() over(partition by  t1.id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) asc, (t1.company = t2.company)::int desc ) 		as api_rn
	,row_number() over(partition by  t2.poi_id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) asc, (t1.company = t2.company)::int desc ) 	as go_rn
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
	tmp_swisslife_match
where 
	go_id is null
;


-- 29 in google not in swisslife
select 
	*
from 
	tmp_swisslife_google
where 
	poi_id not in (select go_id from tmp_swisslife_match where go_id is not null)
;



-- create mobiliar table for final stage 
drop table if exists geo_afo_tmp.tmp_mobiliar_final;
create table 
		geo_afo_tmp.tmp_swisslife_final
as
select 
	*
from 
	tmp_swisslife_match
;

select * from geo_afo_tmp.tmp_swisslife_final where api_id not in (select id from allianz.swisslife_agencies);
select * from allianz.swisslife_agencies where id not in (select api_id from geo_afo_tmp.tmp_swisslife_final);

select 
	api_id
	--go_id
	,count(*)
from
	geo_afo_tmp.tmp_swisslife_final
group by 
	api_id
	--go_id
having 
	count(*) > 1
;













