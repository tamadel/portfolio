---------------------------------------------------
-- Zurich 
---------------------------------------------------
-- 233 google und 138 zurich: Match 148 
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

select * from allianz.zurich_agencies;

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
	 tmp_zurich t1
left join 
	 tmp_zurich_google  t2
on 
	ST_DWithin(t1.geo_point_lv95, t2.geo_point_lv95, 100)
;

select * from tmp_zurich_match where api_rn = 1;

-- duplicates 
-- google:	11
-- api:	  	30 
select 
	--api_id
	go_id
	,count(*)
from
	tmp_zurich_match
--where 
 	--rn = 1
group by 
	--api_id
	go_id
having 
	count(*) > 1
;



-- match address
select 
	*
from 
	tmp_zurich_match
where
	lower(api_adresse) <> lower(go_adresse) 
;

--6000585730825712707


-- 26 in zurich not in google match
select 
	*
from 
	tmp_zurich_match
where 
	go_id is null
;


-- 97 in google not in zurich
select 
	*
from 
	tmp_zurich_google
where 
	poi_id not in (select go_id from tmp_zurich_match where go_id is not null)
; 
	

-- create zurich table for final stage 
drop table if exists geo_afo_tmp.tmp_zurich_final;
create table 
		geo_afo_tmp.tmp_zurich_final
as
select 
	*
from 
	tmp_zurich_match
where 
	api_rn = 1
;

select 
	* 
from 
	geo_afo_tmp.tmp_zurich_final
where 
	go_id in (
				select 
					go_id 
				from 
					geo_afo_tmp.tmp_zurich_final
				group by 
					--api_id
					go_id
				having 
					count(*) > 1
	)
;

-- case1: 9190531149024404666  https://www.zurich.ch/de/ueber-uns/standorte# 2 differents id same cid
-- case2: 6169427421289946932  
-- Zurich, Agence Générale Walter Tosalli, Freiburg/Fribourg


select 
	*
from 
	geo_afo_tmp.tmp_zurich_final
where 
	go_id in ('1653380808995993627', '16909150673779446266', '18094680522615932973', '6169427421289946932')
	and 
	go_rn > 1
;



update geo_afo_tmp.tmp_zurich_final
set 
	go_id = null 
where 
	go_id in ('1653380808995993627', '16909150673779446266', '18094680522615932973', '6169427421289946932')
	and 
	go_rn > 1
;









