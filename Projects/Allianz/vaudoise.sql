----------------------
-- Vaudoise
----------------------
-- 127 google und 115 vaudoise: Match 102 
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


select 
	*
from 
	tmp_vaudoise_google
where
 	ort in (
				'Mézières VD'
				,'Sugiez'
				,'Yvonand'
				,'Brusio (GR)'
				,'Vaduz'
				,'Bussigny'
				,'Genf'
				,'Nyon'
 	)
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
	,row_number() over(partition by  t1.id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) asc, (t1.company = t2.company)::int desc ) 		as api_rn
	,row_number() over(partition by  t2.poi_id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) asc, (t1.company = t2.company)::int desc ) 	as go_rn
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



-- create vaudoise table for final stage 
drop table if exists geo_afo_tmp.tmp_vaudoise_final;
create table 
		geo_afo_tmp.tmp_vaudoise_final
as
select 
	*
from 
	tmp_vaudoise_match
;

select * from geo_afo_tmp.tmp_vaudoise_final;






select 
	api_id
	--go_id
	,count(*)
from
	geo_afo_tmp.tmp_vaudoise_final
group by
	api_id
	--go_id
having 
	count(*) > 1
;














