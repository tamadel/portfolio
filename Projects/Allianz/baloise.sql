----------------------
--baloise
----------------------
--  google von  baloise: Match 
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
	,row_number() over(partition by  t1.id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) asc, (t1.company = t2.company)::int desc ) 		as api_rn
	,row_number() over(partition by  t2.poi_id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) asc, (t1.company = t2.company)::int desc ) 	as go_rn
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
	


-- 17 in baloise not in google match
select 
	*
from 
	tmp_baloise_match
where 
	go_id is null
;


-- 35 in google not in baloise
select 
	*
from 
	tmp_baloise_google
where 
	poi_id not in (select go_id from tmp_baloise_match where go_id is not null)
; 




-- create baloise table for final stage 
drop table if exists geo_afo_tmp.tmp_baloise_final;
create table 
		geo_afo_tmp.tmp_baloise_final
as
select 
	*
from 
	tmp_baloise_match
;

select * from geo_afo_tmp.tmp_baloise_final;



select 
	--go_id
	api_id
	,count(*)
from
	geo_afo_tmp.tmp_baloise_final
group by 
	--go_id
	api_id
having 
	count(*) > 1
;




