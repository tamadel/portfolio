----------------------
-- generali
----------------------
--  google von  generali: Match 
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
	,row_number() over(partition by  t1.id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) asc, (t1.company = t2.company)::int desc ) 		as api_rn
	,row_number() over(partition by  t2.poi_id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) asc, (t1.company = t2.company)::int desc ) 	as go_rn
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
	*
from 
	tmp_generali_match
where 
	go_id is null
;


-- 24 in google not in generali
select 
	*
from 
	tmp_generali_google
where 
	poi_id not in (select go_id from tmp_generali_match where go_id is not null)
; 





-- create generali table for final stage 
drop table if exists geo_afo_tmp.tmp_generali_final;
create table 
		geo_afo_tmp.tmp_generali_final
as
select 
	*
from 
	tmp_generali_match
;

select * from geo_afo_tmp.tmp_generali_final;



select 
	--go_id
	api_id
	,count(*)
from
	geo_afo_tmp.tmp_generali_final
group by 
	--go_id
	api_id
having 
	count(*) > 1
;










	