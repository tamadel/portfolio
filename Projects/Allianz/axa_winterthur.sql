----------------------
-- tmp_axa_winterthur
----------------------
select * from allianz.axa_winterthur;

--  google von  axa_winterthur: Match 

/*
drop table if exists geo_afo_tmp.tmp_axa_winterthur;
create table geo_afo_tmp.tmp_axa_winterthur
as	
select distinct 
	DENSE_RANK() OVER (ORDER BY COALESCE('AXA Winterthur' || ' ' || agency_name, ''), agency_street,agency_zip, agency_city) as id
	,agency_type::text 											as type
	,coalesce('AXA Winterthur'|| ' ' || agency_name, null)   	as company
	,split_part(agency_name, ' ',1)								as company_unit
	,email 														as emp_email
	,function 											     	as job_function
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

select * from geo_afo_tmp.tmp_axa_winterthur;
*/

-- 2) AXA Winterthur 336
drop table if exists tmp_axa_winterthur;
create temp table tmp_axa_winterthur
as
select  
    dense_rank() over (order by coalesce('AXA Winterthur' || ' ' || agency_name, ''), agency_street, agency_zip, agency_city) 	as id
    ,agency_type::text 																											as type
    ,coalesce('AXA Winterthur' || ' ' || agency_name, null) 																	as company
    ,coalesce(split_part(agency_name, ' ', 1)|| ' / ' ||  agency_type, null)														as company_unit
    ,agency_street 																												as adresse
    ,agency_zip 																												as plz4 
    ,agency_city 																												as ort
    ,agency_phone::text 																										as telefon
    ,agency_mail 																												as email
    ,agency_link 																												as url
    ,agency_latitude 																											as lat
    ,agency_longitude 																											as lng
    ,ST_Transform(ST_SetSRID(ST_MakePoint(agency_longitude, agency_latitude), 4326), 2056) 										as geo_point_lv95
    ,ST_Transform(ST_SetSRID(ST_MakePoint(agency_longitude, agency_latitude), 2056), 4326) 										as geo_point_wgs84
    ,ST_Transform(ST_SetSRID(ST_MakePoint(agency_longitude, agency_latitude), 4326), 21781) 									as geo_point_lv03
    ,count(function) filter (where function = 'AD') 																			as aussendienst
    ,count(function) filter (where function = 'ID') 																			as innendienst
    ,count(function) filter (where function = 'AD') + count(function) filter (where function = 'ID') 							as ma_total
from  
    allianz.axa_winterthur
where 
	agency_type <> 'SAVV'
group by  
    agency_type
    ,agency_name
    ,agency_street
    ,agency_zip
    ,agency_city
    ,agency_phone
    ,agency_mail
    ,agency_link
    ,agency_latitude
    ,agency_longitude
;



select * from tmp_axa_winterthur;
--test cases : AXA Winterthur Generalagentur Michael Zeller Trogenerstrasse 13
--	


-- api axa_winterthur data 336
select * from tmp_axa_winterthur;
select company, adresse, count(*) from tmp_axa_winterthur group by company, adresse having count(*) > 1;

-- google axa_winterthur data 363
drop table tmp_axa_winterthur_google;
create temp table tmp_axa_winterthur_google
as
select 
	* 
from 
	google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot
where 
	lower(company) not like '%vorsorge & vermögen%'
	and
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
	t1.type 			as category
	,t1.id 				as api_id
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
	,t1.ma_total
	,t1.aussendienst
	,t1.innendienst
	,t2.geo_point_lv95 	as go_geo_point_lv95 	
	,ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) as distance
	,row_number() over(partition by  t1.id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) asc, (t1.company = t2.company)::int desc ) 		as api_rn
	,row_number() over(partition by  t2.poi_id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) asc, (t1.company = t2.company)::int desc ) 	as go_rn
from 
	 tmp_axa_winterthur t1
left join 
	 tmp_axa_winterthur_google  t2
on
	ST_DWithin(t1.geo_point_lv95, t2.geo_point_lv95, 100)
;


select * from tmp_axa_winterthur_match where api_rn = 1 or go_rn = 1;

-- duplicates 
--google: 5764609638885703361 and 12734960499776718777
--api: 39 and 3 it comes five times 
select 
	--go_id
	api_id
	,count(*)
from
	tmp_axa_winterthur_match
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
	tmp_axa_winterthur_match
where
	lower(api_adresse) <> lower(go_adresse) 
	and 
	api_rn = 1
;
	


-- 22 in axa_winterthur not in google match
select 
	*
from 
	tmp_axa_winterthur_match
where 
	go_id is null
;


-- 41 in google not in axa_winterthur
select 
	*
from 
	tmp_axa_winterthur_google
where 
	poi_id not in (select go_id from tmp_axa_winterthur_match where go_id is not null)
; 



-- create axa_winterthur table for final stage 
drop table if exists geo_afo_tmp.tmp_axa_winterthur_final;
create table 
		geo_afo_tmp.tmp_axa_winterthur_final
as
select 
	*
from 
	tmp_axa_winterthur_match
where 
	api_rn = 1
;




select * from geo_afo_tmp.tmp_axa_winterthur_final;





select 
	go_id
	--api_id
	,count(*)
from
	geo_afo_tmp.tmp_axa_winterthur_final
group by 
	go_id
	--api_id
having 
	count(*) > 1
;

select 
	*
from 
	geo_afo_tmp.tmp_axa_winterthur_final
where 
	go_id in (
				select 
					go_id
				from
					geo_afo_tmp.tmp_axa_winterthur_final
				group by 
					go_id
				having 
					count(*) > 1
	)
and 
 go_rn <> 1
;



update geo_afo_tmp.tmp_axa_winterthur_final
set 
	go_id = null
where 
	go_id in (
				select 
					go_id
				from
					geo_afo_tmp.tmp_axa_winterthur_final
				group by 
					go_id
				having 
					count(*) > 1
	)
and 
 go_rn <> 1
;





