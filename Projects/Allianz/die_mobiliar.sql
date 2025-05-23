---------------------------------------------
-- Die Mobiliar
---------------------------------------------
-- 222 google von 250 Mobiliar: Match 169
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


-- match data 254
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
	,row_number() over(partition by  t1.id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) asc, (t1.company = t2.company)::int desc ) 		as api_rn
	,row_number() over(partition by  t2.poi_id order by ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) asc, (t1.company = t2.company)::int desc ) 	as go_rn
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
--api: 6843 6796 6776 6885 every id asigne to 2 differnt cid from google one with name "Die Mobiliar" and the other with "Schweizerische Mobiliar"
select 
	go_id
	--,api_id
	,count(*)
from
	--tmp_mobiliar_match
	geo_afo_tmp.tmp_mobiliar_final
group by 
	go_id
	--,api_id
having 
	count(*) > 1
;



-- match address
select 
	*
from 
	tmp_mobiliar_match
where
	lower(api_adresse) <> lower(go_adresse) 
;
-- test case: 7710842333131462480
	

-- 85 in mobiliar not in google match
select 
	count(distinct go_id)
from 
	tmp_mobiliar_match
where 
	go_id is null
;


-- 55 in google not in mobiliar
select 
	*
from 
	tmp_mobiliar_google
where 
	poi_id not in (select go_id from tmp_mobiliar_match where go_id is not null)
; 
	

-- create mobiliar table for final stage 
drop table if exists geo_afo_tmp.tmp_mobiliar_final;
create table 
		geo_afo_tmp.tmp_mobiliar_final
as
select 
	*
from 
	tmp_mobiliar_match
;

select * from geo_afo_tmp.tmp_mobiliar_final;


select 
	--go_id
	api_id
	,count(*)
from
	--tmp_mobiliar_match
	geo_afo_tmp.tmp_mobiliar_final
group by 
	--go_id
	api_id
having 
	count(*) > 1
;





















