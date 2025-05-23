--=================================
--Project: 
-- Date: 04.03.2025
-- Update 
--==================================
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


--=======================================================================================================
-- With helep of "geo_afo_prod.lay_poi_geo_2024_hist" create the table 
select 
	*
from
	geo_afo_prod.lay_poi_geo_2024_hist
where 
	'6002002' = any(poi_typ_id_list)
;


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
	


-- add company 
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


-- add company_group 
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



select * from tmp_versicherung_comp_group;



drop table if exists geo_afo_tmp.tmp_versicherung_tot;
create table 
	geo_afo_tmp.tmp_versicherung_tot
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

select * from geo_afo_tmp.tmp_versicherung_tot where lower(adresse) like '%industriestrasse 14%';



-- Data von Google that not exist in "geo_afo_tmp.tmp_versicherung_tot"
select 
	*
from 
	google_maps_dev_abgleich.poi_abgleich_google_finanzdienstleistung_tot
where
	( 
	google_poi_typ = '[Versicherungsagentur]'
	or 
	poi_typ_id = 105
	)
	and
	quelle = 'AFO'
	and 
	dubletten_nr is not null
	and 
	poi_id not in ( 
					select 
						--cid
						poi_id::text
					from 
						geo_afo_tmp.tmp_versicherung_tot
	)
;


-- Note: there are 23 poi_id from afo that have match with google data and they are not exists in the table from 
-- "geo_afo_prod.lay_poi_geo_2024_hist"









