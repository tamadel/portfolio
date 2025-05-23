--============================
-- Project: Einkaufszentrum
-- Date:
--
--============================

drop table if exists
	google_maps_dev.poi_abgleich_google_hotel_gastro_tot;
create table
	google_maps_dev.poi_abgleich_google_hotel_gastro_tot
as
select  
	*
from  
	dblink(
		'geo_database',
		$POSTGRES$
			select
				* 
			from
				google_maps_dev_abgleich.poi_abgleich_google_hotel_gastro_tot
		$POSTGRES$
	) as poi_abgleich_google_food_tot (
			poi_id varchar(1000) 
			,hauskey numeric 
			,poi_typ_id numeric 
			,poi_typ text 
			,google_poi_typ varchar(1000) 
			,category_ids varchar(1000) 
			,company_group_id numeric 
			,company_group text 
			,company_id numeric 
			,company text 
			,company_unit text 
			,company_brand text 
			,bezeichnung_lang text 
			,bezeichnung_kurz text 
			,adresse text 
			,adress_lang varchar(1000) 
			,plz4 numeric 
			,plz4_orig numeric 
			,ort text 
			,google_strasse varchar(1000) 
			,google_strasse_std varchar(1000) 
			,google_hausnum varchar(1000) 
			,google_plz4 varchar(1000) 
			,google_ort varchar(1000) 
			,gwr_strasse varchar(1000) 
			,gwr_hausnum varchar(1000) 
			,gwr_plz4 int4 
			,gwr_ort varchar(1000) 
			,plz6 varchar(1000) 
			,gemeinde varchar(1000) 
			,gmd_nr varchar(1000) 
			,url varchar(10000) 
			,"domain" varchar(1000) 
			,geo_point_lv95 public.geometry(point, 2056) 
			,quelle varchar(255) 
			,dubletten_nr varchar(1000)
	)
;


select * from google_maps_dev.poi_abgleich_google_einkaufszentrum_tot;

--  google_maps_dev.poi_abgleich_google_einkaufszentrum_tot;
--  select * from google_maps_dev.poi_abgleich_google_non_food_tot 	    --56721
--  google_maps_dev_abgleich.poi_abgleich_google_food_tot 			--39289
--  google_maps_dev_abgleich.poi_abgleich_google_hotel_gastro_tot   --83273


--==========================================================================

drop table if exists geo_afo_tmp.tmp_einkaufszentrum_gesch;
create table 
	geo_afo_tmp.tmp_einkaufszentrum_gesch
as
with shopping_centers as (
    select  
        poi_id 								as sc_id,
        google_poi_typ 						as sc_poi_typ,
        category_ids 						as sc_category_ids,
        company 							as sc_name,
        ST_Buffer(geo_point_lv95, 100) 		as sc_area,
        geo_point_lv95 
    from
    	google_maps_dev.poi_abgleich_google_einkaufszentrum_tot
    where 
    	quelle = 'GOOGLE'
)
select  
    nf.poi_id 												as gesch_id,
    sc.sc_id,
    nf.google_poi_typ 										as gesch_poi_typ,
    sc.sc_poi_typ,
    nf.category_ids 										as gesch_category_ids,
    sc.sc_category_ids,
    nf.company 												as gesch_name,
    sc.sc_name,
    nf.geo_point_lv95 										as gesch_point_lv95,
    sc.geo_point_lv95 										as sc_point_lv95,
    ST_Distance(nf.geo_point_lv95, sc.geo_point_lv95) 		as distance_meters
from (
    -- Combine non-food, food, and restaurant POIs
    select * from google_maps_dev.poi_abgleich_google_non_food_tot where quelle = 'GOOGLE'
    union all 
    select * from google_maps_dev.poi_abgleich_google_food_tot where quelle = 'GOOGLE'
    union all 
    select * from google_maps_dev.poi_abgleich_google_hotel_gastro_tot where quelle = 'GOOGLE'
) nf
join
	shopping_centers sc 
on
	ST_DWithin(nf.geo_point_lv95, sc.geo_point_lv95, 30)  -- 50 (12299) --20 (3354) --30 (5274) meters buffer
order by 
	sc.sc_name, 
	distance_meters
;

select * from geo_afo_tmp.tmp_einkaufszentrum_gesch;




select 
	shopping_center_id,
	count(*)
from
	geo_afo_tmp.tmp_einkaufszentrum_gesch
group by 
	shopping_center_id
having 
	count(*) > 1
order by 
 count(*) desc 
;


-- sc_id = '15474258792423893168' Balexert it has 140 stores
--===========================================================================


select 
	* 
from 
	google_maps_dev.poi_abgleich_google_food_tot   --poi_abgleich_google_non_food_tot 
where 
	quelle = 'GOOGLE'
	and
	lower(company) like '%einkaufszentrum%' 
	or 
	lower(company) like '%shopping center%'
	or 
	lower(company) like '%center%'
	or
	lower(google_poi_typ) like '%einkaufszentrum%' 
	or
	lower(google_poi_typ) like '%shopping center%'
	or
	lower(category_ids) ilike '%shopping_center%' 
;

--886513595036034203     google_poi_typ has no Kaufzentrum but poi is actuall Einkaufszentrum
--15625514816775261317


--========================================================================================================

select 
	* 
from 
	google_maps_dev.poi_abgleich_google_einkaufszentrum_tot
where 
 dubletten_nr is not null;
	quelle = 'GOOGLE'
	and 
	category_ids like '[shopping_center]'
;


-- [railway_services | shopping_center]
-- 


-- Einkaufszentrum by AFO poi_typ_id =504 total= 230
-- 
select 
	*
from 
	google_maps_dev.poi_abgleich_google_einkaufszentrum_tot
where 
 	--dubletten_nr is not null;
	quelle = 'AFO' --'GOOGLE'
	and 
	poi_typ_id = 504
;



-- Einkaufszentrum by Google total= 1001
-- total 1001 including them 97 match with AFO pois 
select 
	*
from 
	google_maps_dev.poi_abgleich_google_einkaufszentrum_tot
where 
 	dubletten_nr is not null
 	and
	quelle = 'GOOGLE'
;


-- Einkaufszentrum by Google where category_ids has no other category but [shopping_center]
-- total 654 including them 73 match with AFO pois 
select 
	*
from 
	google_maps_dev.poi_abgleich_google_einkaufszentrum_tot
where 
 	--dubletten_nr is not null;
	quelle = 'GOOGLE'
	and 
	category_ids like '[shopping_center]'
;



-- Einkaufszenrum by AFO that has no match mit google Data 	
-- 133
select 
	*
from 
	google_maps_dev.poi_abgleich_google_einkaufszentrum_tot
where 
 	dubletten_nr is null
 	and
	quelle = 'AFO' --'GOOGLE'
	and 
	poi_typ_id = 504
;



-- Einkaufszenrum by GOOGLE that has no match mit AFO Data 	
-- 
select 
	*
from 
	google_maps_dev.poi_abgleich_google_einkaufszentrum_tot
where 
 	dubletten_nr is null
 	and
	quelle = 'GOOGLE'
	and 
	poi_typ_id <> 504
;




-- 29 have match with afo thrugh the url 
select 
	t1.company as google_company
	,t2.company as afo_company
	,t1.adress_lang as google_adresse
	,t2.adresse as afo_adresse
	,t1.url 		as google_url 
	,t2.url 	as afo_url
	,t1.poi_id  as google_cid
	,t2.poi_id 	as afo_id
from(
		select 
			*
		from 
			google_maps_dev.poi_abgleich_google_einkaufszentrum_tot
		where 
		 	dubletten_nr is null
		 	and
			quelle = 'GOOGLE'
			and 
			poi_typ_id <> 504
) t1
join (
		select 
			*
		from 
			google_maps_dev.poi_abgleich_google_einkaufszentrum_tot
		where 
		 	dubletten_nr is null
		 	and
			quelle = 'AFO' --'GOOGLE'
			and 
			poi_typ_id = 504
) t2
on 
	t1.url ilike '%' || t2.url || '%' 
;
	
















