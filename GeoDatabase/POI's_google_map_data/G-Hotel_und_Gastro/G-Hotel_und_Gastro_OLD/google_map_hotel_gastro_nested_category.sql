--=============================================================
-- Hotel/Gastro
-- fixing Poi_typ and category >> make it list as category_ids 
-- 11.11.2024
--============================================================= 
-- Dataset Hotel/Gastro v3: create temp for test before update the original table 
create temp table tmp_hotl_gastro_test
as
select
	*
from 
	google_maps_dev.google_map_hotel_gastronomie_v3
;

-- filiter category_ids jsonb to be exactly as category_id_en
SELECT 
	t.cid,
    t.category_ids_en,
    jsonb_agg(elem.filtered_category) AS filtered_category_ids,
    t.category_ids 
FROM 
    google_maps_dev.google_map_hotel_gastronomie_v3 t
JOIN LATERAL (
    SELECT 
        jsonb_array_elements_text(t.category_ids) AS filtered_category
) AS elem ON elem.filtered_category = ANY(string_to_array(t.category_ids_en, ' | '))
GROUP BY 
    t.category_ids_en,
  	t.category_ids,
  	t.cid
  ;

select 
	*
from 
	google_maps_dev.google_map_hotel_gastronomie_v3
where
	cid = '1913848203110022153'



--Step(1) Unnest: create a temp table to store the unnest results 
create temp table tmp_unnest_category
as
select 
	t0.*
	,t1.category_part
from
	tmp_hotl_gastro_test t0
join(
	select  
    	cid,
    	unnest(string_to_array(REPLACE(REPLACE(category_ids_en, '[', ''), ']', ''), ' | ')) AS category_part
	from   
    	tmp_hotl_gastro_test
) t1
on 
	t0.cid = t1.cid
;


select 
	*
from 
	tmp_unnest_category
;
	

--Step(2): Mapping: mapping poi_typ and kategorrie to the unnested category 
update 
	tmp_unnest_category t1
set 
	poi_typ = t2.poi_typ_neu,
	kategorie = t2.kategorie_neu, 
	hauptkategorie = t2.hauptkategorie_neu 
from 
	geo_afo_prod.meta_poi_categories_business_data_aktuell t2
where 
	t1.category_part = t2.category_id 
	and 
	t2.hauptkategorie_neu = 'Hotel & Gastronomie'
;


--Step(3)Nest: nest again 
create temp table tmp_nested_category_poi_typ
as
select   
    cid,
    category_ids_en,
    '[' || array_to_string(array_agg(distinct poi_typ), ' | ') || ']' AS poi_typ,
    '[' || array_to_string(array_agg(distinct kategorie), ' | ') || ']' AS kategorie
from (
    select distinct 
        cid,
        category_ids_en,
        poi_typ,
        kategorie
    from  
        tmp_unnest_category
    where 
        category_part IS NOT NULL
) as distinct_categories
group by   
    cid,
	category_ids_en
;



--Step(4) Update: update temp table to check the results 
update tmp_hotl_gastro_test t0
set 
	poi_typ = t1.poi_typ,
	kategorie = t1.kategorie
from 
	tmp_nested_category_poi_typ t1
where 
	t0.cid = t1.cid
;


select 
	*
from 
	tmp_hotl_gastro_test
;


--========================================
-- Update google_map_hotel_gastronomie_v3
-- with nested poi and category
--========================================
--Point(2) complete poi_typ and category
select 
	*
from 
	google_maps_dev.google_map_hotel_gastronomie_v3
;


update google_maps_dev.google_map_hotel_gastronomie_v3 t0
set 
	poi_typ = t1.poi_typ,
	kategorie = t1.kategorie
from 
	tmp_nested_category_poi_typ t1
where 
	t0.cid = t1.cid
;
--///////////////////////////////////////////////////////////////////////////////////////////////////////

--===========================================
-- Adjustments: Peter Eamil von 10.NOV 2024
--===========================================
--Point(1) Hotel vs Hostel test
select
	*
from 
	google_maps_dev.google_map_hotel_gastronomie_v3
where 
	lower(bezeichnung) like '%hostel%'
	or
	category_ids_en like '[%hostel%]'
	--lower(kategorie) like '%[hostel]%'
	--and
	--kategorie <> '[Hotel]'
;

--Point(3) null korr_strasse take it from gwr_strasse 
-- Str Name von GWR
select 
	cid 
	,bezeichnung 
	,adresse 
	,korr_strasse  
	,gwr_strasse
	,geo_point_google 
	,geo_point_gwr 
	,distance 
from 
	google_maps_dev.google_map_hotel_gastronomie_v4    --tmp_hotl_gastro_test
where 
	korr_strasse is null
	and 
	gwr_strasse is not null
order by 
	distance desc
;



update google_maps_dev.google_map_hotel_gastronomie_v3 --tmp_hotl_gastro_test
set 
	korr_strasse = gwr_strasse 
where 
	korr_strasse is null
	and 
	gwr_strasse is not null
;


select 
	*
from 
	google_maps_dev.google_map_hotel_gastronomie_v3 --tmp_hotl_gastro_test
where 
	korr_strasse is null
	and 
	gwr_strasse is not null
;


--Point(4) bezeichnung that contain category name 

--Bars >> 251
select 
    bezeichnung,
    category_ids_en,
    category_ids_de,
    kategorie,
    poi_typ
from 
    google_maps_dev.google_map_hotel_gastronomie_v4
where 
    LOWER(bezeichnung) like '% bar %'
    and  
    kategorie not in ('[Bar/Pub]', '[Bar/Pub | Café]', '[Bar/Pub | Café | Hotel | Restaurant]',
                      '[Bar/Pub | Café | Restaurant]', '[Bar/Pub | Hotel]', '[Bar/Pub | Hotel | Restaurant]',
                      '[Bar/Pub | Restaurant]')
;

		
--Hotels >> 143
select 
    bezeichnung,
    category_ids_en,
    category_ids_de,
    kategorie,
    poi_typ
from 
    google_maps_dev.google_map_hotel_gastronomie_v3
where 
    LOWER(bezeichnung) like '%hotel%'
    and  
    kategorie not in ('[Bar | Hotel]', '[Bar/Pub | Café | Hotel | Restaurant]', '[Bar/Pub | Café | Hotel]',
                      '[Bar/Pub | Hotel | Restaurant]', '[Bar/Pub | Hotel]', '[Café | Hotel]',
                      '[Bar/Pub | Restaurant]', '[Café | Hotel | Restaurant]', '[Hotel]', '[Hotel | Restaurant]')
;	
	


--Restaurants >> 192
select 
    bezeichnung,
    category_ids_en,
    category_ids_de,
    kategorie,
    poi_typ
from 
    google_maps_dev.google_map_hotel_gastronomie_v4
where 
    LOWER(bezeichnung) like '%restaurant%'
    and 
    LOWER(bezeichnung) not like '%hotel%'
    and  
    kategorie not in ('[Bar/Pub | Café | Restaurant]', '[Bar/Pub | Café | Hotel | Restaurant]', '[Café | Restaurant]',
                      '[Bar/Pub | Hotel | Restaurant]', '[Bar/Pub | Restaurant]', '[Catering | Restaurant]',
                      '[Bar/Pub | Restaurant]', '[Café | Hotel | Restaurant]', '[Restaurant]', '[Hotel | Restaurant]')
;


--Takeaway >> 43
select 
    bezeichnung,
    category_ids_en,
    category_ids_de,
    kategorie,
    poi_typ
from 
    google_maps_dev.google_map_hotel_gastronomie_v4
where 
    LOWER(bezeichnung) like '% takeaway%'
    and  
    kategorie not in ('[Takeaway]', '[Catering | Takeaway]')
;









--Cafè >> 129
select 
    bezeichnung,
    category_ids_en,
    category_ids_de,
    kategorie,
    poi_typ
from 
    google_maps_dev.google_map_hotel_gastronomie_v3
where 
    LOWER(bezeichnung) like '%cafe%'
    and  
    kategorie not in ('[Bar/Pub | Café]', '[Bar/Pub | Café | Restaurant]' , '[Café]', '[Café | Hotel]', '[Café | Restaurant]')
;




select 
	*
from 
	geo_afo_prod.meta_poi_categories_business_data_aktuell
where 
	hauptkategorie_neu like '%Hotel%'
;


--====================================
-- category_ids >> Reihenfolge
--====================================
drop table if exists tmp_gogl_restaurant_hotel;
create temp table 
	tmp_gogl_restaurant_hotel
as
select 
	*
from 
	google_maps_dev.google_map_items 
where 
	lower(keyword) like '%restaurants%'
	or
	lower(keyword) like '%hotels%' 
;


select 
*
from 
	tmp_gogl_restaurant_hotel
where
	cid = '10002785409691413577'
;














