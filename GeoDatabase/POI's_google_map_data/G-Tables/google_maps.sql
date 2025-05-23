--/////////////////////////// GOOGLE MAPS EINE TABELLE ///////////////////////////
select 
	*
from 
	google_maps_dev.google_map_metadata;

 select 
 	*
 from 
 	google_maps_dev.google_map_results
 order by
	random()
	limit 20;
 
 
drop table if exists google_maps_dev.google_map_items;
create table google_maps_dev.google_map_items
as
select
    t.cid,
    t.rank_absolute,
    t.keyword,
    t.address_info->>'zip' as plz4,
    t.address_info->>'city' as ort,
    t.address_info->> 'address' as strasse,
    t.address_info->>'country_code' as country_code,
    t.address,
    t.title,
    t.domain,
    t.url,
    t.rating,
    t.hotel_rating,
    t.category,
    t.additional_categories,
    t.category_ids,
    t.work_hours,
    st_transform(ST_SetSRID(ST_MakePoint(t.longitude, t.latitude), 4326), 2056) AS geo_point_lv95,
    t.longitude,
    t.latitude
from (
    select
        m.cid,
        m.rank_absolute,
        m.address_info,
        m.keyword,
        m.address,
        m.title,
        m.domain,
        m.url,
        m.rating,
        m.hotel_rating,
        m.category,
        m.additional_categories,
        m.category_ids,
        m.work_hours,
        m.longitude,
        m.latitude,
        ROW_NUMBER() over (partition by m.cid order by m.rank_absolute, random()) as row_num
    from
        google_maps_dev.google_map_results  m
    where 
        m.address_info->>'country_code' = 'CH'
) t
where 
    t.row_num = 1;

   
--check duplication 
select
	cid,
	count(*)
from
	google_maps_dev.google_map_items 
group by
	cid 
having 
	count(*) > 1; 

select 
	*
from 
	google_maps_dev.google_map_items
where 
	category like ('%Restaurant%')
	and 
	category like ('%Hotel%');

select 
	*
from 
	google_maps_dev.google_map_items 
where 
	lower(category) NOT LIKE '%restaurant%'
	and 
	lower(category) NOT LIKE '%hotel%'
	and 
	lower(category) NOT LIKE '%supermarket%'
;

select 
	*
from
	google_maps_dev.google_map_items
where 
	--lower(category) like '%resautant%'
	--and 
	--lower(category) LIKE '%hotel%'
	--and
	title  = '%Restaurant Heuberge%'
;



--////////////////////////////////////Resturant und Hotels(GOOGLE MAP)/////////////////////////////////////

    

--///////////////////////// Restaurant & Hotels intersect (AFO POIS & GOOGLE MAP)///////////////////////
-- GOOGLE MAP: 62605 
-- AFO POIS: 26413
-- INTERSECT: 28122
--/////////////////////////////////////////////////////////////////////////////////////////////////////
drop table if exists google_maps_dev.restaurant_hotel_intersect;
create table google_maps_dev.restaurant_hotel_intersect
as
select 
	t1.keyword,
	t1.cid,
	t2.poi_id,
	t1.plz4,
	t1.ort,
	t2.poi_typ_id,
	t1.address,
	t2.adresse,
	t1.title,
	t2.company,
	t2.company_group,
	t1.main_category,
	t1.add_category1,
	t1.add_category2,
	t1.geo_point_lv95 as google_map,
	t2.geo_point_lv95 as afo_point
from 
	google_maps_dev.restaurant_hotels_category t1
join(
	select
		*
	from
		geo_afo_prod.mv_lay_poi_aktuell
	where
		poi_typ_id in (201, 901, 902, 903, 904, 1001, 1002, 1003, 1004, 1005, 1006)
	) t2
on
	ST_intersects(ST_Buffer(t1.geo_point_lv95, 10), ST_Buffer(t2.geo_point_lv95, 10))
	and
    t1.plz4::text ~ '^[0-9]+$' --regular expression ~ '^[0-9]+$' ensures that only numeric values are considered.
	and
    t2.plz4::text ~ '^[0-9]+$'
    and
	t1.plz4::integer = t2.plz4::integer
;


select 
	*
from 
	geo_afo_prod.mv_lay_poi_aktuell; 


select 
	cid,
	count(*)
from
	google_maps_dev.restaurant_hotel_intersect
group by
	cid
having 
	count(*) > 1
;

select 
	poi_id,
	count(*)
from
	google_maps_dev.restaurant_hotel_intersect
group by
	poi_id 
having 
	count(*) > 1
;


select
	distinct cid, 
	poi_id,
	title,
	company,
	address,
	adresse,
	google_map,
	afo_point,
	company_group, 
	main_category,
	add_category1,
	add_category2, 
	ROW_NUMBER() over (partition by cid  order by poi_id, random()) as row_num
from 
	google_maps_dev.restaurant_hotel_intersect;
--where 
	--google_map = afo_point; 












