--==================================
-- Original results from DataForSEO
--==================================
 
-- count = 583,489 lots of duplication because of different "rank_group" and "rank_absolute"
select 
 	*
 from 
 	google_maps_dev.google_map_results
 where 
	address_info->>'zip' = '7246'
 ;

select 
	*
from 
	google_maps_dev.google_map_metadata 
;


--=================================================
-- google map data filtered with min(rank_absolute)
--=================================================
drop table if exists google_maps_dev.google_map_items;
create table google_maps_dev.google_map_items
as
select
    t.cid,
    t.rank_absolute,
    t.keyword,
    t.exact_match,
    t.address_info->>'zip' as plz4,
    t.address_info->>'city' as ort,
    t.address_info->> 'address' as strasse,
    t.address_info->>'country_code' as country_code,
    t.address,
    t.title,
    t.phone,
    t.domain,
    t.url,
    t.rating,
    t.total_photos,
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
        m.exact_match,
        m.address,
        m.title,
        m.phone,
        m.domain,
        m.url,
        m.rating,
        m.hotel_rating,
        m.total_photos,
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
    t.row_num = 1
;




-- check the final items table   
select 
	*
from 
	google_maps_dev.google_map_items
;

--check duplication 
select
	cid,
	count(*)
from
	google_maps_dev.google_map_items 
group by
	cid 
having 
	count(*) > 1
;


--======================================
-- Filter only for Hotel and Restaurant
--======================================

--create temp table to filtter google_items and make it only for Resuaurants and hotels 
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

--======================================================================
-- Unfold the category_id jsonb file to get more insghts on the category
--======================================================================

-- create table to unfold the categories and be able to choose the most relevant categories to Restaurants and hotels
drop table if exists 
	google_maps_dev.google_restaurant_hotel_voll
;

create table 
	google_maps_dev.google_restaurant_hotel_voll
as
select 
	cid,
	split_part(keyword,' ',1) as keyword,
	jsonb_array_elements_text(category_ids::jsonb) as category,
	plz4,
	ort,
	strasse,
	address,
	title,
	domain,
	url,
	geo_point_lv95,
	longitude,
	latitude 
from 
	tmp_gogl_restaurant_hotel
where 
	jsonb_typeof(category_ids::jsonb) = 'array'
;


--check the table
-- this table contains lots of duplication because of unfolded category_ids
select
	*
from 
	google_maps_dev.google_restaurant_hotel_voll
;

--==============================================================
-- final table from google maps filtered with chossen category from Peter 
-- vollständige Tabelle von google mit category filter von Peter
--==============================================================
-->> count 
drop table if exists 
	google_maps_dev.google_map_restaurant_hotels
;

create table 
	google_maps_dev.google_map_restaurant_hotels
as
select distinct
	cid,
	plz4,
	ort,
	strasse,
	address,
	title,
	domain,
	url,
	geo_point_lv95,
	longitude,
	latitude
from(
		select 
			*
		from 
			google_maps_dev.google_restaurant_hotel_voll 
		where 
			category in ( 'hotel'                   --hotel
						 ,'wellness_hotel'			--hotels	
						 ,'bed_and_breakfast'		--hotels
						 ,'resort_hotel'			--hotels
						 ,'youth_hostel'			--hotels
						 ,'hostel'					--hotels
						 ,'guest_house'				--hotels
		                 ,'restaurant'				--Restaurants
						 ,'swiss_restaurant'
						 ,'italian_restaurant'
						 ,'pizza_restaurant'
						 ,'fast_food_restaurant'
						 ,'indian_restaurant'
						 ,'thai_restaurant'
						 ,'bar'
						 ,'cafe'
						 ,'bistro'
						 ,'hamburger_restaurant'
						 ,'asian_restaurant'
						 ,'sushi_restaurant'
						 ,'vegetarian_restaurant'
						 ,'chinese_restaurant'
						 ,'turkish_restaurant'
						 ,'snack_bar'
						 ,'coffee_shop'
						 ,'steak_house'
						 ,'japanese_restaurant'
						 ,'european_restaurant'
						 ,'mediterranean_restaurant'
						 ,'bar_and_grill'
						 ,'lebanese_restaurant'
						 ,'vietnamese_restaurant'
						 ,'mexican_restaurant'
						 ,'american_restaurant'
						 ,'restaurant_brasserie'
						 ,'french_restaurant'
						 ,'brunch_restaurant'
						 ,'fine_dining_restaurant'
						 ,'fondue_restaurant'
						 ,'fusion_restaurant'
						 ,'vegan_restaurant'
						 ,'meat_restaurant'
						 ,'ethiopian_restaurant'
						 ,'haute_french_restaurant'
						 ,'persian_restaurant'
						 ,'doner_kebab_restaurant'
						 ,'asian_fusion_restaurant'
						 ,'chicken_restaurant'
						 ,'tapas_restaurant'
						 ,'barbecue_restaurant'
						 ,'spanish_restaurant'
						 ,'portuguese_restaurant'
						 ,'family_restaurant'
						 ,'sundae_restaurant'
						 ,'greek_restaurant'
						 ,'wine_bar'
						 ,'espresso_bar'
						 ,'tapas_bar'
						 ,'moroccan_restaurant'
						 ,'sri_lankan_restaurant'
						 ,'dominican_restaurant'
						 ,'poke_bar'
						 ,'syrian_restaurant'
						 ,'hookah_bar'
						 ,'seafood_restaurant'
						 ,'tibetan_restaurant'
						 ,'taco_restaurant'
						 ,'irish_pub'
						 ,'halal_restaurant'
						 ,'mandarin_restaurant'
						 ,'modern_european_restaurant'
						 ,'breakfast_restaurant'
						 ,'mountain_hut'
						 ,'health_food_restaurant' 			--Restaurants
						 ,'bed_and_breakfast'
						 ,'hotel'
						 ,'guest_house'
						 ,'wellness_hotel'
						 ,'inn'
						 ,'resort_hotel'
						 ,'hostel'
						 ,'extended_stay_hotel'
						 ,'motel'
						 ,'youth_hostel'
						 ,'restaurant'
						 ,'bar'
						 ,'cafe'
						 ,'pizza_restaurant'
						 ,'swiss_restaurant'
						 ,'italian_restaurant'
						 ,'fast_food_restaurant'
						 ,'coffee_shop'
						 ,'hamburger_restaurant'
						 ,'bistro'
						 ,'vegetarian_restaurant'
						 ,'asian_restaurant'
						 ,'breakfast_restaurant'
						 ,'snack_bar'
						 ,'bar_and_grill'
						 ,'buffet_restaurant'
						 ,'wine_bar'
						 ,'european_restaurant'
						 ,'cocktail_bar'
						 ,'meat_restaurant'
						 ,'brunch_restaurant'
						 ,'mediterranean_restaurant'
						 ,'vegan_restaurant'
						 ,'thai_restaurant'
						 ,'sushi_restaurant'
						 ,'family_restaurant'
						 ,'barbecue_restaurant'
						 ,'japanese_restaurant'
						 ,'fondue_restaurant'
						 ,'steak_house'
						 ,'lunch_restaurant'
						 ,'pub'
						 ,'turkish_restaurant'
						 ,'halal_restaurant'
						 ,'french_restaurant'
						 ,'tea_house'
						 ,'restaurant_brasserie'
						 ,'fine_dining_restaurant'
						 ,'cafeteria'
						 ,'espresso_bar'
						 ,'american_restaurant'
			)
		) t 
;



-- Table for Simon to compare with our afo_pois
-- the cid is unique and there is no duplication
-- >> 37,238
select 
	*
from 
	google_maps_dev.google_map_restaurant_hotels
;

--check duplication 
select
	cid,
	count(*)
from
	google_maps_dev.google_map_restaurant_hotels
group by
	cid 
having 
	count(*) > 1
;

--=============================================
-- Test tables for the missings results after the comparison
--=============================================

--(I)- searching in table that is filtered with chossen categories
select 
	*
from 
	google_maps_dev.google_map_restaurant_hotels
where 
	address  = 'St. Gallerstrasse 22, 9200 Gossau'
	--title like ('%Alpenrösli%')
	--address = '7246 Partnun'
;


--(II)- searching in table that have all categories
select 
	*
from 
	tmp_gogl_restaurant_hotel
where 
	title like ('%Alpenrösli%')
	--address = 'Rue du Port-Franc 11, 1003 Lausanne'
;


--(III)- searching in table with unfolded category_ids
select 
	*
from 
	google_maps_dev.google_restaurant_hotel_voll
where 
	title like ('%Alpenrösli%')
	--plz4  = '7246'
;



--====================================================
-- Poi_typ SuperMarket: Abgleich test mit Google Data
--====================================================
drop table if exists google_maps_dev_abgleich.afo_poi_typ_food;
create table google_maps_dev_abgleich.afo_poi_typ_food
as
select 
	*
from 
	geo_afo_prod.mv_lay_poi_aktuell
where 
	poi_typ = 'Food'
;

select 
	*
from 
	google_maps_dev_abgleich.afo_poi_typ_food
;	



























