--=====================================
-- 6330 Cham (Ohne Definerte Kategorie)
--=====================================
drop table if exists google_maps_dev_abgleich.google_map_results_gasto_cham;
create table google_maps_dev_abgleich.google_map_results_gasto_cham
as
select 
	*
from 
	google_maps_dev_abgleich.google_map_results_restau_und_hotel
where 
	address like '%6330%'
	or
	address like '%6332%'
;

--test
 select
	cid,
	count(*)
from
	google_maps_dev_abgleich.google_map_results_gasto_cham
group by
	cid 
having 
	count(*) > 1;


-- get rid of duplication 
drop table if exists google_maps_dev_abgleich.google_map_items_gasto_cham;
create table google_maps_dev_abgleich.google_map_items_gasto_cham
as
select
   t.*
from (
    select
        m.*
        ,ROW_NUMBER() over (partition by m.cid order by m.rank_absolute, random()) as row_num
    from
        google_maps_dev_abgleich.google_map_results_restau_und_hotel  m
    where 
        address like '%6330%'
        or 
        address like '%6332%'
) t
where 
    t.row_num = 1;
   
   
   
   
-- cham mit unserem definierte Kategorie 
drop table if exists google_maps_dev_abgleich.google_map_categ_cham;
create table  google_maps_dev_abgleich.google_map_categ_cham
as
select distinct 
	type
	,rank_group 
	,rank_absolute 
	,domain 
	,title 
	,url 
	,contact_url 
	,contributor_url 
	,rating 
	,hotel_rating 
	,price_level 
	,rating_distribution 
	,snippet 
	,address 
	,address_info 
	,place_id 
	,phone 
	,main_image 
	,total_photos 
	,category 
	,additional_categories
	,category_ids
	,work_hours
	,feature_id
	,cid
	,latitude
	,longitude
	,is_claimed
	,local_justifications
	,row_num
from(
	select 
		*,
		jsonb_array_elements_text(category_ids::jsonb) as def_category 
	from 
		google_maps_dev_abgleich.google_map_items_gasto_cham
) t
where 
	def_category in ( 'hotel'                   --hotel
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
;

-- test
 select
	cid,
	count(*)
from
	google_maps_dev_abgleich.google_map_items_gasto_cham
group by
	cid 
having 
	count(*) > 1;

---------------------
-- Tabelle für cham 
---------------------
--(I) Alle Results
select 
	*
from 
	google_maps_dev_abgleich.google_map_results_gasto_cham
where 
	cid = '8313496031103165669'
;

--(II) eindeutig cid

alter table 
	google_maps_dev_abgleich.google_map_items_gasto_cham
add column geo_point_lv95 geometry
;

update 
	google_maps_dev_abgleich.google_map_items_gasto_cham
set 
	geo_point_lv95 = ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), 2056)
;

select 
	cid
	,title 
	,address 
	,address_info 
	,keyword
	,geo_point_lv95
from 
	google_maps_dev_abgleich.google_map_items_gasto_cham
where 
	address like '%6332%'
;






delete from google_maps_dev_abgleich.google_map_items_gasto_cham
where  
	category in ('Kiosk')
;

select 
	*
from 
	google_maps_dev_abgleich.google_map_items_gasto_cham
;


--(III) defined category
select 
	*
from 
	google_maps_dev_abgleich.google_map_categ_cham
where 
	category in ('Kiosk'
				,'Petrol Station'
				,'Sports complex'
				,'Take Away'
				,'Wholesaler'
				,'Pizza Delivery'
	)
;



--(V) Afo POI 
select 
	*
from 
	geo_afo_prod.mv_lay_poi_aktuell
where 
	plz4 = 6330
	and 
	poi_typ_id in (1001, 1002, 1003, 1004, 1005, 1006, 901, 902, 903, 904)
	--poi_typ in ('Restaurant', 'Café', 'Bar', 'Hotel')
;







create temp tab








