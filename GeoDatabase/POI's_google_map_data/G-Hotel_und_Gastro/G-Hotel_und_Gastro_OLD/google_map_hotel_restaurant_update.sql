--=================================================
-- Abgleich DFSEO >> Restaurant and hotels (Update)
--=================================================
drop table if exists google_maps_dev_abgleich.google_map_metadata_restau_und_hotel;
select 
	*
from 
	google_maps_dev_abgleich.google_map_metadata_restau_und_hotel
;

alter table google_maps_dev_abgleich.google_map_metadata_restau_und_hotel
add column datetime date,
add column n_result numeric,
add column item_type text,
add column exact_match text
;


drop table if exists google_maps_dev_abgleich.google_map_results_restau_und_hotel;

select
	*
from 
	google_maps_dev_abgleich.google_map_results_restau_und_hotel;


--############################################
-- clean the data and get rid of duplication 
--############################################

--=================================================
-- google map data filtered with min(rank_absolute)
--=================================================
drop table if exists 
	google_maps_dev_abgleich.google_map_items_restau_und_hotel
;

create table google_maps_dev_abgleich.google_map_items_restau_und_hotel
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
    t.phone,
    t.domain,
    t.url,
    t.total_photos,
    t.rating,
    t.hotel_rating,
    t.category,
    t.additional_categories,
    t.category_ids,
    t.work_hours,
    t.work_hours->>'current_status' as current_status, 
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
        m.phone,
        m.domain,
        m.url,
        m.total_photos,
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
        google_maps_dev_abgleich.google_map_results_restau_und_hotel  m
    where 
        m.address_info->>'country_code' = 'CH'
) t
where 
    t.row_num = 1;

   
   
-- check the final items table 
-- >> 42,557
select 
	*
from 
	google_maps_dev_abgleich.google_map_items_restau_und_hotel
where 
	cid= '9998442398117096198'
;



--check duplication 
select
	cid,
	count(*)
from
	google_maps_dev_abgleich.google_map_items_restau_und_hotel 
group by
	cid 
having 
	count(*) > 1;


--======================================================================
-- Unfold the category_id jsonb file to get more insghts on the category
--======================================================================

-- create table to unfold the categories and be able to choose the most relevant categories to Restaurants and hotels
drop table if exists 
	google_maps_dev_abgleich.google_restaurant_hotel_kateg
;

create table 
	google_maps_dev_abgleich.google_restaurant_hotel_kateg
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
	phone,
	domain,
	url,
	total_photos,
	rating,
	hotel_rating,
    work_hours,
	geo_point_lv95,
	longitude,
	latitude 
from 
	google_maps_dev_abgleich.google_map_items_restau_und_hotel 
where 
	jsonb_typeof(category_ids::jsonb) = 'array'
;


-->> 85,756 nach unfolded categorie_ids
select 
	*
from 
	google_maps_dev_abgleich.google_restaurant_hotel_kateg
;



--==============================================================
-- final table from google maps filtered with chossen category from Peter 
-- vollständige Tabelle von google mit category filter von Peter
--==============================================================
-->> count 
drop table if exists 
	google_maps_dev_abgleich.google_map_restaurant_hotels_update
;

create table 
	google_maps_dev_abgleich.google_map_restaurant_hotels_update
as
select distinct
	cid,
	plz4,
	ort,
	strasse,
	address,
	title,
	phone,
	domain,
	url,
	total_photos as anz_fotos,
	hotel_rating,
	rating,
	rating->>'value' as google_bewertung,
	rating->>'votes_count' as anz_bewertungen,
	work_hours,
	work_hours->>'current_status' as status,
	geo_point_lv95,
	longitude,
	latitude
from(
		select 
			*
		from 
			google_maps_dev_abgleich.google_restaurant_hotel_kateg 
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



/*
-- Table for Simon to compare with our afo_pois
-- the cid is unique and there is no duplication
-- >> 37,427
select 
	*
from 
	google_maps_dev_abgleich.google_map_restaurant_hotels_update
where 
	cid = '9998442398117096198'
;

--check duplication 
select
	cid,
	count(*)
from
	google_maps_dev_abgleich.google_map_restaurant_hotels_update
group by
	cid 
having 
	count(*) > 1
;

*/

--=============================================
-- Abgleich: Resaurants und Hotels mit update
-- Update Datum: 16.09.2024
--=============================================
-- cid, die in die neuee version existiert und nicht in die alte
select 
	cid
	,plz4 
	,ort 
	,strasse 
	,address
	,title 
from 
	google_maps_dev_abgleich.google_map_restaurant_hotels_update 
where 
	cid not in (
                select 
                    cid 
                from 
                    google_maps_dev.google_map_restaurant_hotels
            )
   ;


-- cid, die in die alte version existiert und nicht in die neue
-- permenant geschlossen 
create temp table tmp_cid_alte_tabelle
as
select 
	cid
	,plz4 
	,ort 
	,strasse 
	,adresse
	,bezeichnung 
	,anz_fotos
	,google_bewertung
	,anz_bewertungen
	,status
from 
	google_maps_dev.google_hotel_und_gastronomie_neu
	--google_maps_dev.google_map_restaurant_hotels
where 
	cid not in (
                select 
                    cid 
                from 
                    google_maps_dev_abgleich.google_map_restaurant_hotels_update 
            )
;





--=======================================
-- Google My Business >> Business Info 
--=======================================
-- search with "cid" 
-- Google_Map_metadata_status 

alter table google_maps_dev_test.google_map_metadata_status
--add column datetime date,
--add column item_type text,
--add column n_result numeric,
--add column status text,
--add column current_status text,
add column latitude text,
add column longitude text
;



drop table if exists 
	google_maps_dev_test.google_map_metadata_status
;

UPDATE google_maps_dev_test.google_map_metadata_status
SET 
	datetime = default, 
	item_type = default,
	n_result = default,
	status = default,
	current_status = default,
	latitude = default,
	longitude  = default 
;


--================ TEST ====================

select 
	*
from 
	google_maps_dev_test.google_map_metadata_status
where 
	current_status like 'closed_forever'
;


select 
	*
from 
	google_maps_dev_test.google_map_metadata_status
where 
	current_status like 'open'
;



select 
	*
from 
	google_maps_dev_test.google_map_metadata_status
where 
	current_status like 'close'
;



--Update table
select 
	*
from 
	google_maps_dev_abgleich.google_map_restaurant_hotels_update 
where 
	cid = '15169427184683371851' --'6672726968672032158'
	--title like '%Pizza City%'
	--and
	--address = 'Hauptstrasse 82, 4853 Murgenthal'
;

select
	*
from 
	google_maps_dev_abgleich.google_map_results_restau_und_hotel
where 
	cid = '15169427184683371851' --'6672726968672032158'
	--title like '%Pizza City%'
	--and
	--address = 'Hauptstrasse 82, 4853 Murgenthal'
;


-- old table with unique cid
select
	*
from 
	google_maps_dev.google_hotel_und_gastronomie_neu
where 
	cid = '15169427184683371851' --'6672726968672032158'
;


-- old table row data 
select 
	*
	--,jsonb_array_elements(local_justifications::jsonb)->>'text' AS g_text
from
	google_maps_dev.google_map_results
where 
	--cid = '6672726968672032158'
	cid in (
			select 
				cid
			from 
				google_maps_dev_test.google_map_metadata_status
			where 
				current_status like 'open'
	)
;







--========================
-- cids to test 
-- 5661099383825114848 7925414820521942067 two different cid same restuarants differnt names 













/*
CREATE table google_maps_dev_test.google_map_status_results (
    type TEXT,
    rank_group INTEGER,
    rank_absolute INTEGER,
    position TEXT,
    title TEXT,
    description TEXT,
    category TEXT,
    category_ids JSONB,
    additional_categories JSONB,
    cid TEXT,
    feature_id TEXT,
    address TEXT,
    address_info JSONB,
    place_id TEXT,
    phone TEXT,
    url TEXT,
    contact_url TEXT,
    contributor_url TEXT,
    domain TEXT,
    logo TEXT,
    main_image TEXT,
    total_photos INTEGER,
    snippet TEXT,
    latitude FLOAT,
    longitude FLOAT,
    is_claimed BOOLEAN,
    questions_and_answers_count INTEGER,
    attributes JSONB,
    place_topics JSONB,
    rating JSONB,
    hotel_rating FLOAT,
    price_level TEXT,
    rating_distribution JSONB,
    people_also_search JSONB,
    work_time JSONB,
    popular_times JSONB,
    local_business_links JSONB,
    is_directory_item BOOLEAN,
    directory TEXT,
    keyword TEXT,
    current_status TEXT
)
;
*/                            























































--==================TEST==========================
-- id's for later 
drop table if exists google_maps_dev_test.google_map_status_metadata;

select 
	*
from 
	google_maps_dev_test.google_map_status_metadata;











