--=================================
-- Hotel und Gastro POI (bereingen)
--=================================

----------------------------------------------------
-- Dataset Tabelle für Hotel und Gastro (vorbreiten)
----------------------------------------------------

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

select 
	*
from 
	google_maps_dev.google_map_results
where 
	cid = '17900603087584000000'
;


alter table  
       tmp_gogl_restaurant_hotel
add column  
       category_ids_de JSONB
;

--category_ids_de aktualisieren:
update  
    tmp_gogl_restaurant_hotel
set  
    category_ids_de = case 
                        when additional_categories is null  
                             or  
                             additional_categories::text = 'null'
                        then
                        	to_jsonb(array[category])
                        when
                        	jsonb_typeof(additional_categories::jsonb) = 'array'
                        then
                        	jsonb_insert(additional_categories::jsonb, '{0}', to_jsonb(category))
                        else      
                  			to_jsonb(array[category] || additional_categories::text)
                      end 
;

--======================================================================
-- Unfold the category_id jsonb file to get more insghts on the category
--======================================================================

-- create table to unfold the categories and be able to choose the most relevant categories to Restaurants and hotels
drop table if exists 
	google_maps_dev.google_map_hotel_und_gastro
;

create table 
	google_maps_dev.google_map_hotel_und_gastro
as
select 
	cid,
	exact_match,
	keyword as kw_long,
	split_part(keyword,' ',1) as keyword,
	jsonb_array_elements_text(category_ids_de::jsonb) as category_ids_de,
	jsonb_array_elements_text(category_ids::jsonb) as category_ids_en,
	title,
	address,
	strasse,
	plz4,
	ort,
	domain,
	url,
	phone,
	total_photos as anz_fotos,
	rating->>'value' as google_bewertung,
	rating->>'votes_count' as anz_bewertungen,
	work_hours,
	--jsonb_array_elements_text(work_hours::jsonb) as work_hours,
	work_hours->>'current_status' as status,
	geo_point_lv95,
	longitude,
	latitude 
from 
	tmp_gogl_restaurant_hotel
where 
	jsonb_typeof(category_ids::jsonb) = 'array'
;



select 
	*
from 
	google_maps_dev.google_map_hotel_und_gastro
where
  cid = '7552736995962150000'
;



--========================================================
-- create a temp table for Hotel and Restuarant 
-- filitered by the following category from Peter
--========================================================
drop table if exists tmp_filitered_hotel_gastro;
create temp table tmp_filitered_hotel_gastro
as
select 
	*
from 
	google_maps_dev.google_map_hotel_und_gastro
where 
	category_ids_en in ( 
						'hotel','wellness_hotel','bed_and_breakfast','resort_hotel','youth_hostel'			
						 ,'hostel','guest_house','restaurant','swiss_restaurant','italian_restaurant','pizza_restaurant'
						 ,'fast_food_restaurant','indian_restaurant','thai_restaurant','bar','cafe','bistro','hamburger_restaurant'
						 ,'asian_restaurant','sushi_restaurant','vegetarian_restaurant','chinese_restaurant','turkish_restaurant'
						 ,'snack_bar','coffee_shop','steak_house','japanese_restaurant','european_restaurant','mediterranean_restaurant'
						 ,'bar_and_grill','lebanese_restaurant','vietnamese_restaurant','mexican_restaurant','american_restaurant'
						 ,'restaurant_brasserie','french_restaurant','brunch_restaurant','fine_dining_restaurant','fondue_restaurant'
						 ,'fusion_restaurant','vegan_restaurant' ,'meat_restaurant','ethiopian_restaurant','haute_french_restaurant'
						 ,'persian_restaurant','doner_kebab_restaurant','asian_fusion_restaurant','chicken_restaurant','tapas_restaurant'
						 ,'barbecue_restaurant' ,'spanish_restaurant','portuguese_restaurant','family_restaurant','sundae_restaurant'
						 ,'greek_restaurant','wine_bar','espresso_bar','tapas_bar','moroccan_restaurant','sri_lankan_restaurant'
						 ,'dominican_restaurant','poke_bar','syrian_restaurant','hookah_bar','seafood_restaurant','tibetan_restaurant'
						 ,'taco_restaurant','irish_pub','halal_restaurant','mandarin_restaurant','modern_european_restaurant','breakfast_restaurant'
						 ,'mountain_hut','health_food_restaurant','bed_and_breakfast','hotel','guest_house','wellness_hotel','inn','resort_hotel'
						 ,'hostel','extended_stay_hotel','motel','youth_hostel','restaurant','bar','cafe','pizza_restaurant','swiss_restaurant','italian_restaurant'
						 ,'fast_food_restaurant','coffee_shop','hamburger_restaurant','bistro','vegetarian_restaurant','asian_restaurant'
						 ,'breakfast_restaurant','snack_bar','bar_and_grill','buffet_restaurant','wine_bar','european_restaurant'
						 ,'cocktail_bar','meat_restaurant','brunch_restaurant','mediterranean_restaurant','vegan_restaurant','thai_restaurant'
						 ,'sushi_restaurant','family_restaurant','barbecue_restaurant','japanese_restaurant','fondue_restaurant','steak_house'
						 ,'lunch_restaurant','pub','turkish_restaurant','halal_restaurant','french_restaurant','tea_house'
						 ,'restaurant_brasserie','fine_dining_restaurant','cafeteria','espresso_bar','american_restaurant'
	)
;


--==========================================
-- create a temp table for category mapping
--==========================================
drop table tmp_category_mapping;
create temp table tmp_category_mapping
as
select 
	*
from 
	google_maps_dev.google_map_category_hierarchy
where 
	hauptkategorie_neu = 'Hotel & Gastronomie'
;


-----------------
-- Daten sichten
-----------------
select 
	*
from 
	google_maps_dev.google_map_hotel_und_gastro
;



select 
	*
from 
	tmp_category_mapping
where 
	category_en like '%hamburger%'
;
	

select 
	*
from 
	tmp_filitered_hotel_gastro
where 
	cid = '13878000709797810582'
;
	
--===============================================
-- Step(1):Use the Mapping Table to Create New POI Categories
-- step(2): 
--===============================================
drop table if exists google_maps_dev.googl_map_hotel_gastro;
create table google_maps_dev.googl_map_hotel_gastro
as
select
	t0.cid
	,t0.exact_match
	,t0.kw_long
	,t0.keyword
	--,t1.hauptkategorie_neu
	--,t0.category_ids_en
	--,t0.category_ids_de
	,STRING_AGG(DISTINCT t1.hauptkategorie_neu, ' | ') as mapped_hauptkategorie
	,STRING_AGG(DISTINCT t1.poi_typ_neu, ' | ') as mapped_poi_typ
	,STRING_AGG(DISTINCT t1.kategorie_neu, ' | ') as mapped_category
	,STRING_AGG(DISTINCT t0.category_ids_en, ' | ') as category_ids_en
	,STRING_AGG(DISTINCT t0.category_ids_de, ' | ') as category_ids_de
	,t0.title
	,t0.address
	,t0.strasse as strasse_h_no
	,REGEXP_REPLACE(t0.strasse, '[0-9]+[A-Za-z]*$', '') as strasse
    ,(REGEXP_MATCH(t0.strasse, '[0-9]+[A-Za-z]*$'))[1] as hausnummer
	,t0.plz4
	,t0.ort
	,t0."domain"
	,t0.url
	,t0.phone
	,t0.anz_fotos
	,t0.google_bewertung
	,t0.anz_bewertungen
	,t0.work_hours
	,t0.status
	,t0.geo_point_lv95
	,t0.longitude
	,t0.latitude
from 
	tmp_filitered_hotel_gastro t0
left join
	tmp_category_mapping t1
on
	t0.category_ids_en = t1.category_en
group by 
	t0.cid
	,t0.title
	,t0.address
	,t0.strasse
	,t0.plz4
	,t0.ort
	,t0."domain"
	,t0.url
	,t0.phone
	,t0.work_hours
	,t0.status
	,t0.geo_point_lv95
	,t0.longitude
	,t0.latitude
	,t0.exact_match
	,t0.kw_long
	,t0.keyword
	,t0.google_bewertung
	,t0.anz_bewertungen
	,t0.anz_fotos
	--,t1.hauptkategorie_neu
;

select 
	*
from 
	google_maps_dev.googl_map_hotel_gastro
where 
	cid = '13878000709797810582'
;

--==================================
-- step(3) Working_hours (Reformat)
--==================================
alter table google_maps_dev.googl_map_hotel_gastro
add column if not exists status_neu text,
add column if not exists opening_times text
;

alter table google_maps_dev.googl_map_hotel_gastro
drop column opening_times
;

update google_maps_dev.googl_map_hotel_gastro
set 
	status = case  -- Determine the status of the POI
        -- If no valid opening hours and current_status is 'permanently_closed', set 'Permanently Closed'
        when
        	work_hours->>'current_status' = 'permanently_closed' 
	        then
	        	'Permanently Closed'
        when
        	work_hours->>'current_status' = 'temporarily_closed' 
        	then
        		'Temporarily Closed'
        -- If there are valid opening hours, set as 'Open'
        when (
            select
            	COUNT(*)
            from
            	jsonb_each(work_hours->'timetable') as days(day, schedules)
            where
            	jsonb_typeof(schedules) = 'array'
                  and exists (
                      select 
                      	1
                      from
                      	jsonb_array_elements(schedules) as schedule
                      where
                      	schedule->'open' is not null
                      	and
                      	schedule->'close' is not null 
                  )
                and 
    			jsonb_typeof(work_hours->'timetable') = 'object'
        ) > 0 
        	then 'Open'
        -- Otherwise, set as 'Closed'
        else 'Closed'
    end
;

update google_maps_dev.googl_map_hotel_gastro
set  
    opening_times = case 
        -- If there are valid opening hours, extract them
        when (
            select 
                COUNT(*)
            from 
                jsonb_each(work_hours->'timetable') as days(day, schedules)
            where  
                jsonb_typeof(schedules) = 'array' 
                and exists (
                    select 
                        1
                    from 
                        jsonb_array_elements(schedules) as schedule
                    where 
                        schedule->'open' is not null 
                        and schedule->'close' is not null  
                )
                and jsonb_typeof(work_hours->'timetable') = 'object'
        ) > 0 then (
            select 
                STRING_AGG(
                    CONCAT(
                        -- Day abbreviations
                        case  
                            when day = 'monday' then 'MO'
                            when day = 'tuesday' then 'DI'
                            when day = 'wednesday' then 'MI'
                            when day = 'thursday' then 'DO'
                            when day = 'friday' then 'FR'
                            when day = 'saturday' then 'SA'
                            when day = 'sunday' then 'SO'
                        end, ': ',
                        day_times.times
                    ), ' | ' -- Concatenate days with ' | '
                )
            from (
                select  
                    day, 
                    STRING_AGG(
                        CONCAT(
                            LPAD(schedule->'open'->>'hour', 2, '0'), ':',
                            LPAD(
                                case  
                                    when schedule->'open'->>'minute' = '0' then '00'
                                    else schedule->'open'->>'minute'
                                end, 2, '0'
                            ), ' - ',
                            LPAD(schedule->'close'->>'hour', 2, '0'), ':',
                            LPAD(
                                case  
                                    when schedule->'close'->>'minute' = '0' then '00'
                                    else schedule->'close'->>'minute'
                                end, 2, '0'
                            )
                        ), ' & ' -- Concatenate multiple time slots for the same day with ' / '
                    ) as times
                from 
                    jsonb_each(work_hours->'timetable') as days(day, schedules),
                    lateral jsonb_array_elements(schedules) as schedule
                where 
                    jsonb_typeof(schedules) = 'array' 
                    and schedule->'open' is not null   
                    and schedule->'close' is not null  
                    and jsonb_typeof(work_hours->'timetable') = 'object'
                group by day -- Group by day to merge multiple time ranges for each day
            ) as day_times
        )
        -- If no valid opening hours and status is 'closed_forever' or 'temporarily_closed', set 'Closed'
        when 
            work_hours->>'current_status' IN ('closed_forever', 'temporarily_closed', 'Permanently Closed', 'Temporarily Closed') 
        then  
            work_hours->>'current_status'
        -- If no valid opening hours and status is not 'closed_forever' or 'temporarily_closed', set 'NA'
        else  
            'N/A'
    end;


select 
 count(*)
from 
	google_maps_dev.googl_map_hotel_gastro
where 
	opening_times = 'NA'
;
    
    

select
	cid
	,count(*)
from 
	google_maps_dev.googl_map_hotel_gastro
group by
	cid
having 
	count(*) > 1
;

select 
	count(*)
from 
	google_maps_dev.googl_map_hotel_gastro
where 
	opening_times = 'Closed'
; -- 14778




--==================================
-- table for Peter: Hotel und Gastro
--=================================
drop table if exists google_maps_dev.google_hotel_und_gastronomie_neu;
create table google_maps_dev.google_hotel_und_gastronomie_neu
as
select
	cid
	,title as bezeichnung
	,'[' || array_to_string(array_agg(category_ids_en), '/') || ']' as category_ids_en
	,'[' || array_to_string(array_agg(category_ids_de), '/') || ']' as category_ids_de
	,'[' || array_to_string(array_agg(mapped_poi_typ), '/') || ']' as poi_typ
	,'[' || array_to_string(array_agg(mapped_category), '/') || ']' as kategorie
	,mapped_hauptkategorie as hauptkategorie
	,phone as telefon
	,address as adresse
	,strasse
	,hausnummer
	,plz4
	,ort
	,url
	,domain
	,anz_fotos
	,google_bewertung
	,anz_bewertungen
	,exact_match
	,status
	,opening_times
	--,'[' || array_to_string(array_agg(opening_times), ',') || ']' as opening_times
	,geo_point_lv95
from
	google_maps_dev.googl_map_hotel_gastro
group by
	cid
	,title 
	,phone 
	,address 
	,strasse
	,hausnummer
	,plz4
	,ort
	,url
	,domain
	,google_bewertung
	,anz_bewertungen
	,status
	,exact_match
	,anz_fotos
	,geo_point_lv95
	,mapped_hauptkategorie
	,opening_times
;


/*alter table 
	google_maps_dev.google_hotel_und_gastronomie_neu
add column poi_typ text
;

update 
	google_maps_dev.google_hotel_und_gastronomie_neu
set 
	poi_typ = (
				select
					'[' || array_to_string(array_agg(mapped_poi_typ), '/') || ']'
				from
					google_maps_dev.googl_map_hotel_gastro
				group by
					cid
	)
	kategorie_ids = '[' || array_to_string(array_agg(category_ids), ',') || ']'
;
*/


-- get plz4 from address if exists
select 
	*
from
	google_maps_dev.google_hotel_und_gastronomie_neu
where 
	plz4 NOT SIMILAR TO '[0-9]{4}'
;

update google_maps_dev.google_hotel_und_gastronomie_neu
set 
	plz4 = regexp_replace(adresse, '([^0-9]*)([0-9]{4,6})([^0-9]*)', '\2')
where 
	plz4 is null
;

/*
-- get hausenummer from address if exists
select  
    adresse,
    -- Extract street name: everything up to the first occurrence of a number or comma, but excluding any numeric building number or postal code.
   	TRIM(REGEXP_REPLACE(adresse, '^([^,0-9]+).*', '\1')) as strasse_neu,
    -- Extract building number: captures the first occurrence of a numeric value that follows the street name.
    TRIM(REGEXP_REPLACE(adresse, '^.*? ([0-9]+)(,| ).*$', '\1')) as hausnummer_neu,
    -- Extract postal code (PLZ): extracts the first occurrence of a 4-digit number at or near the end of the address.
    plz4
    --TRIM(REGEXP_REPLACE(adresse, '.*[^0-9]([0-9]{4})([^0-9]|$).*', '\1')) AS plz_zip_code
from
	google_maps_dev.google_hotel_und_gastronomie_neu
where 
	hausnummer isnull  
    and
   	not adresse ~ '^[0-9]{4} [A-Za-zäöüÄÖÜß\s-]+$'
;
*/

-- get hausenummer from address if exists
select   
    adresse,
    strasse,
    -- Extract street name: everything up to the first occurrence of a number or comma, but excluding any numeric building number or postal code.
    TRIM(REGEXP_REPLACE(adresse, '^([^,0-9]+).*', '\1')) AS strasse_neu,
    -- Extract building number: captures the first occurrence of numeric value only.
    case  
        when
        	TRIM(REGEXP_REPLACE(adresse, '^.*? ([0-9]+)(,| ).*$', '\1')) = plz4 
        	then	
        		null	
        when
        	TRIM(REGEXP_REPLACE(adresse, '^.*? ([0-9]+)(,| ).*$', '\1')) ~ '^[0-9]+$' 
            then
            	TRIM(REGEXP_REPLACE(adresse, '^.*? ([0-9]+)(,| ).*$', '\1'))
        else 
        	null 
    end as hausnummer_neu,
    -- Extract postal code (PLZ): extracts the first occurrence of a 4-digit number at or near the end of the address.
    plz4
from 
    google_maps_dev.google_hotel_und_gastronomie_neu
where  
    hausnummer is null  
    and
   	not adresse ~ '^[0-9]{4} [A-Za-zäöüÄÖÜß\s-]+$'
;

   
-- update hotel und gastronomie Table 
update google_maps_dev.google_hotel_und_gastronomie_neu
set 
	hausnummer = case  
			        when
			        	TRIM(REGEXP_REPLACE(adresse, '^.*? ([0-9]+)(,| ).*$', '\1')) = plz4 
			        	then	
			        		null	
			        when
			        	TRIM(REGEXP_REPLACE(adresse, '^.*? ([0-9]+)(,| ).*$', '\1')) ~ '^[0-9]+$' 
			            then
			            	TRIM(REGEXP_REPLACE(adresse, '^.*? ([0-9]+)(,| ).*$', '\1'))
			        else 
			        	null 
			    end,
	strasse = TRIM(REGEXP_REPLACE(adresse, '^([^,0-9]+).*', '\1'))
where  
    hausnummer is null  
    and
   	not adresse ~ '^[0-9]{4} [A-Za-zäöüÄÖÜß\s-]+$'
;




----------------------------------------------------------------------------------------------------------------
-- Step(6): add PLZ6 and Gemeinde 
--							>> ST_Intersects or ST_Contains (geo_poly_lv95"PLZ6", geo_point_lv95"google_maps")
--        : fill the missing hausenummer 
--							>> ST_Intersects or ST_Contains (geo_poly_lv95"gbd", geo_point_lv95"google_maps")
----------------------------------------------------------------------------------------------------------------


geo_afo_prod.imp_gmd_geo_neu

geo_afo_prod.imp_plz6_geo_neu

geo_afo_prod.mv_lay_gbd_aktuell

select 
	*
from
	geo_afo_prod.mv_qu_gbd_gwr_aktuell
;










-------------------------------------
--intersect with plz6 or ST_Contains
--indexing auf "geo_point_lv95" 
-------------------------------------
/*
 * DROP INDEX IF EXISTS google_maps_dev.idx_google_afo_pois_items_geo_point_lv95;
DROP INDEX IF EXISTS google_maps_dev.idx_google_map_afo_pois_v1_geo_point_lv95;
DROP INDEX IF EXISTS idx_tmp_afo_poi_zürich_geo_line_lv95;
create index idx_google_afo_pois_items_geo_point_lv95 on google_maps_dev.google_afo_pois_items using GIST(geo_point_lv95);
 */


create index idx_google_hotel_und_gastronomie_neu_point_lv95 on google_maps_dev.google_hotel_und_gastronomie_neu using GIST(geo_point_lv95);
create index idx_imp_plz6_geo_neu_poly_lv95 on geo_afo_prod.imp_plz6_geo_neu using GIST(geo_poly_lv95);
create index idx_mv_lay_plz4_aktuell_poly_lv95 on geo_afo_prod.mv_lay_plz4_aktuell using GIST(geo_poly_lv95);
create index idx_imp_gmd_geo_neu_poly_lv95 on geo_afo_prod.imp_gmd_geo_neu using GIST(geo_poly_lv95);
create index idx_mv_qu_gbd_gwr_aktuell_point_lv95 on geo_afo_prod.mv_qu_gbd_gwr_aktuell using GIST(geo_point_eg_lv95);

select 
	count(*)
from(
	select 
		t0.cid
		,t0.bezeichnung 
		,t0.adresse
		,t0.strasse
		,t3.strname
		,t0.hausnummer
		,t3.deinr
		,t2.gemeinde
		,t0.plz4
		,t3.dplz4
		,t1.plz
		,t1.plz6
		,t0.geo_point_lv95 as gogl_geo
		,t3.geo_point_eg_lv95 as gbd_geo
		,t1.geo_poly_lv95 as plz6_geo
	from
		google_maps_dev.google_hotel_und_gastronomie_neu t0
	left join
		geo_afo_prod.imp_plz6_geo_neu t1
	on
		ST_Contains(t1.geo_poly_lv95 , t0.geo_point_lv95)
	left join 
		geo_afo_prod.imp_gmd_geo_neu t2
	on
		ST_Contains(t2.geo_poly_lv95 , t0.geo_point_lv95)
	left join 
		geo_afo_prod.mv_qu_gbd_gwr_aktuell t3
	on
		ST_DWithin(t3.geo_point_eg_lv95, t0.geo_point_lv95, 20)
	--where 
		--t0.hausnummer is null
	limit 100
	where 
		t0.plz4 <> t1.plz::text
) s
;
--===================================
-- correct PLZ4 
--===================================
-- fix missing "plz4" and add "plz6"
-- n_count >> 76
-- Descution about the plz4 that is not the same
select 
	t0.*
	,t1.plz
from
		google_maps_dev.google_hotel_und_gastronomie_neu t0
	left join
		geo_afo_prod.imp_plz6_geo_neu t1
	on
		ST_Contains(t1.geo_poly_lv95 , t0.geo_point_lv95)
where 
	t0.plz4 NOT SIMILAR TO '[0-9]{4}'
	or 
	t0.plz4 is null
	or 
	t0.plz4 <> t1.plz::text
;


update google_maps_dev.google_hotel_und_gastronomie_neu t0
set
	plz4 = t1.plz4
from
	geo_afo_prod.mv_lay_plz4_aktuell  t1
where  
    t0.plz4 not similar to '[0-9]{4}'
    or
    t0.plz4 is null
    and
   	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;


--=====================
-- Adding PLZ6 
--=====================
alter table
	google_maps_dev.google_hotel_und_gastronomie_neu
add column 
	plz6 text
;


update
	google_maps_dev.google_hotel_und_gastronomie_neu t0
set
	plz6 = t1.plz6
from
	geo_afo_prod.imp_plz6_geo_neu t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;

--======================
-- Add Gemeinde
--======================
alter table
	google_maps_dev.google_hotel_und_gastronomie_neu
add column 
	gemeinde text,
add column
	gmd_nr numeric
;

update
	google_maps_dev.google_hotel_und_gastronomie_neu t0
set
	gemeinde = t1.gemeinde
	,gmd_nr = t1.gmd_nr
from
	geo_afo_prod.imp_gmd_geo_neu t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;

--test
select 
	*
from
	google_maps_dev.google_hotel_und_gastronomie_neu
where
	adresse NOT ILIKE '%' || gemeinde || '%'
; 
	

--===========================
-- define Relevanz formula
--==========================
-- Add the new 'relevant' column to the table
alter table 
	google_maps_dev.google_hotel_und_gastronomie_neu
add column 
	relevant numeric
;

-- Update the 'relevant' column based on the specified conditions
update 
	google_maps_dev.google_map_hotel_gastronomie
set 
	relevant =
    	-- Total score calculation
    		(
			    -- anz_fotos scoring
			    case  
			        when 
			        	anz_fotos is not null or anz_fotos > 0 
			        then 
			        	anz_fotos * 1
			        else 
			        	0
			    end 
			    +
			    -- anz_bewertungen scoring
			    case  
			        when
			        	anz_bewertungen is not null or anz_bewertungen::numeric > 0 
			        then 
			        	anz_bewertungen::numeric * 10 
			        else
			        	0
			    end 
			    +
			    -- telefon scoring
			    case  
			        when
			        	telefon is null 
			        then  
			        	0
			        else
			        	50
			    end 
			    +
			    -- domain scoring
			    case  
			        when 
			        	domain is null
			        then
			        	0
			        else
			        	50
			    end
			    +
			    -- strasse
			    case  
			        when 
			        	google_strasse is null
			        then
			        	0
			        else
			        	50
			    end
			    +
			    -- Hausnummer
			    case  
			        when 
			        	google_hausnum is null
			        then
			        	0
			        else
			        	25
			    end
			     +
			    -- opening_times
			    case  
			        when 
			        	opening_times = 'N/A'
			        then
			        	0
			        else
			        	25
			    end
	)
 ;


   

select 
	*
from 
	google_maps_dev.google_hotel_und_gastronomie_neu
where 
	plz4 = '4123'
;

--===========================
-- fix missing hausnummer  
--===========================

create index idx_google_hotel_und_gastronomie_neu_poly_lv95 on google_maps_dev.google_hotel_und_gastronomie_neu using GIST(geo_point_lv95);
create index idx_mv_qu_gbd_gwr_aktuell_point_lv95 on geo_afo_prod.mv_qu_gbd_gwr_aktuell using GIST(geo_point_eg_lv95);
create index idx_mv_lay_gbd_aktuell_point_lv95 on geo_afo_prod.mv_lay_gbd_aktuell using GIST(geo_point_eg_lv95);
create index idx_mv_lay_gbd_aktuell_poly_lv95 on geo_afo_prod.mv_lay_gbd_aktuell using GIST(geo_poly_lv95);
--================================
-- Final Table according to Peter
--================================
drop table if exists google_maps_dev.google_map_hotel_gastronomie;

--google_maps_dev.google_map_hotel_gastronomie >> using gwr_layer
--google_maps_dev.google_map_hotel_gastronomie_v1 >> using gbd_layer
create table google_maps_dev.google_map_hotel_gastronomie
as
select 
    cid
    ,bezeichnung
    ,category_ids_en
    ,category_ids_de
    ,poi_typ
    ,kategorie
    ,hauptkategorie
    ,telefon
    ,adresse
    ,strasse as google_strasse
    ,hausnummer as google_hausnum
    ,plz4 as google_plz4
    ,ort as google_ort
    ,strname as gwr_strasse --gbd_strasse
    ,deinr as gwr_hausnum --gbd_hausnum
    ,dplz4 as gwr_plz4 --gbd_plz4
    ,dplzname as gwr_ort --gbd_ort
    ,distance
    ,url
    ,domain
    ,anz_fotos
    ,google_bewertung
    ,anz_bewertungen
    ,relevant
    ,status
    ,opening_times
    ,plz6
    ,gemeinde
    ,gmd_nr
    ,geo_point_lv95 as geo_point_google
    ,geo_point_eg_lv95 as geo_point_gwr
    --,geo_poly_lv95 as geo_poly_gbd
from (
		select  
	        t0.cid
	        ,t0.bezeichnung
	        ,t0.category_ids_en
		    ,t0.category_ids_de
		    ,t0.poi_typ
		    ,t0.kategorie
		    ,t0.hauptkategorie
		    ,t0.telefon
	        ,t0.adresse
	        ,t0.strasse
	        ,t0.hausnummer
	        ,t0.plz4
   			,t0.ort
	        ,t1.strname  --strbez2l as gbd_strasse
	        ,t1.deinr    --hnr as gbd_hausnum --
	        ,t1.dplz4    --plz4 as gbd_plz4  --
	        ,t1.dplzname --ort as gbd_ort --
	        ,t0.url
		    ,t0.domain
		    ,t0.anz_fotos
		    ,t0.google_bewertung
		    ,t0.anz_bewertungen
		    ,t0.status
		    ,t0.opening_times
		    ,t0.relevant
		    ,t0.plz6
		    ,t0.gemeinde
		    ,t0.gmd_nr
	        ,t0.geo_point_lv95
	        ,t1.geo_point_eg_lv95
	        ,t1.geo_poly_lv95
	        ,ST_Distance(t1.geo_point_eg_lv95, t0.geo_point_lv95) as distance
	        ,ROW_NUMBER() over (partition by t0.cid order by ST_Distance(t1.geo_point_eg_lv95, t0.geo_point_lv95)) as rn
	    from 
	        google_maps_dev.google_hotel_und_gastronomie_neu t0
	    left join  
	        geo_afo_prod.mv_lay_gbd_aktuell t1  -- --geo_afo_prod.mv_qu_gbd_gwr_aktuell t1
	    on 
	        ST_DWithin(t1.geo_point_eg_lv95, t0.geo_point_lv95, 20)
	        --or 
	        --ST_Contains(t1.geo_poly_lv95 , t0.geo_point_lv95)
) t
where 
    rn = 1
;

--Bsp: 355735942750278926 

select 
	cid
	,count(*)
from
	google_maps_dev.google_map_hotel_gastronomie
group by 
	cid 
having 
	count(*) > 1
;



-- missing hausnummer by google >> 3489 
select
 	*
 from 
 	google_maps_dev.google_map_hotel_gastronomie
 where 
 	google_hausnum is null
 ;

-- missing hausnummer by gwr >> 4109
select
 	*
 from 
 	google_maps_dev.google_map_hotel_gastronomie
 where 
 	gwr_hausnum is null
 ;

-- missing hausnummer by gbd >> 5590
select
 	*
 from 
 	google_maps_dev.google_map_hotel_gastronomie_v1
 where 
 	gbd_hausnum is null
 ;



select 
	count(*)
from 
	geo_afo_prod.mv_qu_gbd_gwr_aktuell
where 
	--deinr is null --103609
	--strname is null --8631
	geo_point_eg_lv95 is null --65
;



select 
	count(*)
from 
	geo_afo_prod.mv_lay_gbd_aktuell
where
	--strbezk is null --4981
	--strbezl is null --0
	hnr is null -- 6816
;



--=======================================
-- Opening Times >> cleaning
--=======================================
update
	google_maps_dev.google_map_hotel_gastronomie
set 
    status = case 
                when
                	opening_times = 'NA' then 'Not Available'
                when 
                	opening_times = 'temporarily_closed' then status
                else
                	'Open'
             end,
    opening_times = case 
                       when  
                       	opening_times = 'NA' then 'N/A'
                       else
                       	opening_times
                    end
;


-- Permanently closed 443
select 
	*
from 
	google_maps_dev.google_map_hotel_gastronomie
where 
 	cid in (
 			select 
				cid
			from 
				google_maps_dev_test.google_map_metadata_status
			where 
				current_status like 'closed_forever'
	)
;


update google_maps_dev.google_map_hotel_gastronomie
set 
	opening_times = 'permanently_closed'
where 
 	cid in (
 			select 
				ms.cid
			from 
				google_maps_dev_test.google_map_metadata_status ms
			where 
				ms.current_status like 'closed_forever'
	)
;




-- test the data in Allschwil plz4 4123
select 
	*
from 
	google_maps_dev.google_map_hotel_gastronomie
order by
	relevant desc
;

--==============================
-- fix missing categories
--==============================
-- this 853 are not defined in category mapping layer
select 
	*
from 
	google_maps_dev.google_hotel_und_gastronomie_neu
where 
	kategorie = '[]'
	and 
	kategorie_ids not in (
						select 
							category_en
						from 
							google_maps_dev.google_map_category_hierarchy
						where 
							hauptkategorie_neu = 'Hotel & Gastronomie'
	)
;


--=================================
-- Attribute to test and cleanning
--=================================

-- missing  hausnummer >> 3671 >>3495
select 
	--count(*)
	*
from
	google_maps_dev.google_map_hotel_gastronomie
where 
	gwr_strasse  isnull 
	--and 
	--strasse isnull 
;

select  
    *
from  
    geo_afo_prod.mv_qu_gbd_gwr_aktuell 
;



-- missing domain >> 9652
select 
	count(*)
from
	google_maps_dev.google_hotel_und_gastronomie_neu
where 
	domain isnull 
;


-- missing url >> 9652
select 
	count(*)
from
	google_maps_dev.google_hotel_und_gastronomie_neu
where 
	url isnull 
;



-- missing telfon >> 4257
select 
	count(*)
from
	google_maps_dev.google_hotel_und_gastronomie_neu
where 
	telefon isnull 
;


--invalid opning times >> 14,778
select 
	count(*)
from
	google_maps_dev.google_hotel_und_gastronomie_neu
where 
	opening_times = '[Closed]'
;


-- missing category mapping >>854
select 
	count(*)
from
	google_maps_dev.google_hotel_und_gastronomie_neu
where 
	kategorie = '[]'
;



--exact_match are 0 >> means the address is not containing "plz4" and "ort" in a keyword
-- exact_match 0 >> 13'288
select 
	count(*)
from
	google_maps_dev.google_hotel_und_gastronomie_neu
where 
	exact_match = '0'
;

--=========================================
-- Final Table according to Peter Feedback
-- Date: 19.10.2024
--========================================
--Tabble Hotel and Gastronomy
select
 	*
 from 
 	google_maps_dev.google_map_hotel_gastronomie
;

select 
	*
from 
	geo_afo_prod.meta_poi_categories_business_data_aktuell
;

-- point(1) missing categories
alter table 
	google_maps_dev.google_map_hotel_gastronomie
add column category_id text;



--create version 2 for adjusments
create table google_maps_dev.google_map_hotel_gastronomie_v2
as
select
	*
from 
	google_maps_dev.google_map_hotel_gastronomie
;



update
	google_maps_dev.google_map_hotel_gastronomie_v2 t1
set
	category_id = sc.category_part
from (
    select  
      t1_orig.cid,
      unnest(string_to_array(REPLACE(REPLACE(t1_orig.category_ids_en, '[', ''), ']', ''), ' | ')) as category_part
    from  
      google_maps_dev.google_map_hotel_gastronomie_v2 t1_orig
   -- where   
     -- t1_orig.poi_typ = '[]'
      --and
      --t1_orig.kategorie = '[]'
) as sc
WHERE 
	t1.cid = sc.cid
   -- and
    --t1.poi_typ = '[]'
    --and
    --t1.kategorie = '[]'
 ;




-- version 2 for adjustment 
select 
	*
from 
	google_maps_dev.google_map_hotel_gastronomie_v2
;



update 
	google_maps_dev.google_map_hotel_gastronomie_v2 t1
set 
	poi_typ = t2.poi_typ_neu,
	kategorie = t2.kategorie_neu
from 
	geo_afo_prod.meta_poi_categories_business_data_aktuell t2
where 
	t1.category_id = t2.category_id 
	--and 
	--t1.poi_typ = '[]'
	--and 
	--t1.kategorie = '[]'
;

update  
    google_maps_dev.google_map_hotel_gastronomie_v2 t1
set  
    poi_typ = sq.agg_poi_typ,
    kategorie = sq.agg_kategorie
from (
	select  
        cid,
        '[' || array_to_string(array_agg(poi_typ), '/') || ']' as agg_poi_typ,
        '[' || array_to_string(array_agg(kategorie), '/') || ']' as agg_kategorie
     from  
        google_maps_dev.google_map_hotel_gastronomie_v2
     where
    	category_id is not null
     group by  
        cid
    ) as sq
where  
    t1.cid = sq.cid
    and 
    t1.category_id is not null
 ;
   

--point(2) Opnening Hours week days format
update  
    google_maps_dev.google_map_hotel_gastronomie t0
set  
	opening_times = t1.opening_times
from 
	google_maps_dev.googl_map_hotel_gastro t1
where 
	t0.cid = t1.cid
;	
	
select 
	status
	,opening_times
from
	google_maps_dev.google_map_hotel_gastronomie
where 
	status = 'Permanently Closed'
;

update  
    google_maps_dev.google_map_hotel_gastronomie t0
set  
	opening_times = 'permanently_closed'
where 
	status = 'Permanently Closed'
;


update  
    google_maps_dev.google_map_hotel_gastronomie_v2 t0
set  
	opening_times = default 
where 
	t0.opening_times = 'N/A'
;

-- point(3) lower() case for google hausnumer letters
update  
    google_maps_dev.google_map_hotel_gastronomie
set  
    google_hausnum = regexp_replace(google_hausnum, '[A-Z]', lower(substring(google_hausnum FROM '[A-Z]')), 'g')
where  
    google_hausnum ~ '[A-Z]'
;

select 
	google_hausnum 
from
	google_maps_dev.google_map_hotel_gastronomie
where 
	 google_hausnum ~ '[A-Z]'
;

--point(4) Google plz4 und plz4 
select
	t0.korr_plz4
	,t0.adresse 
	,t0.google_plz4 
	,t0.plz
	,t0.google_ort
	,t1.ort as plz4_ort
from 
	google_maps_dev.google_map_hotel_gastronomie_v2 t0
left join
	geo_afo_prod.mv_lay_plz4_aktuell t1
on
	t0.plz = t1.plz4::text 
where 
	google_plz4 = plz
;

select 
	*
from 
	--geo_afo_prod.imp_plz6_geo_neu
	google_maps_dev.google_map_hotel_gastronomie
;

select
	t0.plz6
	,t0.plz
	,t1.plz4 
	,t1.ort
from 
	geo_afo_prod.imp_plz6_geo_neu t0
	--google_maps_dev.google_map_hotel_gastronomie t0
left join
	geo_afo_prod.mv_lay_plz4_aktuell t1 
on
	t0.plz::text = t1.plz4::text
where 
	plz6::text in (
			select 
				plz6::text
			from
				google_maps_dev.google_map_hotel_gastronomie
			where 
				google_plz4::text <> plz::text
	)
;


update google_maps_dev.google_map_hotel_gastronomie t1
set 
  plz = t0.plz
from 
	geo_afo_prod.imp_plz6_geo_neu t0
where 
	t1.plz6 = t0.plz6::text
;

alter table google_maps_dev.google_map_hotel_gastronomie
add column ort text;


update google_maps_dev.google_map_hotel_gastronomie
set ort = default;

update google_maps_dev.google_map_hotel_gastronomie_v2 t0
set 
	ort = t1.ort
from 
	geo_afo_prod.mv_lay_plz4_aktuell t1 
where
	t0.plz = t1.plz4::text
	--and 
	--t0.google_plz4 = t0.plz
;













-- point(5): GWR Hausnummer
select 
	adresse 
	,google_hausnum 
	,gwr_hausnum
	,geo_point_google 
	,geo_point_gwr 
from
	google_maps_dev.google_map_hotel_gastronomie
where 
	--gwr_hausnum ~ '[a-z]'
	--gwr_hausnum ~ '^[0-9]+\.([0-9]+)?$'
	--gwr_hausnum like '%.%'
;


select 
	*
from 
	geo_afo_prod.mv_qu_gbd_gwr_aktuell 
;

update google_maps_dev.google_map_hotel_gastronomie t0
set 
	gwr_hausnum = t1.deinr_std
from 
	geo_afo_prod.mv_qu_gbd_gwr_aktuell t1
where 
	t0.gwr_hausnum = t1.deinr 
	and 
	t0.gwr_strasse = t1.strname 
;
	
	
select
	cid
	,count(*)
from 
	google_maps_dev.google_map_hotel_gastronomie
group by
	cid 
having 
	count(*) > 1
;

--point(6): GWR data quilty and add str_name from standrad has been made by Simon
alter table google_maps_dev.google_map_hotel_gastronomie
add column gwr_strasse_std text;

update google_maps_dev.google_map_hotel_gastronomie t0
set
	gwr_strasse_std = t1.strname_std
from 
	geo_afo_prod.mv_qu_gbd_gwr_aktuell t1
where 
	t0.gwr_hausnum = t1.deinr_std  
	and 
	t0.gwr_strasse = t1.strname 
;


-- Final Table after adjustment 
create table temp.tmp_final_hotel_gastro
as
select
	cid
	,bezeichnung
	,category_ids_en
	,category_ids_de
	,poi_typ
	,kategorie
	,hauptkategorie
	,telefon
	,adresse
	,google_strasse
	,google_hausnum
	,google_plz4
	,google_ort
	,gwr_strasse
	,gwr_hausnum
	,gwr_plz4 
	,gwr_ort
	,distance
	,plz6
	,ort
	,gmd_nr
	,gemeinde
	,url
	,"domain"
	,anz_fotos
	,google_bewertung
	,anz_bewertungen
	,relevant
	,status
	,opening_times
	,geo_point_google
	,geo_point_gwr
from 
	google_maps_dev.google_map_hotel_gastronomie
order by
	relevant desc,
	anz_bewertungen desc
;
--==================================
-- test closed stauts Restaurants 
--==================================
/*
alter table 
	google_maps_dev_test.google_map_metadata_status_dienstag_früh
add column datetime date,
add column item_type text,
add column n_result numeric,
add column status text,
add column current_status text,
add column latitude text,
add column longitude text
;
 */

/*
17023673531721385264  --opening time 8am - 11pm 
6653219422418673845	  --opening time 10:30am – 1:30pm, 5–11 pm Sat/Sun 10:30am – 11pm  Tuesday Closed
14699246189702583258  --opening time 7–10am, 11:45am – 1:45pm, 5:45–9:45pm closed Sunday Monday	
15258747240190793782  --opening time 6–10pm Monday closed	
2665469912637143776   --opening time 11:30am – 11pm Sun Closed
1550301117278913504   --opening time 6.30am - 11pm closed am Monday
 */

select
	*
from 
	google_maps_dev.google_hotel_und_gastronomie_neu
where 
	cid in (
			'17023673531721385264'	
			,'6653219422418673845'	
			,'14699246189702583258'	
			,'15258747240190793782'	
			,'2665469912637143776'
			,'1550301117278913504'
	)
;
 

-- Friday 04.10.2024 8 am
select 
	*
from
	google_maps_dev_test.google_map_metadata_status_close_test
;

-- monday 07.10.2024 7 37am
select 
	*
from
	google_maps_dev_test.google_map_metadata_status_montag_morning
;

-- monsday afternoon 12:30 
select 
	*
from
	google_maps_dev_test.google_map_metadata_status_montag_mittag
;


select 
	*
from
	google_maps_dev_test.google_map_metadata_status_dienstag_früh
;



--=========================
-- Rank Absolute
--=========================
select 
	*
from 
	google_maps_dev.google_map_results 
where
	title like '%Schlössli%'
	
;


--title like '%Jugendherberge Davos%'
	--and
	address like '%Horlaubenstrasse 27, 7260 Davos%'












