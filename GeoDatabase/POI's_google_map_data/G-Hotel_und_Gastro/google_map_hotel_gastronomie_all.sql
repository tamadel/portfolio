--=====================================
-- Hauptkategory = Hotel & Gastronomie
-- Poi_typ = alle
-- 21.11.2024
--=====================================

update 
	geo_afo_prod.meta_poi_google_maps_category
set next_run_date  = current_date  
where 
	hauptkategorie_neu  = 'Hotel & Gastronomie'
;


select
	*
from 
	geo_afo_prod.meta_poi_google_maps_category
where
	hauptkategorie_neu  = 'Hotel & Gastronomie'
	and 
	kategorie_neu in (
					'Bar/Pub',
					'Café',
					'Nachtclub', 
					'Caterer',
					'Catering',
					'Food Zustellservice',
					'Food-Court',
					'Hotel',
					'Restaurant',
					'Takeaway'       
	)
;


--=======================================================================================================
-- because of som performance issue on DB I am going to devide the Hotel_Gastro category on 2 table each 
-- metadata_1 and metadata2
-- results_1 and results_2
-- items_1 and items_2
--=======================================================================================================
--############
--(1) TEILE I
--############

/*
drop table if exists google_maps_dev.google_map_results_hotel_gastronomie_test;
drop table if exists google_maps_dev.google_map_items_hotel_gastronomie_test;
drop table if exists geo_afo_tmp.tmp_results;
select * from geo_afo_tmp.tmp_results;  
update
	google_maps_dev.google_map_metadata_hotel_gastronomie_test1
set datetime = default,
	item_type = default,
	n_result = default,
	status_message = default,
	status_code= default
;
*/


---------------
--(1) METADATA 
---------------
--Part (1) 18891 are null from 92626
select
	* 
from 
	google_maps_dev.google_map_metadata_hotel_gastronomie_pi
where 
	n_result is null
;


--Part (2) 18891 
create table google_maps_dev.google_map_metadata_hotel_gastronomie_pii
as
select
	* 
from 
	google_maps_dev.google_map_metadata_hotel_gastronomie_pi
where 
	n_result is null
;



select
	* 
from 
	google_maps_dev.google_map_metadata_hotel_gastronomie_pii
where 
	n_result is not null
;

--union all pi und pii
create table google_maps_dev.google_map_metadata_hotel_gastronomie_all
as
select 
	*
from
	google_maps_dev.google_map_metadata_hotel_gastronomie_pi
where
	n_result is not null
union all
select 
	*
from 
	google_maps_dev.google_map_metadata_hotel_gastronomie_pii
where
	n_result is not null
;

-------------
--(2) RESULTS 
-------------
--Part (1)
select
	* 
from 
	google_maps_dev.google_map_results_hotel_gastronomie_pi
;

--Part (2)
select
	* 
from 
	google_maps_dev.google_map_results_hotel_gastronomie_pii
;

--union all Pi und Pii
create table google_maps_dev.google_map_results_hotel_gastronomie_all
as
select 
	*
from
	google_maps_dev.google_map_results_hotel_gastronomie_pi
union all
select 
	*
from 
	google_maps_dev.google_map_results_hotel_gastronomie_pii
;



-------------------------------------------
--(3) ITEMS - one table for both pi and pii
-------------------------------------------
select
	* 
from 
	google_maps_dev.google_map_items_hotel_gastronomie_all
;


select 
	cid
	,count(*)
from
	google_maps_dev.google_map_items_hotel_gastronomie_all
group by
	cid 
having 
	count(*) > 1
;


-------------------------------------------------------------------------------
--(4) CATEGORY - table that filitered and mapped with afo category and afo poi
-------------------------------------------------------------------------------
select 
	*
from 
	google_maps_dev.google_map_hotel_gastronomie
;
	


-- Check the dropped category to see if there is any relevant category that should be considered.
drop table if exists tmp_dropped_category;
create temp table tmp_dropped_category
as
select 
	*
from 
	google_maps_dev.google_map_items_hotel_gastronomie_all
where 
	cid not in (
				select 
					cid
				from
					google_maps_dev.google_map_hotel_gastronomie
	)
;	

-- Investigate 
select 
	cid
	,title
	,url
	,category_ids_de
	,category_ids
from 
	tmp_dropped_category
where 
	lower(title) like '% bar %' 
	or
	lower(title) like '% loung %'
	or 
	lower(title) like '% takeaway %'
	or
	lower(title) like '% nachtclub %'
	or
	lower(title) like '% bed and breakfast %'
	or
	lower(title) like '% biergarten %' 
	or
	lower(title) like '% brauhaus %'
	or 
	lower(title) like '% café %'
	or
	lower(title) like '% coffeeshop %'
	or
	lower(title) like '% catering %'
	or 
	lower(title) like '% diskothek %'
	or
	lower(title) like '% food court %'
	or
	lower(title) like '% gasthaus %'
	or
	lower(title) like '% hostel %' 
	or
	lower(title) like '% hotel %'
	or 
	lower(title) like '% jugendherberge %'
	or
	lower(title) like '% langzeithotel %'
	or
	lower(title) like '% Pizza Takeaway %' 
	or
	lower(title) like '% Pizzeria %'
	or 
	lower(title) like '% Pub %'
	or
	lower(title) like '% Restaurant %'
;


-- update "meta_poi_categories_business_data_aktuell" to map the following category to 'Hotel & Gastronomie'
--'gastropub' 'kebab_shop' 'coffee_stand' 'cottage' 'adult_entertainment_club' 'bar_tabac' 'lodging'
-- then run the second part of the python script
update
	geo_afo_prod.meta_poi_categories_business_data_aktuell 
set 
	hauptkategorie_neu  = 'Hotel & Gastronomie'
where 
	category_id  in ('gastropub', 'kebab_shop', 'coffee_stand', 'cottage', 'adult_entertainment_club', 'bar_tabac', 'lodging')
;

--test 
select
	*
from 
	geo_afo_prod.meta_poi_categories_business_data_aktuell 
where
	category_id  in ('gastropub', 'kebab_shop', 'coffee_stand', 'cottage', 'adult_entertainment_club', 'club', 'bar_tabac', 'event_venue', 'lodging')
	and
	hauptkategorie_neu  = 'Hotel & Gastronomie'
;



-- final category table 
select 
	*
from 
	google_maps_dev.google_map_hotel_gastronomie
;


select 
	*
from 
	google_maps_dev.google_map_hotel_gastronomie
where
	cid in ('14223574091158415389', '8581994845898996953')




--==============================
-- Hauptkategory = Hotel & Gastronomie
-- Poi_typ = alle
-- 27.11.2024
-- GeoDatabase
--==============================
--(1) updated Hotel Gastro - all poi_typ
--57135
select 
	*
from 
	google_maps_dev.google_map_hotel_gastronomie
where
	cid in ('14223574091158415389', '8581994845898996953')
;

--(2) current Hotel Gastro - cleaned - Poi_typ "Restaurant", "Hotels", "Hostels"
-- 40572
select 
	*
from 
	google_maps_dev.google_map_hotel_gastronomie_v4
where
	cid in ('14223574091158415389', '8581994845898996953')
;



--(3) new cids that not exist in the current Hotel Gastro
-- 18150
drop table if exists tmp_google_map_hotel_gastro_rest;
create temp table tmp_google_map_hotel_gastro_rest
as
select 
	*
from 
	google_maps_dev.google_map_hotel_gastronomie
where 
	cid not in(
				select 
					cid
				from 
					google_maps_dev.google_map_hotel_gastronomie_v4
	)
;	

select 
	*
from 
	tmp_google_map_hotel_gastro_rest
;

--#############################################################
-- DATA CLEANING AND PREPEARING A FINAL TABLE FOR COMPARISON
--#############################################################
--==================================
-- STEP(1) Working_hours (Reformat)
--==================================

-- (1.1) OPENING HOURS
alter table 
	tmp_google_map_hotel_gastro_rest
add column 
	opening_times text
;

--alter table tmp_google_map_hotel_gastro_rest
--drop column opening_times
--;

update tmp_google_map_hotel_gastro_rest
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
                        and 
                        schedule->'close' is not null  
                )
                and 
                jsonb_typeof(work_hours->'timetable') = 'object'
        ) > 0 
        then (
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
	            ''
    	end;   



--(1.2) STATUS
update 
	tmp_google_map_hotel_gastro_rest
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
                  and 
                  exists (
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
        else 
        	'Not Available'
    end
;


--==================================
-- STEP(2) RELEVANT COLUMN
--==================================
-- Add the new 'relevant' column to the table
alter table 
	tmp_google_map_hotel_gastro_rest
add column 
	relevant numeric
;

-- Update the 'relevant' column based on the specified conditions
update 
	tmp_google_map_hotel_gastro_rest
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
			        	phone is null 
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
			        	strasse is null
			        then
			        	0
			        else
			        	50
			    end
			    +
			    -- Hausnummer
			    --case  
			       -- when 
			        --	hausnummer is null
			       -- then
			        --	0
			      --  else
			        --	25
			  --  end
			     +
			    -- opening_times
			    case  
			        when 
			        	opening_times = ''
			        then
			        	0
			        else
			        	25
			    end
	)
 ;

/*
--create table for redshift
create table google_maps_dev_test.google_map_hotel_gastro_rest
as
select * from tmp_google_map_hotel_gastro_rest;
*/



--==================================
-- STEP(3) PLZ6
--==================================
-- Adding PLZ6 
alter table
	tmp_google_map_hotel_gastro_rest
add column 
	plz6 text
;


update
	tmp_google_map_hotel_gastro_rest t0
set
	plz6 = t1.plz6
from
	geo_afo_prod.imp_plz6_geo_neu t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;


--==================================
-- STEP(4) PLZ4 und PLZ_ORT
--==================================
-- Adding PLZ4 zu plz6
alter table
	tmp_google_map_hotel_gastro_rest
add column 
	plz text,
add column 
	plz_ort text
;


update
	tmp_google_map_hotel_gastro_rest t0
set
	plz = t1.plz,
	plz_ort = t1.plz_ort 
from
	geo_afo_prod.imp_plz6_geo_neu t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;




select * from tmp_google_map_hotel_gastro_rest;



--==================================
-- STEP(5) GEMEINDE
--==================================
-- Add Gemeinde
alter table
	tmp_google_map_hotel_gastro_rest
add column 
	gemeinde text,
add column
	gmd_nr numeric
;


update
	tmp_google_map_hotel_gastro_rest t0
set
	gemeinde = t1.gemeinde
	,gmd_nr = t1.gmd_nr
from
	geo_afo_prod.imp_gmd_geo_neu t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;


--=================================
--STEP(6) GWR STRASSE & HAUSNUMMER
--=================================
create index idx_tmp_google_map_hotel_gastro_rest_poly_lv95 on tmp_google_map_hotel_gastro_rest using GIST(geo_point_lv95);
create index idx_mv_qu_gbd_gwr_aktuell_point_lv95 on geo_afo_prod.mv_qu_gbd_gwr_aktuell using GIST(geo_point_eg_lv95);
create index idx_mv_lay_gbd_aktuell_point_lv95 on geo_afo_prod.mv_lay_gbd_aktuell using GIST(geo_point_eg_lv95);
create index idx_mv_lay_gbd_aktuell_poly_lv95 on geo_afo_prod.mv_lay_gbd_aktuell using GIST(geo_poly_lv95);

--drop table if exists tmp_google_map_hotel_gastro_rest;
--google_maps_dev.google_map_hotel_gastronomie >> using gwr_layer
--google_maps_dev.google_map_hotel_gastronomie_v1 >> using gbd_layer
create temp table tmp_google_map_hotel_gastro_rest_v1
as
select 
    cid
    ,bezeichnung
    ,category_en_ids
    ,category_de_ids
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
    ,gwr_strasse_std 
    ,gwr_hausnum  
    ,gwr_plz4  
    ,gwr_ort  
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
    ,plz4 
    ,ort
    ,gemeinde
    ,gmd_nr
    ,geo_point_google 
    ,geo_point_gwr 
    ,category_ids
    ,category_ids_de
    --,geo_poly_lv95 as geo_poly_gbd
from (
		select  
	        t0.cid
	        ,t0.title as bezeichnung
			,'[' ||t0.category_en_ids|| ']' 						as category_en_ids
			,'[' ||t0.category_de_ids|| ']' 						as category_de_ids
			,'[' ||t0.afo_poi_typ || ']' 							as poi_typ
			,'[' ||t0.afo_category|| ']' 							as kategorie
			,t0.afo_hauptkategorie 									as hauptkategorie
		    ,t0.phone 												as telefon
			,t0.address 											as adresse
	        ,t0.strasse   											as google_strasse
	        ,t0.hausnummer 											as google_hausnum
	        ,t0.plz4 												as google_plz4
   			,t0.ort  												as google_ort
	        ,t1.strname 											as gwr_strasse       	--strbez2l as gbd_strasse
	        ,t1.strname_std 										as gwr_strasse_std
	        ,t1.deinr_std 											as gwr_hausnum   		--hnr as gbd_hausnum --
	        ,t1.dplz4   											as gwr_plz4 			--plz4 as gbd_plz4  --
	        ,t1.dplzname 											as gwr_ort				--ort as gbd_ort --
	        ,t0.url
		    ,t0.domain
		    ,t0.anz_fotos
		    ,t0.google_bewertung
		    ,t0.anz_bewertungen
		    ,t0.status
		    ,t0.opening_times
		    ,t0.relevant
		    ,t0.plz6
		    ,t0.plz 												as plz4
		    ,t0.plz_ort 											as ort
		    ,t0.gemeinde
		    ,t0.gmd_nr
	        ,t0.geo_point_lv95 										as geo_point_google
	        ,t1.geo_point_eg_lv95 									as geo_point_gwr
	        ,t0.category_ids
    		,t0.category_ids_de
	        ,ST_Distance(
	        			t1.geo_point_eg_lv95 
	        			,t0.geo_point_lv95
	        ) 														as distance
	        ,ROW_NUMBER() over(
	        				partition by 
	        						t0.cid 
	        				order by 
	        						ST_Distance(
	        							t1.geo_point_eg_lv95
	        							,t0.geo_point_lv95
	        						)
	        			) 											as rn
	    from 
	        tmp_google_map_hotel_gastro_rest t0
	    left join  
	        geo_afo_prod.mv_qu_gbd_gwr_aktuell t1 --geo_afo_prod.mv_lay_gbd_aktuell t1  -- 
	    on 
	        ST_DWithin(t1.geo_point_eg_lv95, t0.geo_point_lv95, 20)
	        --or 
	        --ST_Contains(t1.geo_poly_lv95 , t0.geo_point_lv95)
) t
where 
    rn = 1
;


select * from tmp_google_map_hotel_gastro_rest_v1;


--============================================================================================
--STEP(7) ADDRESS CLEANING 
--desired_street_name = 
--         regexp_replace(reverse(trim(SPLIT_PART(reverse(adresse), ',', 2))),'\s+\d.*$', '')
--============================================================================================
-- add a new address block (korr_ ) 
alter table tmp_google_map_hotel_gastro_rest_v1
add column korr_strasse text,
add column korr_hausnum text,
add column korr_plz4 text,
add column korr_ort text
;

---------------------------------
--(7.1)column: Korr_plz4
---------------------------------
select
	bezeichnung 
	,adresse 
	,korr_plz4
	,google_plz4 
	,gwr_plz4 
	,plz4
	,gwr_strasse 
from
	tmp_google_map_hotel_gastro_rest_v1
where 
	--google_plz4 = plz4
	--google_plz4 <> plz4
	--google_plz4 is null
	google_plz4 like '%,%' 
;

--Korr_plz4
update tmp_google_map_hotel_gastro_rest_v1
set 
	korr_plz4 = plz4
where  
	google_plz4 = plz4
	or 
	google_plz4 is null
	or 
	google_plz4 like '%,%'
;


update tmp_google_map_hotel_gastro_rest_v1
set 
	korr_plz4 = google_plz4
where  
	google_plz4 <> plz4 
;


-----------------------------------
--(7.2)column: Korr_Ort
-----------------------------------
select
	korr_ort
	,google_ort 
	,gwr_ort 
	,ort
from
	tmp_google_map_hotel_gastro_rest_v1
where 
	google_plz4 <> plz4
;

-- Korr_Ort
update tmp_google_map_hotel_gastro_rest_v1
set 
	korr_ort = ort
where 
	google_ort = ort
	and 
	korr_ort is null 
;

update tmp_google_map_hotel_gastro_rest_v1
set 
	korr_ort = google_ort
where 
	google_ort <> ort
	and 
	korr_ort is null 
;


--------------------------------------------------------
--(7.3)column: Korr_Strasse 
-- cleaning street name from google data
--------------------------------------------------------
select 
	google_strasse 
	,regexp_replace(reverse(trim(SPLIT_PART(reverse(adresse), ',', 2))),'\s+\d.*$', '') as google_strasse_std
from 
	tmp_google_map_hotel_gastro_rest_v1
where 
	google_strasse is not null
;

alter table tmp_google_map_hotel_gastro_rest_v1
add column google_strasse_std text
;

update tmp_google_map_hotel_gastro_rest_v1
set 
	google_strasse_std = regexp_replace(reverse(trim(SPLIT_PART(reverse(adresse), ',', 2))),'\s+\d.*$', '') 
;

update tmp_google_map_hotel_gastro_rest_v1
set 
	google_strasse = trim(google_strasse)
;


-------------------------------
--(7.3.1)Korr_Strasse - Part(1)
-------------------------------
select 
	korr_strasse
	,adresse
	,google_strasse
	,google_strasse_std
	,gwr_strasse
	,gwr_strasse_std
	,geo_point_google
	,geo_point_gwr
	,distance
from
	tmp_google_map_hotel_gastro_rest_v1
where 
	google_strasse 		ilike '%'|| gwr_strasse ||'%' 
	or 
	google_strasse_std 	ilike '%'|| gwr_strasse ||'%' 
	or 
	google_strasse 		ilike '%'|| gwr_strasse_std ||'%' 
	or 
	google_strasse_std 	ilike '%'|| gwr_strasse_std ||'%' 
;

update tmp_google_map_hotel_gastro_rest_v1
set 
	korr_strasse = gwr_strasse 
where 
	korr_strasse is null 
	and(
	google_strasse 		ilike '%'|| gwr_strasse ||'%' 
	or 
	google_strasse_std 	ilike '%'|| gwr_strasse ||'%' 
	or 
	google_strasse 		ilike '%'|| gwr_strasse_std ||'%' 
	or 
	google_strasse_std 	ilike '%'|| gwr_strasse_std ||'%' 
	)
;


-------------------------------
--(7.3.2)Korr_Strasse - Part(2) --????
-------------------------------
select 
	korr_strasse 
	,adresse 
	,google_strasse
	,google_strasse_std 
	,gwr_strasse 
	,gwr_strasse_std 
from
	tmp_google_map_hotel_gastro_rest_v1
where 
	korr_strasse is null
	and 
	google_strasse <> google_strasse_std
	and( 
		google_strasse ilike '%'||gwr_strasse||'%'
		or 
		google_strasse_std ilike '%'||gwr_strasse||'%'
	)
;

update tmp_google_map_hotel_gastro_rest_v1
set 
	korr_strasse = gwr_strasse 
where 
	korr_strasse is null
	and 
	google_strasse <> google_strasse_std
	and 
	google_strasse_std ilike '%'|| gwr_strasse ||'%' 
;


-------------------------------
--(7.3.3)Korr_Strasse - Part(3)
-------------------------------
select 
	korr_strasse 
	,adresse 
	,google_strasse
	,google_strasse_std
	,gwr_strasse 
	,gwr_strasse_std 
from
	tmp_google_map_hotel_gastro_rest_v1
where 
	korr_strasse is null 
	and
	(
	   lower(google_strasse) 	 ilike '%strasse%'
    or lower(google_strasse) 	 ilike '%rue%'
    or lower(google_strasse) 	 ilike '%av. %'
    or lower(google_strasse) 	 ilike '%avenue%'
    or lower(google_strasse) 	 ilike '%chem. des%'
    or lower(google_strasse) 	 ilike '%chemin des%'
    or lower(google_strasse) 	 ilike '%weg%'
    or lower(google_strasse) 	 ilike '%chem.%'
    or lower(google_strasse) 	 ilike '%rte de%'
    or lower(google_strasse) 	 ilike '%pl.%'
    or lower(google_strasse) 	 ilike '%chemin%'
    or lower(google_strasse) 	 ilike '%rte%'
    or lower(google_strasse) 	 ilike '%les %'
	or lower(google_strasse) 	 ilike '%le %'
	or lower(google_strasse) 	 ilike '%la %'
    or lower(google_strasse) 	 ilike '%prom.%'
    or lower(google_strasse) 	 ilike '%via%'
    or lower(google_strasse) 	 ilike '%esp%'
    or lower(google_strasse) 	 ilike '%rue%'
    or lower(google_strasse) 	 ilike '%quai%'
    or lower(google_strasse) 	 ilike '%route%'
    or lower(google_strasse_std) ilike '%strasse%'
    or lower(google_strasse_std) ilike '%rue%'
    or lower(google_strasse_std) ilike '%av. %'
    or lower(google_strasse_std) ilike '%avenue%'
    or lower(google_strasse_std) ilike '%chem. des%'
    or lower(google_strasse_std) ilike '%chemin des%'
    or lower(google_strasse_std) ilike '%weg%'
    or lower(google_strasse_std) ilike '%chem.%'
    or lower(google_strasse_std) ilike '%rte de%'
    or lower(google_strasse_std) ilike '%pl.%'
    or lower(google_strasse_std) ilike '%chemin%'
    or lower(google_strasse_std) ilike '%rte%'
    or lower(google_strasse_std) ilike '%les %'
	or lower(google_strasse_std) ilike '%le %'
	or lower(google_strasse_std) ilike '%la %'
    or lower(google_strasse_std) ilike '%prom.%'
    or lower(google_strasse_std) ilike '%via%'
    or lower(google_strasse_std) ilike '%esp%'
    or lower(google_strasse_std) ilike '%rue%'
    or lower(google_strasse_std) ilike '%quai%'
    or lower(google_strasse_std) ilike '%route%'
    )
;

update tmp_google_map_hotel_gastro_rest_v1 t0
set 
	korr_strasse = google_strasse_std 
where 
	korr_strasse is null 
	and
	(
	   lower(google_strasse) 	 ilike '%strasse%'
    or lower(google_strasse) 	 ilike '%rue%'
    or lower(google_strasse) 	 ilike '%av. %'
    or lower(google_strasse) 	 ilike '%avenue%'
    or lower(google_strasse) 	 ilike '%chem. des%'
    or lower(google_strasse) 	 ilike '%chemin des%'
    or lower(google_strasse) 	 ilike '%weg%'
    or lower(google_strasse) 	 ilike '%chem.%'
    or lower(google_strasse) 	 ilike '%rte de%'
    or lower(google_strasse) 	 ilike '%pl.%'
    or lower(google_strasse) 	 ilike '%chemin%'
    or lower(google_strasse) 	 ilike '%rte%'
    or lower(google_strasse) 	 ilike '%les %'
	or lower(google_strasse) 	 ilike '%le %'
	or lower(google_strasse) 	 ilike '%la %'
    or lower(google_strasse) 	 ilike '%prom.%'
    or lower(google_strasse) 	 ilike '%via%'
    or lower(google_strasse) 	 ilike '%esp%'
    or lower(google_strasse) 	 ilike '%rue%'
    or lower(google_strasse) 	 ilike '%quai%'
    or lower(google_strasse) 	 ilike '%route%'
    or lower(google_strasse_std) ilike '%strasse%'
    or lower(google_strasse_std) ilike '%rue%'
    or lower(google_strasse_std) ilike '%av. %'
    or lower(google_strasse_std) ilike '%avenue%'
    or lower(google_strasse_std) ilike '%chem. des%'
    or lower(google_strasse_std) ilike '%chemin des%'
    or lower(google_strasse_std) ilike '%weg%'
    or lower(google_strasse_std) ilike '%chem.%'
    or lower(google_strasse_std) ilike '%rte de%'
    or lower(google_strasse_std) ilike '%pl.%'
    or lower(google_strasse_std) ilike '%chemin%'
    or lower(google_strasse_std) ilike '%rte%'
    or lower(google_strasse_std) ilike '%les %'
	or lower(google_strasse_std) ilike '%le %'
	or lower(google_strasse_std) ilike '%la %'
    or lower(google_strasse_std) ilike '%prom.%'
    or lower(google_strasse_std) ilike '%via%'
    or lower(google_strasse_std) ilike '%esp%'
    or lower(google_strasse_std) ilike '%rue%'
    or lower(google_strasse_std) ilike '%quai%'
    or lower(google_strasse_std) ilike '%route%'
    )
;

-------------------------------
--(7.3.4)Korr_Strasse - Part(4)
-------------------------------
select 
	korr_strasse 
	,adresse 
	,google_strasse
	,google_strasse_std 
	,gwr_strasse 
	,gwr_strasse_std 
	,geo_point_google
	,geo_point_gwr
	,distance
from
	tmp_google_map_hotel_gastro_rest_v1
where
	korr_strasse is null  
	and( 
		adresse ilike '%'||gwr_strasse||'%'
		or 
		gwr_strasse is not null
	)
order by 
 distance desc
;


update tmp_google_map_hotel_gastro_rest_v1
set 
	korr_strasse = gwr_strasse 
where 
	korr_strasse is null  
	and( 
		adresse ilike '%'||gwr_strasse||'%'
		or 
		gwr_strasse is not null
	)
;


-- empty korr_strasse in case google_strasse is empty as well we will take gwr_strasse but it needs to be manually tested
update tmp_google_map_hotel_gastro_rest_v1
set 
	korr_strasse = gwr_strasse 
where 
	(korr_strasse is null or korr_strasse = '') 
	and
	gwr_strasse is not null
;


-------------------------------
--(7.3.5)Korr_Strasse - Part(5)
-------------------------------
select 
	korr_strasse 
	,adresse 
	,google_strasse
	,google_strasse_std 
	,gwr_strasse 
	,gwr_strasse_std 
	,geo_point_google
	,geo_point_gwr
	,distance
from
	tmp_google_map_hotel_gastro_rest_v1
where
	korr_strasse is null  
	and 
	gwr_strasse is null
	and 
	google_strasse_std !~ '^\d+(\.\d+)?$'
	and 
	google_strasse_std <> ''
;


update tmp_google_map_hotel_gastro_rest_v1
set 
	korr_strasse = google_strasse_std 
where
	korr_strasse is null  
	and 
	gwr_strasse is null
	and 
	google_strasse_std !~ '^\d+(\.\d+)?$'
	and 
	google_strasse_std <> ''
;


--------------------------------------------------------
--(7.4)column: Korr_hausnum
--------------------------------------------------------
-------------------------------
--(7.4.1)Korr_hausnum - Part(1)
-------------------------------
select
	korr_hausnum
	,adresse 
	,google_strasse
	,google_strasse_std
	,gwr_strasse
	,gwr_strasse_std
	,google_hausnum 
	,gwr_hausnum 
from
	tmp_google_map_hotel_gastro_rest_v1
where 
	korr_hausnum is null
	and 
	google_hausnum <> google_plz4 
;


update tmp_google_map_hotel_gastro_rest_v1
set 
	korr_hausnum = lower(google_hausnum)
where  
	korr_hausnum is null
	and 
	google_hausnum <> google_plz4 
;

-------------------------------
--(7.4.2)Korr_hausnum - Part(2)
-------------------------------
select
	korr_hausnum
	,adresse 
	,google_strasse
	,google_strasse_std
	,gwr_strasse
	,gwr_strasse_std
	,google_hausnum 
	,gwr_hausnum 
from
	tmp_google_map_hotel_gastro_rest_v1
where 
	korr_hausnum is null
	and 
	gwr_hausnum <> ''
	and 
	(google_hausnum is null or google_hausnum = google_plz4)
	and
	korr_strasse ilike '%'||gwr_strasse||'%'
;	
	--(
	--google_strasse 		ilike '%'||gwr_strasse||'%' 
	--or 
	--google_strasse_std 	ilike '%'||gwr_strasse||'%' 
	--or 
	--google_strasse 		ilike '%'||gwr_strasse_std||'%' 
	--or 
	--google_strasse_std 	ilike '%'||gwr_strasse_std||'%' 
	--)



update tmp_google_map_hotel_gastro_rest_v1
set 
	korr_hausnum = gwr_hausnum
where  
	korr_hausnum is null
	and 
	gwr_hausnum <> ''
	and 
	(google_hausnum is null or google_hausnum = google_plz4)
	and
	korr_strasse ilike '%'||gwr_strasse||'%'
;


-------------------------------
--(7.4.3)Korr_hausnum - Part(3)
-------------------------------
select
	korr_hausnum
	,adresse 
	,google_strasse
	,google_strasse_std
	,korr_strasse
	,gwr_strasse
	,gwr_strasse_std
	,google_hausnum 
	,gwr_hausnum 
	,geo_point_google
	,geo_point_gwr
	,distance
from
	tmp_google_map_hotel_gastro_rest_v1
where 
	korr_hausnum is null
	and 
	gwr_strasse is not null
	and 
	korr_strasse is not null
	and 
	gwr_hausnum <> ''
	and 
	gwr_hausnum is not null
order by 
	distance desc
;

-- empty korr_hausnum in case google_hausnum is empty as well, we will take gwr_hausnum but it needs to be manually tested
update tmp_google_map_hotel_gastro_rest_v1
set 
	korr_hausnum = gwr_hausnum
where  
	korr_hausnum is null
	and 
	gwr_strasse is not null
	and 
	gwr_hausnum <> ''
;


update tmp_google_map_hotel_gastro_rest_v1
set 
	korr_hausnum = gwr_hausnum 
where 
	(korr_hausnum  is null or korr_hausnum = '') 
	and
	gwr_hausnum <> ''
;


--=====================================
--STEP(8) Final Table
--=====================================
drop table if exists google_maps_dev_test.google_map_hotel_gastro_rest;

create table 
	google_maps_dev_test.google_map_hotel_gastro_rest
as
select 
	cid    
	,bezeichnung 
	,category_en_ids 
	,category_de_ids 
	,poi_typ 
	,kategorie 
	,hauptkategorie 
	,korr_strasse
	,korr_hausnum
	,korr_plz4
	,korr_ort
	,telefon 
	,adresse 
	,google_strasse
	,google_strasse_std
	,google_hausnum 
	,google_plz4 
	,google_ort 
	,gwr_strasse
	,gwr_strasse_std
	,gwr_hausnum 
	,gwr_plz4 
	,gwr_ort 
	,distance
	,plz6
	,plz4
	,ort
	,gemeinde 
	,gmd_nr
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
	,category_ids
	,category_ids_de
from 
	tmp_google_map_hotel_gastro_rest_v1
order by
	relevant desc
;

select 
	cid
	,count(*)
from 
	google_maps_dev_test.google_map_hotel_gastro_rest
group by
	cid
having 
	count(*) > 1
;


--=================================
-- Most updated Hotel/Gastro Table 
--=================================
create table google_maps_dev.google_map_hotel_gastronomie_v5
as
select 
	*
	,null as category_ids_de
from
	google_maps_dev.google_map_hotel_gastronomie_v4
union all
select 
	*
from 
	google_maps_dev_test.google_map_hotel_gastro_rest
;



select
	cid
	,korr_plz4 
	,plz4 
	,google_plz4 
	,gmd_nr 
from 
	google_maps_dev.google_map_hotel_gastronomie_v5
where
	google_plz4 like '%3.%' 
;

/*
--TEST
select 
	cid
	,count(*)
from 
	google_maps_dev.google_map_hotel_gastronomie_v5
group by 
	cid
having 
	count(*) > 1
where 
	cid = '6581140534828110929'
;
*/



--===================
-- Abgleich Table 
--===================
-- create table for Comparison with our AFO POI 
drop table if exists google_maps_dev_abgleich.google_hotel_gastro;
create table google_maps_dev_abgleich.google_hotel_gastro
as
select
	 cid as cid 
	,trim(coalesce(korr_strasse, ' ')||' '|| coalesce(korr_hausnum, '')) as strasse
	,korr_plz4::numeric as plz4
	,korr_ort as ort
	,adresse as address
	,bezeichnung as title
	,google_strasse
	,google_strasse_std
	,google_hausnum 
	,google_plz4
	,google_ort
	,gwr_strasse 
	,gwr_hausnum 
	,gwr_plz4 
	,gwr_ort 
	,plz6
	,gmd_nr
	,gemeinde
	,"domain" 
	,url 
	,poi_typ as google_poi_typ
	,category_en_ids as category_ids_text 
	,category_ids as category_ids_jsonb 
	,status
	,geo_point_google as geo_point_lv95 
from 
	google_maps_dev.google_map_hotel_gastronomie_v5
;

select
	*
from 
	google_maps_dev_abgleich.google_hotel_gastro
;


--===============================
--table for peter
--===============================

select 
    *
from  
    google_maps_dev_abgleich.poi_abgleich_google_tot
where 
    dubletten_nr IS NULL
    and
    quelle = 'GOOGLE'
    and (
        category_ids_text NOT LIKE '%[restaurant%'
        and category_ids_text not like  '%bar%'
        and category_ids_text not like  '%cafe%'
        and category_ids_text not like  '%hotel%'
        and category_ids_text not like  '%pizza_delivery%'
        and category_ids_text not like  '%wine_bar%'
        and category_ids_text not like  '%adult_entertainment_club%'
        and category_ids_text not like  '%hostel%'
        and category_ids_text not like  '%lounge%'
        and category_ids_text not like  '%bed_and_breakfast%'
        and category_ids_text not like  '%catering_service%' 
        and category_ids_text not like  '%coffee_shop%'
        and category_ids_text not like  '%pub%'
        and category_ids_text not like  '%kebab_shop%'
        and category_ids_text not like  '%night_club%'
        and category_ids_text not like  '%brewpub%'
        and category_ids_text not like  '%mountain_hut%'
        and category_ids_text not like  '%guest_house%'
        and category_ids_text not like  '%inn%'
        and category_ids_text not like  '%meal_takeaway%'
        and category_ids_text not like  '%lodging%'
        and category_ids_text not like  '%dance_club%'
        and category_ids_text not like  '%disco%'
        and category_ids_text not like  '%cottage%'
        and category_ids_text not like  '%bistro%'
        and category_ids_text not like  '%sushi_takeaway%'
        and category_ids_text not like  '%pizzatakeaway%'
        and category_ids_text not like  '%tea_house%'
        and category_ids_text not like  '%asador%'
        and category_ids_text not like  '%food_court%'
        and category_ids_text not like  '%meal_delivery%'
        and category_ids_text not like  '%steak_house%'
        and category_ids_text not like  '%motel%'
        and category_ids_text not like  '%beer_garden%'
        and category_ids_text not like  '%_takeaway%'
        and category_ids_text not like  '%beer_hall%'
        and category_ids_text not like  '%_takeaway%'
    );

   
select 
	poi_id
    ,google_poi_typ
	,category_ids_text
	,company
from  
    google_maps_dev_abgleich.poi_abgleich_google_tot
where 
	poi_id in (
			'4911509136221871840'
			,'8655363019242285544', '10940477724884639459', '8954951447337445175', '16844618168290505017', '10440053066821036228',  '11446426647765614749', '6599281568896245014'
			,'5448516088854113366', '3740462140511256460', '3953969710345824870', '2056953361776971082', '15217882080525808875', '3877380495341897675', '8887242683410724441', '886513595036034203'
	)
;	
   


--to delete 5448516088854113366, 4911509136221871840, 3877380495341897675, 3740462140511256460

drop table if exists google_maps_dev_abgleich.poi_abgleich_google_tot_csv;
create table google_maps_dev_abgleich.poi_abgleich_google_tot_csv
as
select
	*
from 
	google_maps_dev_abgleich.poi_abgleich_google_tot
;	


update google_maps_dev_abgleich.poi_abgleich_google_tot_csv
set poi_id = '''' || poi_id,
	dubletten_nr = '''' || dubletten_nr
;


select 
	*
from 
	google_maps_dev_abgleich.poi_abgleich_google_tot_csv
;


