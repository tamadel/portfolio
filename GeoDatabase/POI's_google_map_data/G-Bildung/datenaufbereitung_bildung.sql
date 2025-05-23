-------------------------------------------------------------------------------------------
-- Kunde: AFO
-- Thema: GOOGLE POI's
-- Datum: 02.12.2024
-- Autor: Tamer Adel
-- DB: geo_database
-- Schema: google_maps_dev
-------------------------------------------------------------------------------------------
-- Hauptkategorie: Bildung
-----------------------
-- Daten sichten
-----------------------
-- Python script: google_maps_get_new.py 
-- outcome two Tables (1) - google_map_result_<Hauptkategorie-Name>
-- 					  (2) - google_map_items_<Hauptkategorie-Name>
--					  (3) - google_map_<Hauptkategorie-Name>
-- we start from Step(2)
-- Bildung | Einkaufszentrum | Hotel und Gastronomie | Landwirtschaft | Non - Food Geschäft
--#############################################################
-- DATA CLEANING AND PREPEARING A FINAL TABLE FOR COMPARISON
--#############################################################




--==================================
-- STEP(2) Working_hours (Reformat)
--==================================
drop table if exists
	tmp_google_map_bildung;

create temp table 
	tmp_google_map_bildung
as 
select
	*
from 
	google_maps_dev.google_map_bildung
;

-- (2.1) OPENING HOURS
alter table 
	tmp_google_map_bildung
add column if not exists 
	opening_times text
;

update 
	tmp_google_map_bildung
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
                        ), ' & ' -- Concatenate multiple time slots for the same day
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

--(2.2) STATUS
update 
	tmp_google_map_bildung
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
-- STEP(3) RELEVANT COLUMN
--==================================
-- Add the new 'relevant' column to the table
alter table 
	tmp_google_map_bildung
add column 
	relevant numeric
;

-- Update the 'relevant' column based on the specified conditions
update 
	tmp_google_map_bildung
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

--==================================
-- STEP(4) PLZ6
--==================================
-- Adding PLZ6 
alter table
	tmp_google_map_bildung
add column 
	plz6 text
;

update
	tmp_google_map_bildung t0
set
	plz6 = t1.plz6
from
	geo_afo_prod.imp_plz6_geo_neu t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;

--==================================
-- STEP(5) PLZ4 und PLZ_ORT
--==================================
-- Adding PLZ4 to plz6
alter table
	tmp_google_map_bildung
add column 
	plz text,
add column 
	plz_ort text
;

-- add plz4 and ORT
update
	tmp_google_map_bildung t0
set
	plz = t1.plz,
	plz_ort = t1.plz_ort 
from
	geo_afo_prod.imp_plz6_geo_neu t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;


--select * from geo_afo_prod.imp_plz6_geo_neu;
--alter table geo_afo_prod.imp_plz6_geo_neu add column plz_ort text;
--update geo_afo_prod.imp_plz6_geo_neu t0 set plz_ort = t1.ort from geo_afo_prod.mv_lay_plz4_aktuell t1 where t0.plz = t1.plz4;

--==================================
-- STEP(6) GEMEINDE
--==================================
-- Add Gemeinde
alter table
	tmp_google_map_bildung
add column 
	gemeinde text,
add column
	gmd_nr numeric
;

update
	tmp_google_map_bildung t0
set
	gemeinde = t1.gemeinde
	,gmd_nr = t1.gmd_nr
from
	geo_afo_prod.imp_gmd_geo_neu t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;


--=================================
--STEP(7) GWR STRASSE & HAUSNUMMER
--=================================
create index if not exists idx_tmp_googl_missing_hostel_poly_lv95 on tmp_google_map_bildung using GIST(geo_point_lv95);
create index if not exists idx_mv_qu_gbd_gwr_aktuell_point_lv95 on geo_afo_prod.mv_qu_gbd_gwr_aktuell using GIST(geo_point_eg_lv95);
create index if not exists idx_mv_lay_gbd_aktuell_point_lv95 on geo_afo_prod.mv_lay_gbd_aktuell using GIST(geo_point_eg_lv95);
create index if not exists idx_mv_lay_gbd_aktuell_poly_lv95 on geo_afo_prod.mv_lay_gbd_aktuell using GIST(geo_poly_lv95);

drop table if exists 
	tmp_google_map_bildung_V1;
--google_maps_dev.google_map_hotel_gastronomie >> using gwr_layer
--google_maps_dev.google_map_hotel_gastronomie_v1 >> using gbd_layer
create temp table 
	tmp_google_map_bildung_V1
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
        tmp_google_map_bildung t0
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
select * from tmp_google_map_bildung_v1;
-- select * from tmp_google_map_bildung_v1;


--============================================================================================
--STEP(8) ADDRESS CLEANING 
--desired_street_name = 
--         regexp_replace(reverse(trim(SPLIT_PART(reverse(adresse), ',', 2))),'\s+\d.*$', '')
--============================================================================================
-- add a new address block (korr_ ) 
alter table 
	tmp_google_map_bildung_V1
add column 
	korr_strasse text,
add column 
	korr_hausnum text,
add column 
	korr_plz4 text,
add column 
	korr_ort text
;

---------------------------------
--(8.1)column: Korr_plz4
---------------------------------
select
	cid
	,bezeichnung 
	,adresse 
	,korr_plz4
	,google_plz4 
	,gwr_plz4 
	,plz4
	,gwr_strasse 
from
	tmp_google_map_bildung_V1
where 
	--google_plz4 = plz4
	--google_plz4 <> plz4
	google_plz4 is null
;

--Korr_plz4
update 
	tmp_google_map_bildung_V1
set
	korr_plz4 = plz4
where	
	korr_plz4 IS NULL
  	and	(
        google_plz4 is null  
        or not 
        google_plz4 ~ '^[1-9][0-9]{3}$'  -- Matches exactly 4 digits, no letters, not starting with 0
      	or
        google_plz4 = plz4
      )
;


update 
	tmp_google_map_bildung_V1
set 
	korr_plz4 = google_plz4
where  
	korr_plz4 is null
	or
	google_plz4 <> plz4 
	and
	google_plz4 ~ '^[1-9][0-9]{3}$'
;


-----------------------------------
--(8.2)column: Korr_Ort
-----------------------------------
select
	korr_ort
	,google_ort 
	,gwr_ort 
	,ort
from
	tmp_google_map_bildung_V1
where 
	google_plz4 <> plz4
;

-- Korr_Ort
update 
	tmp_google_map_bildung_V1
set  
    korr_ort = ort
where  
    korr_ort is null 
    and (
        google_ort is null 
        or
        google_ort !~ '[0-9]'  -- Contains numbers
        or
        LENGTH(google_ort) < 4  -- Has fewer than 4 characters
    )
;

update 
	tmp_google_map_bildung_V1
set 
	korr_ort = google_ort
where 
	google_ort <> ort
	and 
	korr_ort is null 
;


--------------------------------------------------------
--(8.3)column: Korr_Strasse 
-- cleaning street name from google data
--------------------------------------------------------
alter table 
	tmp_google_map_bildung_V1
add column google_strasse_std text
;

update 
	tmp_google_map_bildung_V1
set 
	google_strasse_std = regexp_replace(reverse(trim(SPLIT_PART(reverse(adresse), ',', 2))),'\s+\d.*$', '') 
;

update 
	tmp_google_map_bildung_V1
set 
	google_strasse_std = trim(google_strasse)
where 
	google_strasse_std ~ '[0-9]'
	and 
	google_strasse !~ '[0-9]'
;

update 
	tmp_google_map_bildung_V1
set 
	google_strasse_std = null
where 
	google_strasse_std ~ '[0-9]'
;

update 
	tmp_google_map_bildung_V1
set 
	google_strasse = trim(google_strasse)
;


-------------------------------
--(8.3.1)Korr_Strasse - Part(1)
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
	tmp_google_map_bildung_V1
where 
	google_strasse 		ilike '%'|| gwr_strasse ||'%' 
	or 
	google_strasse_std 	ilike '%'|| gwr_strasse ||'%' 
	or 
	google_strasse 		ilike '%'|| gwr_strasse_std ||'%' 
	or 
	google_strasse_std 	ilike '%'|| gwr_strasse_std ||'%' 
;

update 
	tmp_google_map_bildung_V1
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
--(8.3.2)Korr_Strasse - Part(2) --????
-------------------------------
select 
	korr_strasse 
	,adresse 
	,google_strasse
	,google_strasse_std 
	,gwr_strasse 
	,gwr_strasse_std 
from
	tmp_google_map_bildung_V1
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

update 
	tmp_google_map_bildung_V1
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
--(8.3.3)Korr_Strasse - Part(3)
-------------------------------
select 
	korr_strasse 
	,adresse 
	,google_strasse
	,google_strasse_std
	,gwr_strasse 
	,gwr_strasse_std 
from
	tmp_google_map_bildung_V1
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

update 
	tmp_google_map_bildung_V1 t0
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
--(8.3.4)Korr_Strasse - Part(4)
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
	tmp_google_map_bildung_V1
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


update 
	tmp_google_map_bildung_V1
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


-------------------------------
--(8.3.5)Korr_Strasse - Part(5)
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
	tmp_google_map_bildung_V1
where
	korr_strasse is null  
	and 
	gwr_strasse is null
	and 
	google_strasse_std !~ '^\d+(\.\d+)?$'
	and 
	google_strasse_std <> ''
;


update 
	tmp_google_map_bildung_V1
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
--(8.4)column: Korr_hausnum
--------------------------------------------------------
-------------------------------
--(8.4.1)Korr_hausnum - Part(1)
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
	tmp_google_map_bildung_V1
where 
	korr_hausnum is null
	and 
	google_hausnum <> google_plz4 
;


update 
	tmp_google_map_bildung_V1
set 
	korr_hausnum = lower(google_hausnum)
where  
	korr_hausnum is null
	and 
	google_hausnum <> google_plz4 
;

-------------------------------
--(8.4.2)Korr_hausnum - Part(2)
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
	tmp_google_map_bildung_V1
where 
	korr_hausnum is null
	and 
	gwr_hausnum <> ''
	and 
	(google_hausnum is null or google_hausnum = google_plz4)
	and
	korr_strasse ilike '%'||gwr_strasse||'%'
;	
	

update 
	tmp_google_map_bildung_V1
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
--(8.4.3)Korr_hausnum - Part(3)
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
	tmp_google_map_bildung_V1
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


update 
	tmp_google_map_bildung_V1
set 
	korr_hausnum = gwr_hausnum
where  
	korr_hausnum is null
	and 
	gwr_strasse is not null
	and 
	gwr_hausnum <> ''
	and 
	gwr_hausnum is not null
;


--=====================================
--STEP(9) Final Table
--=====================================
drop table if exists 
	google_maps_dev.google_map_bildung_v1;

create table 
	google_maps_dev.google_map_bildung_v1
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
	tmp_google_map_bildung_V1
order by
	relevant desc
;

/* Test and Adjust
select 
	cid
	,count(*)
from 
	google_maps_dev.google_map_bildung_v1
group by
	cid
having 
	count(*) > 1
;

select 
	cid
 	,korr_plz4
from 
	google_maps_dev.google_map_bildung_v1
where 
	korr_plz4 like '%,%'
;


update google_maps_dev.google_map_bildung
set 
	korr_plz4 = '8004'
where 
	cid = '8013190822013999088'
;
*/



--========================
--STEP(9): Abgleich Table 
--========================
-- create table for Comparison with our AFO POI 
drop table if exists 
	google_maps_dev_abgleich.google_abgleich_bildung;

create table 
	google_maps_dev_abgleich.google_abgleich_bildung
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
	,category_en_ids as category_ids
	,geo_point_google as geo_point_lv95
from 
	google_maps_dev.google_map_bildung_v1
;

--POIS: Google bildung
select 
	*
from 
	google_maps_dev_abgleich.google_abgleich_bildung
;

	
--POIS: AFO bildungdienst
drop table if exists 
	google_maps_dev_abgleich.afo_poi_typ_bildung;

create table 
	google_maps_dev_abgleich.afo_poi_typ_bildung
as
select 
	*
from 
	geo_afo_prod.mv_lay_poi_aktuell
where
	poi_typ in (
		select distinct
			poi_typ_alt 
		from
			google_maps_dev.google_map_category_hierarchy 
		where
			hauptkategorie_neu = 'Bildung'
	)
;	


--=====================================
--STEP(10) Matching zwischen AFO-POIs und Google-POIs
--=====================================
call 
	google_maps_dev_abgleich.sp_abgleich_poi_google 
(
	'google_maps_dev_abgleich.afo_poi_typ_bildung'						-- afo_poi_input_table
	,'google_maps_dev_abgleich.google_abgleich_bildung'					-- google_poi_input_table
	,'google_maps_dev_abgleich.poi_abgleich_google_bildung_tot'			-- abgleich_output_table
)
;


--=====================================
--STEP(11) File-Export nach S3
--=====================================
SELECT 
	* 
from 
	aws_s3.query_export_to_s3(
		'	
			select 
				* 
			from 
				google_maps_dev_abgleich.poi_abgleich_google_bildung_tot
		', 
   		aws_commons.create_s3_uri(
   			'afo-db-prod-data-exchange', 
   			'google_maps_dev_abgleich/poi_abgleich_google_bildung_tot.csv', 
   			'eu-central-1'
   		),
        options :='format csv, delimiter $$|$$, HEADER true' 
	)
;
