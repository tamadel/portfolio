--==============================
-- Hauptkategory = Hotel & Gastronomie
-- Poi_typ = ('Hostel', 'Jugendherberge')
-- 12.11.2024
--==============================
update 
	geo_afo_prod.meta_poi_google_maps_category
set next_run_date  = current_date  
where 
	hauptkategorie_neu  = 'Hotel & Gastronomie'
	and 
	poi_typ_neu in ('Hostel', 'Jugendherberge')
;


select
	*
	--count(distinct kategorie_neu)
from 
	geo_afo_prod.meta_poi_google_maps_category
where
	hauptkategorie_neu  = 'Hotel & Gastronomie'
	and 
	poi_typ_neu in ('Hostel', 'Jugendherberge')
;

--=======================================
-- Metadata

select 
	*
from 
	google_maps_dev.google_map_metadata_hostel
where 
	n_result is not null 
;


alter table  
	google_maps_dev.google_map_metadata_hostel
add column if not exists datetime DATE,
add column if not exists item_type TEXT,
add column if not exists n_result NUMERIC,
add column if not exists status_message TEXT,
add column if not exists status_code TEXT
;


--Results
select 
	*
from 
	google_maps_dev.google_map_results_hostel
where 
	cid = '6581140534828110929'
;


--Items
select 
	*
from 
	google_maps_dev.google_map_items_hostel
where 
	cid = '6581140534828110929'
;





--Items filtered with category and Hotel/Gastro updated table
create temp table tmp_google_map_hostel
as
select 
	*
from 
	google_maps_dev.google_map_items_hostel t0
where 
	cid not in (
				select 
					cid
				from 
					google_maps_dev.google_map_hotel_gastronomie_v3
	)
	and 
	exists (
    select 1
    from geo_afo_prod.meta_poi_categories_business_data_aktuell as t2
    where
    	t0.category_ids @> to_jsonb(t2.category_id)::jsonb
    	and 
    	t2.hauptkategorie_neu = 'Hotel & Gastronomie'
)  
;

select * from tmp_google_map_hostel;

--======================================================
-- Mapping category_ids >> category_ids into category_ids_de
--======================================================
alter table  
       tmp_google_map_hostel
add column  
       category_ids_de JSONB
;

--category_ids_de aktualisieren:
update  
    tmp_google_map_hostel
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

-- create a temp table to unfold category_ids for mapping to our poi_catgory
drop table if exists tmp_google_hostel;
create temp table 
		tmp_google_hostel
as
select 
	cid,
	exact_match,
	keyword as kw_long,
	split_part(keyword,' ',1) as keyword,
	jsonb_array_elements_text(category_ids_de::jsonb) as category_de_ids,
	jsonb_array_elements_text(category_ids::jsonb) as category_en_ids,
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
	tmp_google_map_hostel
where 
	jsonb_typeof(category_ids::jsonb) = 'array'
;


-- create a temp table for category mapping
drop table tmp_category_mapping;
create temp table tmp_category_mapping
as
select 
	*
from 
	geo_afo_prod.meta_poi_categories_business_data_aktuell
where 
	hauptkategorie_neu = 'Hotel & Gastronomie'
;


select 
	cid
	,STRING_AGG(distinct t0.category_ids_en, ' | ')
	,STRING_AGG(distinct t0.category_ids_de, ' | ')
	,STRING_AGG(distinct t1.kategorie_neu, ' | ')
	,STRING_AGG(distinct t1.poi_typ_neu, ' | ')
from
	tmp_google_hostel t0
left join
	tmp_category_mapping t1
on
	t0.category_ids_en = t1.category_id
group by 
	cid
;


--===============================================
-- Step(1):Use the Mapping Table
-- map poi_typ and kategory_neu 
--===============================================
drop table if exists tmp_googl_missing_hostel;
create temp table tmp_googl_missing_hostel
as
select
	t0.cid
	,t0.exact_match
	,t0.kw_long
	,t0.keyword
	,STRING_AGG(DISTINCT t1.hauptkategorie_neu, ' | ') as afo_hauptkategorie
	,STRING_AGG(DISTINCT t1.poi_typ_neu, ' | ') as afo_poi_typ
	,STRING_AGG(DISTINCT t1.kategorie_neu, ' | ') as afo_category
	,STRING_AGG(DISTINCT t0.category_en_ids, ' | ') as category_en_ids
	,STRING_AGG(DISTINCT t0.category_de_ids, ' | ') as category_de_ids
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
	,category_ids_de
	,category_ids
	,t0.longitude
	,t0.latitude
from 
	tmp_google_hostel t0
left join
	tmp_category_mapping t1
on
	t0.category_ids_en = t1.category_id
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
	,category_ids_de
	,category_ids
;


select * from tmp_googl_missing_hostel;

--==================================
-- step(2) Working_hours (Reformat)
--==================================
alter table tmp_googl_missing_hostel
drop column opening_times
;

alter table tmp_googl_missing_hostel
add column if not exists opening_times text
;

--STATUS
update tmp_googl_missing_hostel
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
        else status
    end
;


-- OPENING HOURS
update tmp_googl_missing_hostel
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
            ''
    end;   
   
-- STATUS: NOT AVAILABLE
update
	tmp_googl_missing_hostel
set 
    status = case 
                when
                	opening_times = '' then 'Not Available'
                else
                	status
             end
;



--==================================
-- step(3) RELEVANT COLUMN
--==================================
-- Add the new 'relevant' column to the table
alter table 
	tmp_googl_missing_hostel
add column 
	relevant numeric
;

-- Update the 'relevant' column based on the specified conditions
update 
	tmp_googl_missing_hostel
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
			        	opening_times = 'N/A'
			        then
			        	0
			        else
			        	25
			    end
	)
 ;

select * from tmp_googl_missing_hostel;


--==================================
-- step(4) PLZ6
--==================================
-- Adding PLZ6 
alter table
	tmp_googl_missing_hostel
add column 
	plz6 text
;


update
	tmp_googl_missing_hostel t0
set
	plz6 = t1.plz6
from
	geo_afo_prod.imp_plz6_geo_neu t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;


--==================================
-- step(5) PLZ4
--==================================
-- Adding PLZ4 zu plz6
alter table
	tmp_googl_missing_hostel
add column 
	plz text
;


update
	tmp_googl_missing_hostel t0
set
	plz = t1.plz
from
	geo_afo_prod.imp_plz6_geo_neu t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;


--==================================
-- step(6) ORT
--==================================
-- add ort to plz6 and plz4 
alter table tmp_googl_missing_hostel
add column plz_ort text;


--update google_maps_dev.google_map_hotel_gastronomie
--set ort = default;

update tmp_googl_missing_hostel t0
set 
	plz_ort = t1.ort
from 
	geo_afo_prod.mv_lay_plz4_aktuell t1 
where
	t0.plz = t1.plz4::text
;

--==================================
-- step(7) GEMEINDE
--==================================
-- Add Gemeinde
alter table
	tmp_googl_missing_hostel
add column 
	gemeinde text,
add column
	gmd_nr numeric
;

update
	tmp_googl_missing_hostel t0
set
	gemeinde = t1.gemeinde
	,gmd_nr = t1.gmd_nr
from
	geo_afo_prod.imp_gmd_geo_neu t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;


--=================================
--Step(8) GWR STRASSE & HAUSNUMMER
--=================================
create index idx_tmp_googl_missing_hostel_poly_lv95 on tmp_googl_missing_hostel using GIST(geo_point_lv95);
create index idx_mv_qu_gbd_gwr_aktuell_point_lv95 on geo_afo_prod.mv_qu_gbd_gwr_aktuell using GIST(geo_point_eg_lv95);
create index idx_mv_lay_gbd_aktuell_point_lv95 on geo_afo_prod.mv_lay_gbd_aktuell using GIST(geo_point_eg_lv95);
create index idx_mv_lay_gbd_aktuell_poly_lv95 on geo_afo_prod.mv_lay_gbd_aktuell using GIST(geo_poly_lv95);

drop table if exists tmp_googl_missing_hostel_v1;
--google_maps_dev.google_map_hotel_gastronomie >> using gwr_layer
--google_maps_dev.google_map_hotel_gastronomie_v1 >> using gbd_layer
create temp table tmp_googl_missing_hostel_v1
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
	        tmp_googl_missing_hostel t0
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


--============================================================================================
--Step(8) ADDRESS CLEANING 
--desired_street_name = 
--         regexp_replace(reverse(trim(SPLIT_PART(reverse(adresse), ',', 2))),'\s+\d.*$', '')
--============================================================================================
-- add a new address block (korr_ ) 
alter table tmp_googl_missing_hostel_v1
add column korr_strasse text,
add column korr_hausnum text,
add column korr_plz4 text,
add column korr_ort text
;

---------------------------------
--column: Korr_plz4
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
	tmp_googl_missing_hostel_v1
where 
	--google_plz4 = plz4
	--google_plz4 <> plz4
	google_plz4 is null
;
--Korr_plz4
update tmp_googl_missing_hostel_v1
set 
	korr_plz4 = plz4
where  
	google_plz4 = plz4
	or 
	google_plz4 is null
;

update tmp_googl_missing_hostel_v1
set 
	korr_plz4 = google_plz4
where  
	google_plz4 <> plz4 
;


-----------------------------------
--column: Korr_Ort
-----------------------------------
select
	korr_ort
	,google_ort 
	,gwr_ort 
	,ort
from
	tmp_googl_missing_hostel_v1
where 
	google_plz4 <> plz4
;

-- Korr_Ort
update tmp_googl_missing_hostel_v1
set 
	korr_ort = ort
where 
	google_ort = ort
	and 
	korr_ort is null 
;

update tmp_googl_missing_hostel_v1
set 
	korr_ort = google_ort
where 
	google_ort <> ort
	and 
	korr_ort is null 
;



--------------------------------------------------------
-- Korr_Strasse: cleaning street name from google data
--------------------------------------------------------

select 
	google_strasse 
	,regexp_replace(reverse(trim(SPLIT_PART(reverse(adresse), ',', 2))),'\s+\d.*$', '') as google_strasse_std
from 
	tmp_googl_missing_hostel_v1
where 
	google_strasse is not null
;

alter table tmp_googl_missing_hostel_v1
add column google_strasse_std text
;

update tmp_googl_missing_hostel_v1
set 
	google_strasse_std = regexp_replace(reverse(trim(SPLIT_PART(reverse(adresse), ',', 2))),'\s+\d.*$', '') 
;

update tmp_googl_missing_hostel_v1
set 
	google_strasse = trim(google_strasse)
;

--=========================================
-- Strasse - Part(1)
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
	tmp_googl_missing_hostel_v1
where 
	google_strasse 		ilike '%'|| gwr_strasse ||'%' 
	or 
	google_strasse_std 	ilike '%'|| gwr_strasse ||'%' 
	or 
	google_strasse 		ilike '%'|| gwr_strasse_std ||'%' 
	or 
	google_strasse_std 	ilike '%'|| gwr_strasse_std ||'%' 
;

update tmp_googl_missing_hostel_v1
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

--=========================================
-- Strasse - part(2)
select 
	korr_strasse 
	,adresse 
	,google_strasse
	,google_strasse_std 
	,gwr_strasse 
	,gwr_strasse_std 
from
	tmp_googl_missing_hostel_v1
where 
	korr_strasse is null
	and 
	google_strasse <> google_strasse_std
	and 
	google_strasse_std ilike '%'|| gwr_strasse ||'%'  
;

update tmp_googl_missing_hostel_v1
set 
	korr_strasse = gwr_strasse 
where 
	korr_strasse is null
	and 
	google_strasse <> google_strasse_std
	and 
	google_strasse_std ilike '%'|| gwr_strasse ||'%' 
;


--=======================================
-- Strasse - part(3) cleaning google_strasse

select 
	korr_strasse 
	,adresse 
	,google_strasse
	,google_strasse_std
	,gwr_strasse 
	,gwr_strasse_std 
from
	tmp_googl_missing_hostel_v1
where 
	korr_strasse is null 
	and
	(
	   lower(google_strasse) ilike '%strasse%'
    or lower(google_strasse) ilike '%rue%'
    or lower(google_strasse) ilike '%av. %'
    or lower(google_strasse) ilike '%avenue%'
    or lower(google_strasse) ilike '%chem. des%'
    or lower(google_strasse) ilike '%chemin des%'
    or lower(google_strasse) ilike '%weg%'
    or lower(google_strasse) ilike '%chem.%'
    or lower(google_strasse) ilike '%rte de%'
    or lower(google_strasse) ilike '%pl.%'
    or lower(google_strasse) ilike '%chemin%'
    or lower(google_strasse) ilike '%rte%'
    or lower(google_strasse) ilike '%les %'
	or lower(google_strasse) ilike '%le %'
	or lower(google_strasse) ilike '%la %'
    or lower(google_strasse) ilike '%prom.%'
    or lower(google_strasse) ilike '%via%'
    or lower(google_strasse) ilike '%esp%'
    or lower(google_strasse) ilike '%rue%'
    or lower(google_strasse) ilike '%quai%'
    or lower(google_strasse) ilike '%route%'
    )
    and -- google_strasse == google_strasse_std
    (
	   lower(google_strasse_std) ilike '%strasse%'
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


update tmp_googl_missing_hostel_v1 t0
set 
	korr_strasse = google_strasse_std 
where 
	korr_strasse is null 
	and
	(
	   lower(google_strasse) ilike '%strasse%'
    or lower(google_strasse) ilike '%rue%'
    or lower(google_strasse) ilike '%av. %'
    or lower(google_strasse) ilike '%avenue%'
    or lower(google_strasse) ilike '%chem. des%'
    or lower(google_strasse) ilike '%chemin des%'
    or lower(google_strasse) ilike '%weg%'
    or lower(google_strasse) ilike '%chem.%'
    or lower(google_strasse) ilike '%rte de%'
    or lower(google_strasse) ilike '%pl.%'
    or lower(google_strasse) ilike '%chemin%'
    or lower(google_strasse) ilike '%rte%'
    or lower(google_strasse) ilike '%les %'
	or lower(google_strasse) ilike '%le %'
	or lower(google_strasse) ilike '%la %'
    or lower(google_strasse) ilike '%prom.%'
    or lower(google_strasse) ilike '%via%'
    or lower(google_strasse) ilike '%esp%'
    or lower(google_strasse) ilike '%rue%'
    or lower(google_strasse) ilike '%quai%'
    or lower(google_strasse) ilike '%route%'
    )
    and
    (
	   lower(google_strasse_std) ilike '%strasse%'
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


select 
	korr_strasse 
	,adresse 
	,google_strasse
	,google_strasse_std
	,gwr_strasse 
	,gwr_strasse_std 
from
	tmp_googl_missing_hostel_v1
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


update tmp_googl_missing_hostel_v1 t0
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

--=======================================
-- Strasse - part(4) mit geo_point 
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
	tmp_googl_missing_hostel_v1
where
	korr_strasse is null  
	and 
	adresse ilike '%'|| gwr_strasse ||'%'
;


update tmp_googl_missing_hostel_v1
set 
	korr_strasse = gwr_strasse 
where 
	korr_strasse is null  
	and 
	adresse ilike '%'|| gwr_strasse ||'%'
;

--===============================================================
-- Strasse - part(5) mit geo_point the rest while gwr is not null 
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
	tmp_googl_missing_hostel_v1
where
	korr_strasse is null  
	and 
	gwr_strasse is not null
;


update tmp_googl_missing_hostel_v1
set 
	korr_strasse = gwr_strasse 
where 
	korr_strasse is null  
	and 
	gwr_strasse is not null
;


--===============================================================
-- Strasse - part(6) the rest while gwr is null 
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
	tmp_googl_missing_hostel_v1
where
	korr_strasse is null  
	and 
	gwr_strasse is null
	and 
	google_strasse_std !~ '^\d+(\.\d+)?$'
	and 
	google_strasse_std <> ''
;


update tmp_googl_missing_hostel_v1
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
--column: Korr_hausnum
--------------------------------------------------------
--part(1)
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
	tmp_googl_missing_hostel_v1
where 
	korr_hausnum is null
	and 
	google_hausnum <> google_plz4 
;


update tmp_googl_missing_hostel_v1
set 
	korr_hausnum = lower(google_hausnum)
where  
	korr_hausnum is null
	and 
	google_hausnum <> google_plz4 
;

--part(2)
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
	tmp_googl_missing_hostel_v1
where 
	korr_hausnum is null
	and 
	gwr_hausnum <> ''
	and 
	(google_hausnum is null or google_hausnum = google_plz4)
	and
	(
	google_strasse 		ilike '%'|| gwr_strasse ||'%' 
	or 
	google_strasse_std 	ilike '%'|| gwr_strasse ||'%' 
	or 
	google_strasse 		ilike '%'|| gwr_strasse_std ||'%' 
	or 
	google_strasse_std 	ilike '%'|| gwr_strasse_std ||'%' 
	)
;


update tmp_googl_missing_hostel_v1
set 
	korr_hausnum = gwr_hausnum
where  
	korr_hausnum is null
	and 
	gwr_hausnum <> ''
	and 
	(google_hausnum is null or google_hausnum = google_plz4)
	and
	(
	google_strasse 		ilike '%'|| gwr_strasse ||'%' 
	or 
	google_strasse_std 	ilike '%'|| gwr_strasse ||'%' 
	or 
	google_strasse 		ilike '%'|| gwr_strasse_std ||'%' 
	or 
	google_strasse_std 	ilike '%'|| gwr_strasse_std ||'%' 
	)
;


--part(3)

select
	korr_hausnum
	,adresse 
	,google_strasse
	,google_strasse_std
	,gwr_strasse
	,gwr_strasse_std
	,google_hausnum 
	,gwr_hausnum 
	,geo_point_google
	,geo_point_gwr
	,distance
from
	tmp_googl_missing_hostel_v1
where 
	korr_hausnum is null
	and 
	gwr_strasse is not null
	and 
	gwr_hausnum <> ''
	and 
	gwr_hausnum is not null
;


update tmp_googl_missing_hostel_v1
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


--===========================================
--
--===========================================
drop table if exists tmp_googl_missing_hostel_v2;

create table temp.tmp_googl_missing_hostel_v2
as
select 
	cid    
	,bezeichnung 
	,category_ids_en 
	,category_ids_de 
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
from 
	tmp_googl_missing_hostel_v1
;

select 
	*
from 
	temp.tmp_googl_missing_hostel_v2
;



--=================================
-- Most updated Hotel/Gastro Table 
--=================================
create table google_maps_dev.google_map_hotel_gastronomie_v4
as
select 
	*
from
	google_maps_dev.google_map_hotel_gastronomie_v3
union all
select 
	*
	,null as category_ids
from 
	temp.tmp_googl_missing_hostel_v2
;


select 
	*
from 
	google_maps_dev.google_map_hotel_gastronomie_v4
where 
	cid = '6581140534828110929'
;


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
	,google_hausnum --1
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
	--,trim(REPLACE(REPLACE(poi_typ, '[', ''), ']', '')) as google_poi_typ
	,poi_typ as google_poi_typ
	,category_ids_en as category_ids --2
	,status
	--,relevant
	,geo_point_google as geo_point_lv95 
from 
	google_maps_dev.google_map_hotel_gastronomie_v4
;

select 
	*
from 
	google_maps_dev_abgleich.google_hotel_gastro
;


--POIS: AFO Hotel und Gastro
create table google_maps_dev_abgleich.afo_poi_typ_hotel_gastro
as
select 
	*
from 
	geo_afo_prod.mv_lay_poi_aktuell 
where 
	poi_typ_id in (
					'1001' 		--Bar
					,'1002' 	--Biergarten
					,'1003'		--Café
					,'1004'		--Fastfood
					,'1005'		--Pub
					,'1006'		--Restaurant
					,'901'		--Hotel
					,'902'		--Motel
					,'903'		--Hostel
					,'904'		--Guesthouse
	)
;




--============================
-- Simon
--============================
SELECT address, public.search_leerschlag(address,1,',')
FROM google_maps_dev_abgleich.google_hotel_gastro
where REGEXP_COUNT(address,',',1)=2
LIMIT 100;





























