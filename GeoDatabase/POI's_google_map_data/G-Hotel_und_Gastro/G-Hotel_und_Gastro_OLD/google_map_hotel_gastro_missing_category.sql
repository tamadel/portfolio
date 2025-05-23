--===========================
-- Missing Pois: Hotel Gastro
--===========================
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
	title like '%Jugendherberge%'
	and 
	address like '%Hagendorn%'



select 
	*
from 
	google_maps_dev.google_map_results 
where
	--category = 'Café'
	--and
	 

	title like '%Jugendherberge%'
	--and 
	--address like '%Hagendorn%'
;

-- nicht gefunden 
--title like '%Jugendherberge Davos%'
--and
--address like '%Horlaubenstrasse 27, 7260 Davos%'
--cid = '6581140534828110929'


--Museums-Café Ziegelei-Museum
--category = 'Café'
--cid = '14096510771510406513' 



-- gefunden missing category 
--title like '%Schlössli%' --category: Self service restaurant || cid: 5355967426012803047
--and 
--address like '%Hünenberg'

--gefunden missing category 
-- title like '%Be Kind%' -- category: Event management company cid:1571267725716919390

--gefunden missing category
--title like '%Thai Garden Imbiss%' -- category: meal_takeaway cid:15539240070381269296

-----------------------------
-- category testen 
-----------------------------
select 
	*
from
	geo_afo_prod.meta_poi_categories_business_data_aktuell	
where 
	category_id like '%jugendherberge%'
;
		
select 
	*
from 
	geo_afo_prod.meta_poi_google_maps_category
where 
	--hauptkategorie_neu = 'Gesundheit'
	 like '%Self service restaurant%'
;

select  
	*
from  
	google_maps_dev.google_map_category_hierarchy
where 
	kategorie_neu like '%Self service restaurant%'
;


--=====================================
-- Kategorie Filter Anpasssung
--=====================================
select 
	*
from
	google_maps_dev.google_map_results 
	
	
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
	

create temp table tmp_missing_category_hotel_gastro
as
select 
	*
from 
	tmp_gogl_restaurant_hotel
where 
	cid not in (
				select 
					cid
				from
					google_maps_dev.google_map_hotel_gastronomie_v2
	)
;




--This line "t0.category_ids @> to_jsonb(t1.category_id)::jsonb" effectively checks if the category_id value from trh exists within the category_ids array in ghg. 
--This allows you to determine if a specific category ID is present within the JSONB array without needing to unnest or flatten the array.
create temp table tmp_google_hotel_gastro_missing
as
select 
	*
from 
	tmp_missing_category_hotel_gastro as t0
where exists (
    select 1
    from geo_afo_prod.meta_poi_categories_business_data_aktuell as t2
    where
    	t0.category_ids @> to_jsonb(t2.category_id)::jsonb
    	and 
    	t2.hauptkategorie_neu = 'Hotel & Gastronomie'
)
;




--============================
-- Mapping category
--===========================
alter table  
       tmp_google_hotel_gastro_missing
add column  
       category_ids_de JSONB
;

--category_ids_de aktualisieren:
update  
    tmp_google_hotel_gastro_missing
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


drop table if exists tmp_google_map_hotel_und_gastro_missing;
create temp table 
		tmp_google_map_hotel_und_gastro_missing
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
	tmp_google_hotel_gastro_missing
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
	tmp_google_map_hotel_und_gastro_missing t0
left join
	tmp_category_mapping t1
on
	t0.category_ids_en = t1.category_id
group by 
	cid
;



--===============================================
-- Step(1):Use the Mapping Table to Create New POI Categories
-- step(2): 
--===============================================
drop table if exists tmp_googl_hotel_gastro_missing_cate;
create temp table tmp_googl_hotel_gastro_missing_cate
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
	tmp_google_map_hotel_und_gastro_missing t0
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

--==================================
-- step(3) Working_hours (Reformat)
--==================================
alter table tmp_googl_hotel_gastro_missing_cate
add column if not exists status_neu text,
add column if not exists opening_times text
;

alter table tmp_googl_hotel_gastro_missing_cate
drop column opening_times
;

update tmp_googl_hotel_gastro_missing_cate
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

update tmp_googl_hotel_gastro_missing_cate
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

   
--==================================
-- table for Peter: Hotel und Gastro
--=================================
drop table if exists tmp_hotel_gastronomie_missing_cate;
create temp table tmp_hotel_gastronomie_missing_cate
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
	tmp_googl_hotel_gastro_missing_cate
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

select 
	*
from 
	tmp_hotel_gastronomie_missing_cate
;


-- point(1) empty categories
alter table 
	tmp_hotel_gastronomie_missing_cate
add column category_id text;




update
	tmp_hotel_gastronomie_missing_cate t1
set
	category_id = sc.category_part
from (
    select  
      t1_orig.cid,
      unnest(string_to_array(REPLACE(REPLACE(t1_orig.category_ids_en, '[', ''), ']', ''), ' | ')) as category_part
    from  
      tmp_hotel_gastronomie_missing_cate t1_orig
) as sc
WHERE 
	t1.cid = sc.cid
 ;

/*
select 
	t1.cid
	,t1.category_ids_en
	,t1.category_ids_de
	,STRING_AGG(DISTINCT t2.kategorie_neu, ' | ') as kateg
	,STRING_AGG(DISTINCT t2.poi_typ_neu, ' | ') as poi
	,t1.category_id
from 
	tmp_hotel_gastronomie_missing_cate t1
join
	geo_afo_prod.meta_poi_categories_business_data_aktuell t2
on
	t1.category_id = t2.category_id 
group by 
	t1.cid
	,t1.category_ids_en
	,t1.category_ids_de
	,t1.category_id
;
*/

update 
	tmp_hotel_gastronomie_missing_cate t1
set 
	poi_typ = t2.poi_typ_neu,
	kategorie = t2.kategorie_neu, 
	hauptkategorie = t2.hauptkategorie_neu 
from 
	geo_afo_prod.meta_poi_categories_business_data_aktuell t2
where 
	t1.category_id = t2.category_id 
	and 
	t2.hauptkategorie_neu = 'Hotel & Gastronomie'
;


delete from tmp_hotel_gastronomie_missing_cate
where
	hauptkategorie is null 
;

select 
	*
from 
	tmp_hotel_gastronomie_missing_cate;



update  
    tmp_hotel_gastronomie_missing_cate t1
set  
    poi_typ = sq.agg_poi_typ,
    kategorie = sq.agg_kategorie
from (
	select  
        cid,
        '[' || array_to_string(array_agg(poi_typ), '/') || ']' as agg_poi_typ,
        '[' || array_to_string(array_agg(kategorie), '/') || ']' as agg_kategorie
     from  
        tmp_hotel_gastronomie_missing_cate
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




update  
    tmp_hotel_gastronomie_missing_cate t1
set  
    poi_typ = regexp_replace(t1.poi_typ, '^\[|\]$', '', 'g')
from
	tmp_hotel_gastronomie_missing_cate
;


update  
    tmp_hotel_gastronomie_missing_cate t1
set  
    kategorie = regexp_replace(t1.kategorie, '^\[|\]$', '', 'g')
from
	tmp_hotel_gastronomie_missing_cate
;


drop table temp.hotel_gastro_missing_category;
create table temp.hotel_gastro_missing_category
as
select 
	*
from 
	tmp_hotel_gastronomie_missing_cate
;
   




select 
	*
from 
	geo_afo_prod.meta_poi_categories_business_data_aktuell
where 
	category_id = 'delivery_chinese_restaurant'--kategorie_neu = 'Bäckerei & Konditorei'  --like '%restaurant%' --'Hotel & Gastronomie'
;


--==================================================
-- Hotel / gastro: clean the missing category table 
--==================================================
select 
	*
from 
	temp.hotel_gastro_missing_category
;


--cleaning opening time 
update
	temp.hotel_gastro_missing_category
set 
    status = case 
                when
                	opening_times = 'N/A' then 'Not Available'
                when 
                	opening_times = 'temporarily_closed' then status
                else
                	'Open'
             end,
    opening_times = case 
                       when  
                       	opening_times = 'N/A' then ' '
                       else
                       	opening_times
                    end
;


-- Adding PLZ6 
alter table
	temp.hotel_gastro_missing_category
add column 
	plz6 text
;


update
	temp.hotel_gastro_missing_category t0
set
	plz6 = t1.plz6
from
	geo_afo_prod.imp_plz6_geo_neu t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;

-- Adding PLZ4 zu plz6
alter table
	temp.hotel_gastro_missing_category
add column 
	plz text
;


update
	temp.hotel_gastro_missing_category t0
set
	plz = t1.plz
from
	geo_afo_prod.imp_plz6_geo_neu t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;


-- Add Gemeinde

alter table
	temp.hotel_gastro_missing_category
add column 
	gemeinde text,
add column
	gmd_nr numeric
;

update
	temp.hotel_gastro_missing_category t0
set
	gemeinde = t1.gemeinde
	,gmd_nr = t1.gmd_nr
from
	geo_afo_prod.imp_gmd_geo_neu t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;




-- define Relevanz formula

-- Add the new 'relevant' column to the table
alter table 
	temp.hotel_gastro_missing_category
add column 
	relevant numeric
;

-- Update the 'relevant' column based on the specified conditions
update 
	temp.hotel_gastro_missing_category
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
			        	strasse is null
			        then
			        	0
			        else
			        	50
			    end
			    +
			    -- Hausnummer
			    case  
			        when 
			        	hausnummer is null
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



--================================
-- Final Table according to Peter
--================================
-- table for adjustments and address cleaning 
create index idx_hotel_gastro_missing_category_poly_lv95 on temp.hotel_gastro_missing_category using GIST(geo_point_lv95);
create index idx_mv_qu_gbd_gwr_aktuell_point_lv95 on geo_afo_prod.mv_qu_gbd_gwr_aktuell using GIST(geo_point_eg_lv95);
create index idx_mv_lay_gbd_aktuell_point_lv95 on geo_afo_prod.mv_lay_gbd_aktuell using GIST(geo_point_eg_lv95);
create index idx_mv_lay_gbd_aktuell_poly_lv95 on geo_afo_prod.mv_lay_gbd_aktuell using GIST(geo_poly_lv95);

drop table if exists temp.google_missing_hotel_gastronomie;

--google_maps_dev.google_map_hotel_gastronomie >> using gwr_layer
--google_maps_dev.google_map_hotel_gastronomie_v1 >> using gbd_layer
create table temp.google_missing_hotel_gastronomie
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
    ,strname_std as gwr_strname_std
    ,deinr_std as gwr_hausnum --gbd_hausnum
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
    ,plz as plz4
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
	        ,t1.strname_std
	        ,t1.deinr_std    --hnr as gbd_hausnum --
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
		    ,t0.plz
		    ,t0.gemeinde
		    ,t0.gmd_nr
	        ,t0.geo_point_lv95
	        ,t1.geo_point_eg_lv95
	        ,ST_Distance(t1.geo_point_eg_lv95, t0.geo_point_lv95) as distance
	        ,ROW_NUMBER() over (partition by t0.cid order by ST_Distance(t1.geo_point_eg_lv95, t0.geo_point_lv95)) as rn
	    from 
	        temp.hotel_gastro_missing_category t0
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



--===================
-- Address cleaning 
-- desired_street_name = regexp_replace(reverse(trim(SPLIT_PART(reverse(adresse), ',', 2))),'\s+\d.*$', '')
--===================

select 
	*
from 
	temp.google_missing_hotel_gastronomie
;


-- again for new version v2 I need to add a new address block (korr_ ) 
alter table temp.google_missing_hotel_gastronomie
add column korr_strasse text,
add column korr_hausnum text,
add column korr_plz4 text,
add column korr_ort text
;


-- add ort to plz6
select
	t0.plz6
	,t0.plz4
	,t1.plz4 
	,t1.ort
from 
	temp.google_missing_hotel_gastronomie t0
	--geo_afo_prod.imp_plz6_geo_neu t0
	--google_maps_dev.google_map_hotel_gastronomie t0
left join
	geo_afo_prod.mv_lay_plz4_aktuell t1 
on
	t0.plz4::text = t1.plz4::text
where 
	plz6::text in (
			select 
				plz6::text
			from
				temp.google_missing_hotel_gastronomie
			where 
				google_plz4::text <> plz4::text
	)
;



alter table temp.google_missing_hotel_gastronomie
add column ort text;


update google_maps_dev.google_map_hotel_gastronomie
set ort = default;

update temp.google_missing_hotel_gastronomie t0
set 
	ort = t1.ort
from 
	geo_afo_prod.mv_lay_plz4_aktuell t1 
where
	t0.plz4 = t1.plz4::text
	--and 
	--t0.google_plz4 = t0.plz
;


--Korr_plz4
update temp.google_missing_hotel_gastronomie
set 
	korr_plz4 = plz4
where  
	google_plz4 = plz4 
;

update temp.google_missing_hotel_gastronomie
set 
	korr_plz4 = google_plz4
where  
	google_plz4 <> plz4 
;



select 
	google_plz4
	,gwr_plz4
	,plz4
	,korr_plz4
from 
	temp.google_missing_hotel_gastronomie
;


-- Korr_Ort
update temp.google_missing_hotel_gastronomie
set 
	korr_ort = ort
where 
	google_ort = ort
	and 
	korr_ort is null 
;

update temp.google_missing_hotel_gastronomie
set 
	korr_ort = google_ort
where 
	google_ort <> ort
	and 
	korr_ort is null 
;


select
	korr_ort
	,google_ort 
	,gwr_ort 
	,ort
from
	temp.google_missing_hotel_gastronomie
where 
	google_ort <> ort
;





--------------------------------------------------------
-- Korr_Strasse: cleaning street name from google data
--------------------------------------------------------

select 
	google_strasse 
	,regexp_replace(regexp_replace(google_strasse, '[/-]', '', 'g'), '\d+', '', 'g') as google_strasse_neu
from 
	temp.google_missing_hotel_gastronomie
where 
	google_strasse is not null
;

update google_maps_dev.google_map_hotel_gastronomie_v2
set 
	korr_strasse = default 
;

update google_maps_dev.google_map_hotel_gastronomie_v2
set 
	google_strasse = regexp_replace(regexp_replace(google_strasse, '[/-]', '', 'g'), '\d+', '', 'g') 
;
 
--update google_maps_dev.google_map_hotel_gastronomie_v2
--set 
	--google_strasse = trim(google_strasse) 
--;
--======================================================================
-- Strasse - Part(1)
select 
	adresse
	,google_strasse
	,gwr_strasse
	,gwr_strname_std
from
	temp.google_missing_hotel_gastronomie
where 
	trim(google_strasse) = gwr_strasse
;

update temp.google_missing_hotel_gastronomie
set 
	korr_strasse = gwr_strasse 
where 
	trim(google_strasse) = gwr_strasse 
; 

--============================================================================
-- Strasse - part(2)
select 
	korr_strasse 
	,adresse
	,google_strasse
	,gwr_strasse
	,gwr_strname_std
from
	temp.google_missing_hotel_gastronomie
where 
	lower(trim(google_strasse)) = gwr_strname_std
	and 
	korr_strasse is null
;

update temp.google_missing_hotel_gastronomie
set 
	korr_strasse = gwr_strasse 
where 
	lower(trim(google_strasse)) = gwr_strname_std
	and 
	korr_strasse is null
; 

--========================================================================================
-- Strasse - part(3) google_strasse is not clean
-- bsp: 1521846410913068066
-- regexp_replace(reverse(trim(SPLIT_PART(reverse(adresse), ',', 2))),'\s+\d.*$', '') AS desired_part


select 
	adresse 
	,korr_strasse 
	,google_strasse 
	,street_name
	,gwr_strasse 
	,google_hausnum 
from 
	temp.google_missing_hotel_gastronomie
where 
	--trim(google_strasse) = trim(street_name)
	--and 
	korr_strasse is null
;


update temp.google_missing_hotel_gastronomie
set 
	korr_strasse = street_name
where 
	trim(google_strasse) = trim(street_name)
	and 
	korr_strasse is null
;

--street_name = regexp_replace(reverse(trim(SPLIT_PART(reverse(adresse), ',', 2))),'\s+\d.*$', '')



--==========================================================
select 
	google_hausnum 
	,gwr_hausnum 
	,korr_hausnum 
from 
	temp.google_missing_hotel_gastronomie
where 
	--trim(google_hausnum) <> trim(gwr_hausnum)
	--and 
	--korr_hausnum  is null
	--and 
	--google_hausnum is not null
;

update temp.google_missing_hotel_gastronomie
set
	korr_hausnum = google_hausnum 
where 
	--trim(google_hausnum) = trim(gwr_hausnum)
	--trim(google_hausnum) <> trim(gwr_hausnum)
	google_hausnum is not null
	and 
	korr_hausnum is null
;

--=======================================================================
-- create the final table 
--(hotel/gastro + missing part according to new category_business list)
--========================================================================

select 
	*
from
	temp.google_missing_hotel_gastronomie
;


select 
	*
from
	temp.tmp_hotel_gastro_v2
;

select 
	*
from 
	google_maps_dev.google_map_hotel_gastronomie_v2
;




create table google_maps_dev.google_map_hotel_gastronomie_v3
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
	,regexp_replace(reverse(trim(SPLIT_PART(reverse(adresse), ',', 2))),'\s+\d.*$', '') as street_name
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
	,plz
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
	temp.google_missing_hotel_gastronomie
union all
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
	,regexp_replace(reverse(trim(SPLIT_PART(reverse(adresse), ',', 2))),'\s+\d.*$', '') as street_name
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
	,plz
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
	google_maps_dev.google_map_hotel_gastronomie_v2
;

select 
	adresse
	,korr_strasse
	,korr_hausnum 
	,google_strasse
	,google_hausnum 
	,street_name
	,gwr_strasse
from
	google_maps_dev.google_map_hotel_gastronomie_v3
where 
	cid = '1584772495961605602'
	korr_strasse like '%Römerstrasse%'
	--korr_strasse like '%,%'
	--korr_strasse ~ '\d'
	--trim(korr_strasse) <> trim(street_name)
	--and 
	--trim(korr_strasse) <> trim(gwr_strasse)
;


select 
	adresse 
	,korr_plz4 
	,plz
	,google_plz4 
	,gwr_plz4 
from 
	google_maps_dev.google_map_hotel_gastronomie_v3
where
    --gwr_plz4 is null
    --and
	--korr_plz4 <> plz 
	--and 
	adresse !~ korr_plz4
	--and 
	--google_plz4 = '1007'
;
	cid = '16349566801690119706'
	category_ids_en like '%hostel%'
	--korr_hausnum is not null
	--korr_strasse ~ ',' -- like '%,%'--~ '\d'
	--bezeichnung like '%jugendherberge%'
	--url like '%http://www.restaurantschloessli.ch/%'
	--url like '%https://www.bekind-family.ch/%'
	--domain like '%www.bekind-family.ch%'
	--domain like '%www.nuengthai.ch%'
	--domain like '%www.ziegelei-museum.ch%'
	--domain like '%www.youthhostel.ch%'
;


update google_maps_dev.google_map_hotel_gastronomie_v3
set korr_plz4  = plz,
	google_plz4 = plz
where
	gwr_plz4 is null
    and
	korr_plz4 <> plz 
	--and 
	--adresse !~ korr_plz4
	and 
	google_plz4 = '1007'
;


select 
	*
from 
	google_maps_dev.google_map_results 
where 
	cid = '14096510771510406513'  --'16349566801690119706'
	--"domain" like '%www.youthhostel.ch%'
;



select 
	*
from 
	google_maps_dev.google_map_hotel_gastronomie_v3
where 
	--cid = '16349566801690119706'
	"domain" like '%www.youthhostel.ch%'
;

--14096510771510406513  Museums-Café Ziegelei-Museum


select 
	*
from
	geo_afo_prod.meta_poi_categories_business_data_aktuell	
where 
	poi_typ_neu like '%Jugendherberge%'
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
	--,korr_hausnum
	,korr_plz4::numeric as plz4
	,korr_ort as ort
	,adresse as address
	,bezeichnung as title
	,google_strasse
	,street_name as google_str_std
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
	,trim(REPLACE(REPLACE(poi_typ, '[', ''), ']', '')) as google_poi_typ
	,category_ids_en as category_ids --2
	,status
	,geo_point_google as geo_point_lv95 
from 
	google_maps_dev.google_map_hotel_gastronomie_v3
;



select 
	--cid
	--,count(*)
	*
from 
	google_maps_dev_abgleich.google_hotel_gastro
where 
	title like '%Albana Hotel%'
;

SELECT *
FROM google_maps_dev_abgleich.google_hotel_gastro
where cid in ('1584772495961605602','16140773433072100578','16349566801690119706')
;


select  
    ST_Transform(ST_SetSRID(ST_MakePoint(9.8337994, 46.806560499999996), 4326), 2056) AS geo_point_lv95;





--===========================================
-- Adjustments: Peter Eamil von 10.NOV 2024
--===========================================
-- Hotel vs Hostel test
select
	*
from 
	google_maps_dev.google_map_hotel_gastronomie_v3
where 
	lower(bezeichnung) like '%hostel%'
	--or
	--lower(kategorie) like '%[hostel]%'
	and
	kategorie <> '[Hotel]'
;




















