--===========================================
-- Hauptkategorie: 'Bau-und Montagegewerbe'
-- 06.11.2024
--===========================================
select 
	*
from 
	geo_afo_prod.meta_poi_google_maps_category
;


INSERT INTO geo_afo_prod.meta_poi_google_maps_category (hauptkategorie_neu, kategorie_neu ,poi_typ_neu)
VALUES 
    ('Bau- und Montagegewerbe', 'Küchenbau/Küchenmontage' ,'Küchenbau'),
    ('Bau- und Montagegewerbe', 'Küchenbau/Küchenmontage' ,'Küchenmontage'),
    ('Bau- und Montagegewerbe', 'Holzbau' ,'Holzbau'),
    ('Bau- und Montagegewerbe', 'Schreinerei/Möbelschreinerei' ,'Schreinerei'),
    ('Bau- und Montagegewerbe', 'Schreinerei/Möbelschreinerei' ,'Möbelschreinerei')
  ;
  
 select 
 	*
 from 
 	geo_afo_prod.meta_poi_google_maps_category
 where 
 	hauptkategorie_neu = 'Bau- und Montagegewerbe'
 ;
 
update geo_afo_prod.meta_poi_google_maps_category
set next_run_date = current_date 
where 
 	hauptkategorie_neu = 'Bau- und Montagegewerbe'
;


-- Metadata 
select 
	*
from 
	google_maps_dev.google_map_metadata_bau_montagegewerbe
where 
	n_result is null
;


alter table  
	google_maps_dev.google_map_metadata_bau_montagegewerbe
add column if not exists datetime DATE,
add column if not exists item_type text,
add column if not exists n_result numeric,
add column if not exists status_message text,
add column if not exists status_code text
;


-- Results
select 
	*
from 
	google_maps_dev.google_map_results_bau_montagegewerbe
;


--Items
select 
	*
from 
	google_maps_dev.google_map_items_bau_montagegewerbe
;



drop table if exists google_maps_dev.google_map_bau_montagegewerbe;
create table google_maps_dev.google_map_bau_montagegewerbe
as
select 
	*
from 
	google_maps_dev.google_map_items_bau_montagegewerbe as t0
where exists (
    select 1
    from geo_afo_prod.meta_poi_categories_business_data_aktuell as t2
    where
    	t0.category_ids @> to_jsonb(t2.category_id)::jsonb
    	and 
    	t2.hauptkategorie_neu = 'Bau- und Montagegewerbe'
)
;


alter table google_maps_dev.google_map_bau_montagegewerbe
add column  
       category_ids_de JSONB
;

update google_maps_dev.google_map_bau_montagegewerbe
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


select
	*
from 
	google_maps_dev.google_map_bau_montagegewerbe
;



create temp table tmp_category_ids_en
as
select
	--cid
	--keyword
	category_ids_en
	,category_ids_de
	,count(*) as n_count
from(
		select 
			cid,
			split_part(keyword,' ',1) as keyword,
			jsonb_array_elements_text(category_ids_de::jsonb) as category_ids_de,
			jsonb_array_elements_text(category_ids::jsonb) as category_ids_en
		from 
			google_maps_dev.google_map_bau_montagegewerbe
		where 
			jsonb_typeof(category_ids::jsonb) = 'array'
) t
group by
	--cid
	--keyword
	category_ids_de
	,category_ids_en
having 
	category_ids_en in ('carpenter', 'kitchen_remodeler', 'cabinet_maker', 'kitchen_furniture_store')
order by 
	n_count desc
;




--table for Peter 
drop table tmp_google_map_bau_montagegewerbe;
create temp table tmp_google_map_bau_montagegewerbe
as
select 
	cid
	,title
	,address
	,strasse
	,plz4
	,ort
	,phone
	,domain
	,url
	,rating->>'value' as google_bewertung
	,rating->>'votes_count' as anz_bewertungen
	,'' as opening_times
	,work_hours->>'current_status' as status
	,total_photos as anz_fotos
	,category
	,additional_categories
	,category_ids
	,category_ids_de
	,work_hours
from 
	google_maps_dev.google_map_bau_montagegewerbe as t0
where exists (
    select 1
    from tmp_category_ids_en as t2
    where
    	t0.category_ids->> 0 = t2.category_ids_en
    	--t0.category_ids @> to_jsonb(t2.category_ids_en)::jsonb
    	--and 
    	--t2.hauptkategorie_neu = 'Bau- und Montagegewerbe'
)
;



alter table tmp_google_map_bau_montagegewerbe
add column opening_times text;

update tmp_google_map_bau_montagegewerbe
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
   

update
	tmp_google_map_bau_montagegewerbe
set 
    status = case 
                when
                	opening_times = ' ' then 'Not Available'
                else
                	status
             end,
    opening_times = case 
                       when  
                       	opening_times = 'N/A' then ' '
                       else
                       	opening_times
                    end
;



-- Add the new 'relevant' column to the table
alter table 
	tmp_google_map_bau_montagegewerbe
add column 
	relevant numeric
;

-- Update the 'relevant' column based on the specified conditions
update 
	tmp_google_map_bau_montagegewerbe
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







select 
	'''' || cid as cid
	,title
	,category_ids
	,category_ids_de
	,address
	,strasse
	,plz4
	,ort
	,'''' || phone as phone
	,domain
	,url
	,google_bewertung
	,anz_bewertungen
	,relevant
	,opening_times
	,status
	,anz_fotos
from 
	tmp_google_map_bau_montagegewerbe
order by
	relevant desc
;

--=================================================
-- second Table for Peter - Adressen Element-Küchen
--=================================================
drop table temp.tmp_adressen_küchen;
create table temp.tmp_adressen_küchen
as
WITH business_scores AS (
    select
    	'''' || cid as cid,
		title,
		category_ids,
		category_ids_de,
		address,
		strasse,
		plz4,
		ort,
		'''' || phone as phone,
		domain,
		url,
		google_bewertung,
		anz_bewertungen,
		anz_fotos,
		relevant,
		opening_times,
		status,
        -- Scoring: Business Name Keywords
		CASE 
		    WHEN title ILIKE '%küche%' THEN 
		        30 + CASE 
		                WHEN title ILIKE '%studio%' OR title ILIKE '%ausstellung%' THEN 30 
		                ELSE 0 
		             END
		    ELSE 0
		END AS name_score,
        --CASE 
          --  WHEN title ILIKE '%Küchestudio%' THEN 50
            --WHEN title ILIKE '%Küchenausstellung%' THEN 50
            --ELSE 0
        --END AS name_score,
        -- Scoring: Presence of "AG" or "GmbH" in Title
        CASE 
            WHEN title ILIKE '% AG%' THEN 40
            WHEN title ILIKE '% GmbH%' THEN 40
            ELSE 0
        END AS legal_entity_score,
        -- Scoring: Website Presence
        CASE 
            WHEN domain IS NOT NULL AND domain NOT ILIKE '%yellow.local.ch%' THEN 10
            ELSE 0
        END AS website_score,
        -- Scoring: Number of Reviews (Ensure anz_bewertungen is cast to INTEGER)
        CASE 
            WHEN COALESCE(anz_bewertungen::INTEGER, 0) >= 50 THEN 10
            WHEN COALESCE(anz_bewertungen::INTEGER, 0) BETWEEN 10 AND 49 THEN 5
            ELSE 0
        END AS reviews_score,
        -- Scoring: Opening Hours
        CASE 
            WHEN opening_times IS NOT NULL AND opening_times != '' THEN 10
            ELSE 0
        END AS opening_hours_score,
        -- Scoring: Number of Photos (Ensure anz_fotos is cast to INTEGER)
        CASE 
            WHEN COALESCE(anz_fotos::INTEGER, 0) >= 10 THEN 10
            WHEN COALESCE(anz_fotos::INTEGER, 0) BETWEEN 1 AND 9 THEN 5
            ELSE 0
        END AS photos_score,
        -- Scoring: Phone Type (Landline or Mobile)
		CASE 
		    WHEN phone ~ '^\+41(?!7[6-9])' THEN 20 -- Landline
		    WHEN phone ~ '^\+417[6-9]' OR phone IS NULL THEN 0 -- Mobile or Missing
		    ELSE 0
		END AS phone_score,
        -- Scoring: Categories (Count the number of elements in category_ids JSONB array)
        jsonb_array_length(category_ids) * 2 AS category_score
        ,geo_point_lv95
    FROM
        tmp_google_map_bau_montagegewerbe
)
-- Combine and Output Total Score
select
	cid,
	title,
	category_ids,
	category_ids_de,
	address,
	strasse,
	plz4,
	ort,
	phone,
	domain,
	url,
	google_bewertung,
	anz_bewertungen,
	relevant,
	opening_times,
	status,
	anz_fotos,
    -- Individual Scores
    name_score,
    legal_entity_score,
    website_score,
    reviews_score,
    opening_hours_score,
    photos_score,
    phone_score,
    category_score,
    -- Total Score
    (
        name_score +
        legal_entity_score + -- Added this score
        website_score +
        reviews_score +
        opening_hours_score +
        photos_score +
        phone_score +
        category_score
    ) AS total_score
FROM
    business_scores
ORDER BY 
	total_score desc
;




--=================================
-- die Anpassungen für Peter
-- Datum: 20.11.2024
--=================================
select 
	phone
	,case  
        when phone ~ '^\+41(?!7[5-9])' then 20 -- Landline
        when phone ~ '^\+417[5-9]' or phone is null then 0 -- Mobile or Missing
        else 0
    end 
from
	temp.tmp_adressen_küchen
where 
	phone is not null
;


--point(1)
--Update name_score    	
update temp.tmp_adressen_küchen
set name_score = 
    case  
        when title ilike '%küche%' then  
            30 + case  
                    when title ilike '%studio%' or title ilike '%ausstellung%' then 30 
                    else 0 
                 end 
        else 0
    end 
;



--point(2)
--update phone_score 
update temp.tmp_adressen_küchen
set phone = REPLACE(phone, '''', '')
;


update temp.tmp_adressen_küchen
set phone_score = 
    case  
        when phone ~ '^\+41(?!7[5-9])' then 20 -- Landline
        when phone ~ '^\+417[5-9]' or phone is null then 0 -- Mobile or Missing
        else 0
    end 
where
	phone is not null 
;



select 
	cid
	,total_score
	,(
        name_score
        + legal_entity_score 
        + website_score 
        + reviews_score 
        + opening_hours_score 
        + photos_score 
        + phone_score 
        + category_score
    ) AS total_2
from 
	temp.tmp_adressen_küchen
;


update temp.tmp_adressen_küchen
set total_score = name_score
		        + legal_entity_score 
		        + website_score 
		        + reviews_score 
		        + opening_hours_score 
		        + photos_score 
		        + phone_score 
		        + category_score
;



--point(3)
--add kanton to dataset 

select 
	*
from 
	geo_afo_prod.mv_lay_kanton_aktuell
;


alter table temp.tmp_adressen_küchen
add column kanton text,
add column kanton_name text
;

select * from temp.tmp_adressen_küchen;

update
	temp.tmp_adressen_küchen t0
set
	kanton = t1.kanton
	,kanton_name = t1.kanton_name
from
	geo_afo_prod.mv_lay_kanton_aktuell t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;


alter table temp.tmp_adressen_küchen
add column geo_poly_lv95_kanton geometry; 

update
	temp.tmp_adressen_küchen t0
set
	geo_poly_lv95_kanton = t1.geo_poly_lv95 
from
	geo_afo_prod.mv_lay_kanton_aktuell t1
where
	t0.kanton = t1.kanton 
;


select
	title
	,strasse
	,count(*)
from 
	temp.tmp_adressen_küchen t0
group by
	title
	,strasse
having 
	count(*) > 1
;	


select 
	*
from 
	temp.tmp_adressen_küchen
where 
	title in (
				 'Sager Christoph'
				, 'Helmut Weber Schreinerei'
				, 'Sümi Schreinerei GmbH'
	)
;

select 
	t0.cid
	,t0.title
	,t0.address
	,t0.ort
	,t0.plz4
	,t1.plz
	,t0.kanton
	,t1.kanton as afo_kanton
	,kanton_name
	,t0.geo_poly_lv95_kanton::geometry 
	,t1.geo_poly_lv95
from 
	temp.tmp_adressen_küchen t0
join
	geo_afo_prod.mv_lay_plz6_aktuell t1
on
	t1.plz::text = t0.plz4
where 
	t0.kanton <> t1.kanton
;


--=================================
-- die Anpassungen für Peter
-- Datum: 26.11.2024
--=================================
-- Legal Entity Score
select 
	title
from (
	select 
		title
		,legal_entity_score
	from 
		temp.tmp_adressen_küchen
	where 
		CONCAT(lower(title), ' ') like '% sa %'
		or 
		CONCAT(lower(title), ' ') like '% sarl %'
		or 
		CONCAT(lower(title), ' ') like '% snc %'
		or 
		CONCAT(lower(title), ' ') like '% scs %'
		or 
		CONCAT(lower(title), ' ') like '% sagl %'
		or 
		CONCAT(lower(title), ' ') like '% sas %'
) t
where 
	title not in (
					select 
						title
					from 
						temp.tmp_adressen_küchen
					where 
						lower(title) like '% sa'
						or 
						lower(title) like '% sarl'
						or 
						lower(title) like '% snc'
						or 
						lower(title) like '% scs'
						or 
						lower(title) like '% sagl'
						or 
						lower(title) like '% sas'
	)
;


/*
 * 	CONCAT(lower(title), ' ') like '% sa'
	or 
	lower(title) like '% sarl'
	or 
	lower(title) like '% snc'
	or 
	lower(title) like '% scs'
	or 
	lower(title) like '% sagl'
	or 
	lower(title) like '% sas'
 */




--SA SARL SNC SCS SAGL SAS
select 
	title
	,legal_entity_score
  	,case  
        when CONCAT(lower(title), ' ') like '% ag %' 	then 40
        when CONCAT(lower(title), ' ') like '% ag, %' 	then 40
        when CONCAT(lower(title), ' ') like '% gmbh %' 	then 40
        when CONCAT(lower(title), ' ') like '% gmbh, %' then 40
        when CONCAT(lower(title), ' ') like '% sa %'	then 40
        when CONCAT(lower(title), ' ') like '% sarl %' 	then 40
        when CONCAT(lower(title), ' ') like '% sàrl %' 	then 40
		when CONCAT(lower(title), ' ') like '% snc %' 	then 40 
		when CONCAT(lower(title), ' ') like '% scs %' 	then 40
		when CONCAT(lower(title), ' ') like '% sagl %' 	then 40
		when CONCAT(lower(title), ' ') like '% sas %' 	then 40
        else 0
    end as legal_neu
from
	temp.tmp_adressen_küchen
where 
	legal_entity_score <> 0
;

SELECT 
    title,
    legal_entity_score,
    case   
        when CONCAT(lower(title), ' ') LIKE '% ag %' 
          or CONCAT(lower(title), ' ') LIKE '% ag.%'
          or CONCAT(lower(title), ' ') LIKE '% ag,%'
          or CONCAT(lower(title), ' ') LIKE '% ag:%'
          or CONCAT(lower(title), ' ') LIKE '% ag-%'
          or CONCAT(lower(title), ' ') LIKE '%-ag %'
          or CONCAT(lower(title), ' ') LIKE '% ag;%' 	THEN 40
        when CONCAT(lower(title), ' ') LIKE '% gmbh %'
          or CONCAT(lower(title), ' ') LIKE '% gmbh.%'
          or CONCAT(lower(title), ' ') LIKE '% gmbh,%'
          or CONCAT(lower(title), ' ') LIKE '% gmbh:%'
          or CONCAT(lower(title), ' ') LIKE '% gmbh-%'
          or CONCAT(lower(title), ' ') LIKE '% gmbh;%' 	THEN 40
        when CONCAT(lower(title), ' ') LIKE '% sa %' 
          or CONCAT(lower(title), ' ') LIKE '% sa.%'
          or CONCAT(lower(title), ' ') LIKE '% sa,%'
          or CONCAT(lower(title), ' ') LIKE '% sa:%'
          or CONCAT(lower(title), ' ') LIKE '% sa-%'
          or CONCAT(lower(title), ' ') LIKE '% sa;%' 	
          or CONCAT(lower(title), ' ') LIKE '% s.a.%' 	THEN 40
        when CONCAT(lower(title), ' ') LIKE '% sarl %' 
          or CONCAT(lower(title), ' ') LIKE '% sarl.%'
          or CONCAT(lower(title), ' ') LIKE '% sarl,%'
          or CONCAT(lower(title), ' ') LIKE '% sarl:%'
          or CONCAT(lower(title), ' ') LIKE '% sarl-%'
          or CONCAT(lower(title), ' ') LIKE '% sarl;%' 	THEN 40
        when CONCAT(lower(title), ' ') LIKE '% sàrl %'
          or CONCAT(lower(title), ' ') LIKE '% sàrl.%'
          or CONCAT(lower(title), ' ') LIKE '% sàrl,%'
          or CONCAT(lower(title), ' ') LIKE '% sàrl:%'
          or CONCAT(lower(title), ' ') LIKE '% sàrl-%'
          or CONCAT(lower(title), ' ') LIKE '% sàrl;%' 	
          or CONCAT(lower(title), ' ') LIKE '% sàrl.%' 
          or CONCAT(lower(title), ' ') LIKE '% s. à r. l.%' THEN 40
        when CONCAT(lower(title), ' ') LIKE '% snc %' 
          or CONCAT(lower(title), ' ') LIKE '% snc.%'
          or CONCAT(lower(title), ' ') LIKE '% snc,%'
          or CONCAT(lower(title), ' ') LIKE '% snc:%'
          or CONCAT(lower(title), ' ') LIKE '% snc-%'
          or CONCAT(lower(title), ' ') LIKE '% snc;%' 	THEN 40
        when CONCAT(lower(title), ' ') LIKE '% scs %' 
          or CONCAT(lower(title), ' ') LIKE '% scs.%'
          or CONCAT(lower(title), ' ') LIKE '% scs,%'
          or CONCAT(lower(title), ' ') LIKE '% scs:%'
          or CONCAT(lower(title), ' ') LIKE '% scs-%'
          or CONCAT(lower(title), ' ') LIKE '% scs;%' 	THEN 40
        when CONCAT(lower(title), ' ') LIKE '% sagl %' 
          or CONCAT(lower(title), ' ') LIKE '% sagl.%'
          or CONCAT(lower(title), ' ') LIKE '% sagl,%'
          or CONCAT(lower(title), ' ') LIKE '%-sagl %'
          or CONCAT(lower(title), ' ') LIKE '% sagl:%'
          or CONCAT(lower(title), ' ') LIKE '% sagl-%'
          or CONCAT(lower(title), ' ') LIKE '% sagl;%' 	THEN 40
        when CONCAT(lower(title), ' ') LIKE '% sas %' 
          or CONCAT(lower(title), ' ') LIKE '% sas.%'
          or CONCAT(lower(title), ' ') LIKE '% sas,%'
          or CONCAT(lower(title), ' ') LIKE '% sas:%'
          or CONCAT(lower(title), ' ') LIKE '% sas-%'
          or CONCAT(lower(title), ' ') LIKE '% sas;%' 	THEN 40
       when CONCAT(lower(title), ' ') LIKE '% spa %' 
          or CONCAT(lower(title), ' ') LIKE '% s.p.a.%'
          or CONCAT(lower(title), ' ') LIKE '% spa,%'
          or CONCAT(lower(title), ' ') LIKE '% spa:%'
          or CONCAT(lower(title), ' ') LIKE '% spa-%'
          or CONCAT(lower(title), ' ') LIKE '% spa;%' 	THEN 40   
        else 0
    end as legal_neu
from  
    temp.tmp_adressen_küchen
where  
    legal_entity_score <> 0;

--===========================
-- (1)update legal_entity_score mit französischen und italienischen Varianten 
select 
	*
from 
	temp.tmp_adressen_küchen
;

update temp.tmp_adressen_küchen
set 
	legal_entity_score = case   
					        when CONCAT(lower(title), ' ') LIKE '% ag %' 
					          or CONCAT(lower(title), ' ') LIKE '% ag.%'
					          or CONCAT(lower(title), ' ') LIKE '% ag,%'
					          or CONCAT(lower(title), ' ') LIKE '% ag:%'
					          or CONCAT(lower(title), ' ') LIKE '% ag-%'
					          or CONCAT(lower(title), ' ') LIKE '%-ag %'
					          or CONCAT(lower(title), ' ') LIKE '% ag;%' 	THEN 40
					        when CONCAT(lower(title), ' ') LIKE '% gmbh %'
					          or CONCAT(lower(title), ' ') LIKE '% gmbh.%'
					          or CONCAT(lower(title), ' ') LIKE '% gmbh,%'
					          or CONCAT(lower(title), ' ') LIKE '% gmbh:%'
					          or CONCAT(lower(title), ' ') LIKE '% gmbh-%'
					          or CONCAT(lower(title), ' ') LIKE '% gmbh;%' 	THEN 40
					        when CONCAT(lower(title), ' ') LIKE '% sa %' 
					          or CONCAT(lower(title), ' ') LIKE '% sa.%'
					          or CONCAT(lower(title), ' ') LIKE '% sa,%'
					          or CONCAT(lower(title), ' ') LIKE '% sa:%'
					          or CONCAT(lower(title), ' ') LIKE '% sa-%'
					          or CONCAT(lower(title), ' ') LIKE '% sa;%' 	
					          or CONCAT(lower(title), ' ') LIKE '% s.a.%' 	THEN 40
					        when CONCAT(lower(title), ' ') LIKE '% sarl %' 
					          or CONCAT(lower(title), ' ') LIKE '% sarl.%'
					          or CONCAT(lower(title), ' ') LIKE '% sarl,%'
					          or CONCAT(lower(title), ' ') LIKE '% sarl:%'
					          or CONCAT(lower(title), ' ') LIKE '% sarl-%'
					          or CONCAT(lower(title), ' ') LIKE '% sarl;%' 	THEN 40
					        when CONCAT(lower(title), ' ') LIKE '% sàrl %'
					          or CONCAT(lower(title), ' ') LIKE '% sàrl.%'
					          or CONCAT(lower(title), ' ') LIKE '% sàrl,%'
					          or CONCAT(lower(title), ' ') LIKE '% sàrl:%'
					          or CONCAT(lower(title), ' ') LIKE '% sàrl-%'
					          or CONCAT(lower(title), ' ') LIKE '% sàrl;%' 	
					          or CONCAT(lower(title), ' ') LIKE '% sàrl.%' 
					          or CONCAT(lower(title), ' ') LIKE '% s. à r. l.%' THEN 40
					        when CONCAT(lower(title), ' ') LIKE '% snc %' 
					          or CONCAT(lower(title), ' ') LIKE '% snc.%'
					          or CONCAT(lower(title), ' ') LIKE '% snc,%'
					          or CONCAT(lower(title), ' ') LIKE '% snc:%'
					          or CONCAT(lower(title), ' ') LIKE '% snc-%'
					          or CONCAT(lower(title), ' ') LIKE '% snc;%' 	THEN 40
					        when CONCAT(lower(title), ' ') LIKE '% scs %' 
					          or CONCAT(lower(title), ' ') LIKE '% scs.%'
					          or CONCAT(lower(title), ' ') LIKE '% scs,%'
					          or CONCAT(lower(title), ' ') LIKE '% scs:%'
					          or CONCAT(lower(title), ' ') LIKE '% scs-%'
					          or CONCAT(lower(title), ' ') LIKE '% scs;%' 	THEN 40
					        when CONCAT(lower(title), ' ') LIKE '% sagl %' 
					          or CONCAT(lower(title), ' ') LIKE '% sagl.%'
					          or CONCAT(lower(title), ' ') LIKE '% sagl,%'
					          or CONCAT(lower(title), ' ') LIKE '%-sagl %'
					          or CONCAT(lower(title), ' ') LIKE '% sagl:%'
					          or CONCAT(lower(title), ' ') LIKE '% sagl-%'
					          or CONCAT(lower(title), ' ') LIKE '% sagl;%' 	THEN 40
					        when CONCAT(lower(title), ' ') LIKE '% sas %' 
					          or CONCAT(lower(title), ' ') LIKE '% sas.%'
					          or CONCAT(lower(title), ' ') LIKE '% sas,%'
					          or CONCAT(lower(title), ' ') LIKE '% sas:%'
					          or CONCAT(lower(title), ' ') LIKE '% sas-%'
					          or CONCAT(lower(title), ' ') LIKE '% sas;%' 	THEN 40
					       when CONCAT(lower(title), ' ') LIKE '% spa %' 
					          or CONCAT(lower(title), ' ') LIKE '% s.p.a.%'
					          or CONCAT(lower(title), ' ') LIKE '% spa,%'
					          or CONCAT(lower(title), ' ') LIKE '% spa:%'
					          or CONCAT(lower(title), ' ') LIKE '% spa-%'
					          or CONCAT(lower(title), ' ') LIKE '% spa;%' 	THEN 40   
					        else 0
					    end
;

--(2) update total_score
update temp.tmp_adressen_küchen
set total_score = name_score
		        + legal_entity_score 
		        + website_score 
		        + reviews_score 
		        + opening_hours_score 
		        + photos_score 
		        + phone_score 
		        + category_score
;

--=======================
-- create final Table
--=======================


select 
	cid 
	,title 
	,category_ids 
	,category_ids_de 
	,address 
	,strasse 
	,plz4 
	,ort 
	,kanton
	--,kanton_name
	,'''' || phone as phone 
	,"domain" 
	,url 
	,google_bewertung 
	,anz_bewertungen 
	,anz_fotos 
	,relevant 
	,opening_times 
	,status 
	,name_score 
	,legal_entity_score 
	,website_score 
	,reviews_score 
	,opening_hours_score 
	,photos_score 
	,phone_score 
	,category_score 
	,total_score 
	,geo_point_lv95 
from
	temp.tmp_adressen_küchen
order by
	total_score desc 
;
































   	
--////////////////////////////////////////////////////////////////////////////////////////////////////   	
select 
	*
from 
	geo_afo_prod.meta_poi_categories_business_data_aktuell 
where 
	category_id like '%kitchen%'
	or
	category_id like '%kitchen_construction%'
	or 
	category_id like '%kitchen_installation'
	or 
	category_id in ('kitchen_fitting', 'kitchen_assembly','cabinetry_installation', 'kitchen_setup', 'kitchen_building')
	or
	category_id like '%carpentry%'
	or
	category_id like '%woodwork%'
	or 
	category_id like '%timber%'
	or 
	category_id like '%cabinet_maker%'
	or 
	category_id like '%carpenter%'
;


select 
	*
from 
	geo_afo_prod.meta_poi_categories_business_data_aktuell 
where 
	category_id like '%carpentry%'
	or
	category_id like '%woodwork%'
	or 
	category_id like '%timber%'
	or 
	category_id like '%wood_construction%'
;


select 
	*
from 
	geo_afo_prod.meta_poi_categories_business_data_aktuell 
where 
	category_id like '%joinery%'
	or
	category_id like '%cabinetmaking%'
	or 
	category_id like '%furniture_making%'
	or 
	category_id like '%carpenter%'
	or 
	category_id like '%furniture_carpentry%'
;



select
	*
from
	geo_afo_prod.meta_poi_categories_business_data_aktuell
;

update geo_afo_prod.meta_poi_categories_business_data_aktuell
set hauptkategorie_neu = 'Bau- und Montagegewerbe'
where
	category_id in (
					'carpenter'
					,'kitchen'
					,'kitchen_furniture_store'
					,'kitchen_remodeler'
					,'kitchen_supply_store'
					,'kitchens'
					,'shared_use_commercial_kitchen'
					,'timber_trade'
					,'woodwork_wholesaler'
					,'woodworker'
					,'woodworking_industry'
					,'woodworking_supply_store'
					,'cabinet_maker'
					,'furniture_maker'
					,'flooring_contractor'
					,'furniture_repair_shop'
					,'wood_floor_installation_service'
					,'woods'
					,'wood_working_class'
					,'wood_supplier'
					,'wood_stove_shop'
					,'wood_industry'
					,'wood_and_laminate_flooring_supplier'
					,'wood_contractor'
					,'wood_floor_installation_service'
					,'wood_floor_refinishing_service'
					,'wood_frame_supplier'
					,'rustic_furniture_store'	
)
;

