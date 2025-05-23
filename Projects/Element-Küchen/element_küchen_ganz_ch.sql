
drop table if exists
	element_küchen.google_map_bau_montagegewerbe;

create table
	element_küchen.google_map_bau_montagegewerbe
as
SELECT 
	*
FROM 
	dblink(
		'geo_database',
		$POSTGRES$
			SELECT 	  
				*
			FROM 
				element_küchen.google_map_bau_montagegewerbe
		$POSTGRES$
	) AS mv_lay_poi_aktuell (
		cid text
		,rank_absolute int4
		,keyword text
		,poi_typ text
		,exact_match int8
		,plz4 text
		,ort text
		,strasse text
		,country_code text
		,address text
		,title text
		,phone text
		,"domain" text
		,url text
		,rating jsonb
		,total_photos int4
		,hotel_rating float8
		,category text
		,additional_categories jsonb
		,category_ids jsonb
		,work_hours jsonb
		,geo_point_lv95 public.geometry
		,longitude float8
		,latitude float8
		,category_ids_de jsonb 
	)
;


-- table 2
drop table if exists
	element_küchen.element_küchen_v2;

create table
	element_küchen.element_küchen_v2
as
SELECT 
	*
FROM 
	dblink(
		'geo_database',
		$POSTGRES$
			SELECT 	  
				*
			FROM 
				temp.element_küchen_v2
		$POSTGRES$
	) AS mv_lay_poi_aktuell (
		cid text ,
		title text ,
		category_ids jsonb ,
		category_ids_de jsonb ,
		address text ,
		strasse text ,
		plz4 text ,
		ort text ,
		kanton text ,
		phone text ,
		"domain" text ,
		url text ,
		google_bewertung text ,
		anz_bewertungen text ,
		anz_fotos int4 ,
		relevant numeric ,
		opening_times text ,
		status text ,
		name_score int4 ,
		legal_entity_score int4 ,
		website_score int4 ,
		reviews_score int4 ,
		opening_hours_score int4 ,
		photos_score int4 ,
		phone_score int4 ,
		category_score int4 ,
		total_score int4 ,
		geo_point_lv95 public.geometry 
	)
;




--===========================
-- Projekt: Element-Küchen
-- Ganz Schweiz
-- 03.01.2025
--===========================
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
			element_küchen.google_map_bau_montagegewerbe
		where 
			jsonb_typeof(category_ids::jsonb) = 'array'
) t
group by
	--cid
	--keyword
	category_ids_de
	,category_ids_en
having 
	category_ids_en in ('carpenter', 'kitchen_remodeler', 'cabinet_maker', 'kitchen_furniture_store', 'furniture_maker', 'interior_fitting_contractor')
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
	,geo_point_lv95
from 
	element_küchen.google_map_bau_montagegewerbe as t0
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
drop table element_küchen.tmp_adressen_küchen;
create table element_küchen.tmp_adressen_küchen
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
    ,geo_point_lv95
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
	element_küchen.tmp_adressen_küchen
where 
	phone is not null
;


--point(1)
--Update name_score    	
update element_küchen.tmp_adressen_küchen
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
update element_küchen.tmp_adressen_küchen
set phone = REPLACE(phone, '''', '')
;


update element_küchen.tmp_adressen_küchen
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
	element_küchen.tmp_adressen_küchen
;


update element_küchen.tmp_adressen_küchen
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


alter table element_küchen.tmp_adressen_küchen
add column kanton text,
add column kanton_name text
;



select 
	*
from 
	element_küchen.tmp_adressen_küchen;

update
	element_küchen.tmp_adressen_küchen t0
set
	kanton = t1.kanton
	,kanton_name = t1.kanton_name
from
	geo_afo_prod.mv_lay_kanton_aktuell t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;


alter table element_küchen.tmp_adressen_küchen
add column geo_poly_lv95_kanton geometry; 

update
	element_küchen.tmp_adressen_küchen t0
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
	element_küchen.tmp_adressen_küchen t0
group by
	title
	,strasse
having 
	count(*) > 1
;	


select 
	*
from 
	element_küchen.tmp_adressen_küchen
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
	element_küchen.tmp_adressen_küchen t0
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
		element_küchen.tmp_adressen_küchen
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
						element_küchen.tmp_adressen_küchen
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
	element_küchen.tmp_adressen_küchen
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
    element_küchen.tmp_adressen_küchen
where  
    legal_entity_score <> 0;

--===========================
-- (1)update legal_entity_score mit französischen und italienischen Varianten 
select 
	*
from 
	element_küchen.tmp_adressen_küchen
;

update element_küchen.tmp_adressen_küchen
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
update element_küchen.tmp_adressen_küchen
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
	element_küchen.tmp_adressen_küchen
where
	cid in(
				select 
					cid
				from 
					element_küchen.element_küchen_v2
	)
order by
	total_score desc
;



--==================================
-- missing PLZ4 by adding afo_plz4
--==================================
-- Adding afo PLZ4
alter table
	element_küchen.tmp_adressen_küchen
add column 
	afo_plz4 text,
add column 
	afo_ort text
;

-- add afo plz4 and ORT
update
	element_küchen.tmp_adressen_küchen t0
set
	afo_plz4 = t1.plz4::text,
	afo_ort = t1.ort 
from
	geo_afo_prod.mv_lay_plz4_aktuell  t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;

-- update missing plz4
update element_küchen.tmp_adressen_küchen
set 
	plz4 = afo_plz4
where  
	plz4 is null
;



/*
select
	cid
	,title 
	,address 
	,plz4
	,afo_plz4
	,ort 
	,afo_ort  
from
	element_küchen.tmp_adressen_küchen
where 
	--google_plz4 = plz4
	--plz4 <> afo_plz4 -- 59
	--plz4 is null  --30
;*/


select 
	*
from 
	element_küchen.tmp_adressen_küchen
;

create table element_küchen.element_küchen_v3
as
select
	*
from 
	element_küchen.tmp_adressen_küchen
;
	



--==================================
-- missing strasse 
--==================================
create index idx_tmp_adressen_küchen_poly_lv95 on element_küchen.tmp_adressen_küchen using GIST(geo_point_lv95);
create index idx_mv_qu_gbd_gwr_aktuell_point_lv95 on geo_afo_prod.mv_qu_gbd_gwr_aktuell using GIST(geo_point_eg_lv95);

drop table if exists element_küchen.tmp_adressen_küchen_gwr;

create table element_küchen.tmp_adressen_küchen_gwr
as
select 
    cid 
	,title 
	,category_ids 
	,category_ids_de 
	,address 
	,strasse 
	,plz4 
	,ort 
	,gwr_strasse  
    ,gwr_strasse_std 
    ,gwr_hausnum  
    ,gwr_plz4  
    ,gwr_ort  
    ,distance
	,phone 
	,"domain" 
	,url 
	,google_bewertung 
	,anz_bewertungen 
	,relevant 
	,opening_times 
	,status 
	,anz_fotos 
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
	,geo_point_lv95_gwr
	,kanton
	,kanton_name
	,afo_plz4
	,afo_ort
from (
		select  
	           	t0.cid 
				,t0.title 
				,t0.category_ids 
				,t0.category_ids_de 
				,t0.address 
				,t0.strasse 
				,t0.plz4 
				,t0.ort
				,t1.strname 											as gwr_strasse       	
		        ,t1.strname_std 										as gwr_strasse_std
		        ,t1.deinr_std 											as gwr_hausnum   		
		        ,t1.dplz4   											as gwr_plz4 			
		        ,t1.dplzname 											as gwr_ort	
				,t0.phone 
				,t0."domain" 
				,t0.url 
				,t0.google_bewertung 
				,t0.anz_bewertungen 
				,t0.relevant 
				,t0.opening_times 
				,t0.status 
				,t0.anz_fotos 
				,t0.name_score 
				,t0.legal_entity_score 
				,t0.website_score 
				,t0.reviews_score 
				,t0.opening_hours_score 
				,t0.photos_score 
				,t0.phone_score 
				,t0.category_score 
				,t0.total_score 
				,t0.geo_point_lv95										
				,t1.geo_point_eg_lv95									as geo_point_lv95_gwr
				,t0.kanton
				,t0.kanton_name
				,t0.geo_poly_lv95_kanton
				,t0.afo_plz4
				,t0.afo_ort
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
	        element_küchen.tmp_adressen_küchen t0
	    left join  
	        geo_afo_prod.mv_qu_gbd_gwr_aktuell t1 
	    on 
	        ST_DWithin(t1.geo_point_eg_lv95, t0.geo_point_lv95, 20)
) t
where 
    rn = 1
;


select * from element_küchen.tmp_adressen_küchen_gwr;

-- check missing strasse
select
	cid
	,title
	,address
	,strasse
	,gwr_strasse
	,gwr_strasse_std
	,gwr_hausnum
	,plz4
	,gwr_plz4
	,ort
	,gwr_ort
	,distance
	,geo_point_lv95
	,geo_point_lv95_gwr
from 
	element_küchen.tmp_adressen_küchen_gwr
where 
	strasse is null
;

--test results 
select 
	cid
	,title
	,strasse
	,trim(coalesce(t1.gwr_strasse, ' ')||' '|| coalesce(t1.gwr_hausnum, '')) as korr_str
from 
	element_küchen.tmp_adressen_küchen_gwr t1
where  
	strasse = '1 30'
; 

--update missing strasse
update element_küchen.tmp_adressen_küchen as t0
set 
	strasse = trim(coalesce(t1.gwr_strasse, ' ')||' '|| coalesce(t1.gwr_hausnum, ''))
from 
	element_küchen.tmp_adressen_küchen_gwr t1
where  
	t1.strasse = '1 30'
	and 
	t0.strasse = '1 30'
;


select * from element_küchen.tmp_adressen_küchen;





drop table if exists element_küchen.tmp_element_küchen_v3;
create table element_küchen.tmp_element_küchen_v3 
as 
select 
    cid,
    title,
    address,
    null as korr_strasse,
    null as korr_hausnum,
    strasse,
    -- Extract street name
    TRIM(
        case  
            -- Case: House number at the beginning with a comma
            when strasse ~ '^\d+,' then TRIM(SUBSTRING(strasse from ',(.*)'))
            -- General case: Extract everything before the last space followed by numbers or special patterns
            when strasse ~ '^(.*)\s\d+.*$' then TRIM(SUBSTRING(strasse from '^(.*)\s\d+.*$'))
            -- Default: If no pattern matches, return the whole string
            else strasse
        end 
    ) as str_name,
    null as gwr_strasse,
    null as gwr_strasse_std, 
    -- Extract house number
    TRIM(
        case  
            -- Case: House number at the beginning with a comma
            when strasse ~ '^\d+,' then TRIM(SUBSTRING(strasse from  '^(\d+),'))
            -- General case: Extract house number at the end
            when strasse ~ '\s(\d+.*)$' then TRIM(SUBSTRING(strasse from '\s(\d+.*)$'))
            -- Default: Null if no house number is detected
            else null 
        end 
    ) as hausnum,
    null as gwr_hausnum,
    plz4,
    ort,
    geo_point_lv95
from 
    element_küchen.tmp_adressen_küchen
;



alter table element_küchen.tmp_element_küchen_v3
--add column geo_point_lv95_gwr geometry
add column distance float8
;


select * from element_küchen.tmp_element_küchen_v3;



--====================
-- Korr_Strasse
--====================
update element_küchen.tmp_element_küchen_v3 as t0
set 
	gwr_strasse = t1.gwr_strasse,
	gwr_strasse_std = t1.gwr_strasse_std,
	gwr_hausnum = t1.gwr_hausnum,
	geo_point_lv95_gwr = t1.geo_point_lv95_gwr,
	distance = t1.distance
from 
	element_küchen.tmp_adressen_küchen_gwr t1
where 
	t0.cid = t1.cid
;


update element_küchen.tmp_element_küchen_v3
set 
	korr_strasse = default,
	korr_hausnum = default
;	


select 
	cid
	,address
	,strasse
	,korr_strasse
	,str_name
	,gwr_strasse
	,gwr_strasse_std
	,distance
	,geo_point_lv95
	,geo_point_lv95_gwr
from 
	element_küchen.tmp_element_küchen_v3
where 
	lower(gwr_strasse) not ilike '%'|| lower(str_name) ||'%' 

	--lower(str_name) ~ lower(gwr_strasse)
	--gwr_strasse is null
	--or
	--gwr_strasse = ''
	korr_strasse is null
	--and 
	--gwr_strasse <> str_name 
;

-- (1) update korr_strasse with gwr_strasse where   str_name = gwr_strasse
-- 5565
update element_küchen.tmp_element_küchen_v3
set 
	korr_strasse = gwr_strasse
where 
	lower(str_name) ilike '%'|| lower(gwr_strasse) ||'%'
;

--(2) update korr_strasse where gwr_strasse is null will take strasse name
--607
update element_küchen.tmp_element_küchen_v3
set 
	korr_strasse = str_name
where 
	korr_strasse is null
	and
	gwr_strasse is null
;
 
--(3) gwr_strasse will takeover >> in case str_name not like gwr_strasse
--846
update element_küchen.tmp_element_küchen_v3
set 
	korr_strasse = gwr_strasse
where 
	korr_strasse is null
	and 
	lower(gwr_strasse) not ilike '%'|| lower(str_name) ||'%' 
;


update element_küchen.tmp_element_küchen_v3
set 
	korr_strasse = gwr_strasse
where 
	korr_strasse is null
	and 
	lower(gwr_strasse) ilike '%'|| lower(str_name) ||'%' 
;


--====================
-- Korr_Hausnummer
--====================
select 
	cid
	,address
	,strasse
	,korr_strasse
	,str_name
	,korr_hausnum
	,hausnum
	,gwr_hausnum
	,gwr_strasse
	,distance
	,geo_point_lv95
	,geo_point_lv95_gwr
from 
	element_küchen.tmp_element_küchen_v3
where 
	korr_hausnum is null
;
	
	--korr_hausnum is null
	--and 
	--lower(str_name) not ilike '%'|| lower(gwr_strasse) ||'%' 
	--and 
	--lower(hausnum) not ilike '%'|| lower(gwr_hausnum) ||'%' 
	--and 
	--gwr_hausnum ~ '^\d+$';
	--and 
	--hausnum !~ '^\d+[a-zA-Z]?$';

update element_küchen.tmp_element_küchen_v3
set 
	korr_hausnum = gwr_hausnum
where 
	korr_hausnum is null
	and
	hausnum is null 	
;

update element_küchen.tmp_element_küchen_v3
set 
	korr_hausnum = hausnum
where 
	korr_hausnum is null 	
;


select * from element_küchen.tmp_element_küchen_v3 where hausnum !~ '^\d+[a-zA-Z]?$';

/*
--(1) update korr_hausnum with gwr_hausnum where   hausnum = gwr_hausnum
-- 5128
update element_küchen.tmp_element_küchen_v3
set 
	korr_hausnum = gwr_hausnum
where 
	lower(gwr_hausnum) ilike '%'|| lower(hausnum) ||'%'
;

--(2) update korr_hausnum where gwr_hausnum is null will take hausnum
--603
update element_küchen.tmp_element_küchen_v3
set 
	korr_hausnum = hausnum
where 
	korr_hausnum is null 
	and 
	gwr_hausnum is null
;

--(3) hausnum will takeover >> in case str_name like gwr_strasse and gwr_hausnum <> hausnum
--398
update element_küchen.tmp_element_küchen_v3
set 
	korr_hausnum = hausnum
where 
	korr_hausnum is null
	and 
	lower(str_name) ilike '%'|| lower(gwr_strasse) ||'%' 
	and 
	lower(hausnum) not ilike '%'|| lower(gwr_hausnum) ||'%' 
	and 
	gwr_hausnum ~ '^\d+$';
;

update element_küchen.tmp_element_küchen_v3
set 
	korr_hausnum = hausnum
where
	korr_hausnum is null
;	

*/


--=================
--final Table
--=================
select * from element_küchen.element_küchen_v3 where url ilike '%yellow.local.ch%'; 

drop table if exists element_küchen.element_küchen_v3;
create table element_küchen.element_küchen_v3
as
select 
	t0.cid 
	,t0.title 
	,t0.category_ids 
	,t0.category_ids_de 
	,t0.address 
	,t0.strasse
	,trim(coalesce(t1.korr_strasse, ' ')||' '|| coalesce(t1.korr_hausnum, '')) as korr_strasse
	,t1.korr_strasse as korr_str_name
	,t1.korr_hausnum
	,t1.gwr_strasse
	,t1.gwr_hausnum
	,t0.plz4 
	,t0.ort 
	,t0.kanton
	,'''' || t0.phone  as phone 
	,t0."domain" 
	,t0.url 
	,t0.google_bewertung 
	,t0.anz_bewertungen 
	,t0.anz_fotos 
	,t0.relevant 
	,t0.opening_times 
	,t0.status 
	,t0.name_score 
	,t0.legal_entity_score 
	,t0.website_score 
	,t0.reviews_score 
	,t0.opening_hours_score 
	,t0.photos_score 
	,t0.phone_score 
	,t0.category_score 
	,t0.total_score 
	,t0.geo_point_lv95 
from
	element_küchen.tmp_adressen_küchen t0
join
	element_küchen.tmp_element_küchen_v3 t1
on
	t0.cid = t1.cid
order by 
	total_score desc
;






/*
select * from element_küchen.element_küchen_v3 where trim(opening_times) = '';

update element_küchen.element_küchen_v3
set  
    status = 'Not Available',
    opening_hours_score = 0,
    total_score = total_score - 10
where  
    TRIM(opening_times) = '' or opening_times is null
;
*/










































   	
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



















