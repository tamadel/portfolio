--==========================
-- AZD Test Data
-- Date: 20.01.2025
--==========================
--(I)Process Search.ch Data
--1385
select 
	* 
from 
	az_testdata.az_search_url
where 
    type = 'organic' 
    and
   	domain in ('search.ch', 'www.search.ch')
;
-- Step 1: Create a temp table for split URLs
--1385
drop table if exists temp_split_url;
create temp table 
	temp_split_url 
as	
select    
    bestandid,
    name1 as original_name,
    title as google_name,
    zuststrasse as original_address,
    zustplz as plz,
    zustort as original_ort,
    uid_no,
    telefon,
    url,
    domain,
    rank_absolute as rank,
    regexp_replace(url, '^https?://', '') as cleaned_url,
    array_remove(string_to_array(regexp_replace(url, '^https?://', ''), '/'), '') as url_parts
from  
    az_testdata.az_search_url
where 
    type = 'organic' 
    and
   	domain in ('search.ch', 'www.search.ch')
;

select * from temp_split_url;


-- Step 2: Create a temp table for normalized data
-- 6515
drop table if exists temp_normalized_url;
create temp table
	temp_normalized_url 
as 
select 
    bestandid,
    original_name,
    original_address,
    plz,
    original_ort,
    uid_no,
    telefon,
    url,
    domain,
    rank,
    cleaned_url,
    part_number,
    part_value,
    -- Normalize German special characters and replace hyphens in part_value
    regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace(
                  regexp_replace(
                    regexp_replace(
                      regexp_replace(
                        regexp_replace(
                          lower(part_value), 
                          '-', ' ', 'g' 
                        ), 
                        'ä', 'ae', 'g'
                      ), 
                      'ö', 'oe', 'g'
                    ), 
                    'ü', 'ue', 'g'
                  ),
                  'ß', 'ss', 'g'
                ),
                'é', 'e', 'g'
              ),
              'è', 'e', 'g'
            ),
            'ê', 'e', 'g'
          ),
          'ç', 'c', 'g'
        ),
        'à', 'a', 'g'
      ) AS normalized_part_value,
    -- Normalize German special characters and replace hyphens in original_name
      regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace(
                  regexp_replace(
                    regexp_replace(
                      regexp_replace(
                        regexp_replace(
                          lower(original_name), -- Convert to lowercase
                          '-', ' ', 'g' -- Replace hyphens with spaces
                        ), 
                        'ä', 'ae', 'g'
                      ), 
                      'ö', 'oe', 'g'
                    ), 
                    'ü', 'ue', 'g'
                  ),
                  'ß', 'ss', 'g'
                ),
                'é', 'e', 'g'
              ),
              'è', 'e', 'g'
            ),
            'ê', 'e', 'g'
          ),
          'ç', 'c', 'g'
        ),
        'à', 'a', 'g'
      ) as normalized_original_name,
    -- Normalize German special characters and replace hyphens in original_address
       regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace(
                  regexp_replace(
                    regexp_replace(
                      regexp_replace(
                        regexp_replace(
                          lower(original_address), 
                          '-', ' ', 'g' 
                        ), 
                        'ä', 'ae', 'g'
                      ), 
                      'ö', 'oe', 'g'
                    ), 
                    'ü', 'ue', 'g'
                  ),
                  'ß', 'ss', 'g'
                ),
                'é', 'e', 'g'
              ),
              'è', 'e', 'g'
            ),
            'ê', 'e', 'g'
          ),
          'ç', 'c', 'g'
        ),
        'à', 'a', 'g'
      ) as normalized_original_address,
    -- Normalize German special characters and replace hyphens in original_ort
       regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace(
                  regexp_replace(
                    regexp_replace(
                      regexp_replace(
                        regexp_replace(
                          lower(original_ort), 
                          '-', ' ', 'g' 
                        ), 
                        'ä', 'ae', 'g'
                      ), 
                      'ö', 'oe', 'g'
                    ), 
                    'ü', 'ue', 'g'
                  ),
                  'ß', 'ss', 'g'
                ),
                'é', 'e', 'g'
              ),
              'è', 'e', 'g'
            ),
            'ê', 'e', 'g'
          ),
          'ç', 'c', 'g'
        ),
        'à', 'a', 'g'
      ) as normalized_original_ort
from  
    temp_split_url,
    unnest(url_parts) with ordinality as t(part_value, part_number)
;

select * from temp_normalized_url;


-- Step 3: excute similarity and produce final results
select  
    bestandid,
    original_name,
    original_address,
    original_ort,
    url,
    domain,
    rank,
    cleaned_url,
    part_number,
    part_value,
    -- Check similarity between normalized_original_ort and part_number 3
    case  
        when
        	part_number = 3 and similarity(normalized_original_ort, normalized_part_value) >= 0.3 
        then
        	similarity(normalized_original_ort, normalized_part_value)
        else
        	null 
    end as ort_similarity,
    -- Check similarity between normalized_original_address and part_number 4
    case  
        when
        	part_number = 4 and similarity(normalized_original_address, normalized_part_value) >= 0.3 
        then
        	similarity(normalized_original_address, normalized_part_value)
        else
        	null 
    end as address_similarity,
     -- Check similarity between normalized_original_name and part_number 5
    case  
        when
        	part_number = 5 and similarity(normalized_original_name, normalized_part_value) >= 0.3 
        then
        	similarity(normalized_original_name, normalized_part_value)
        else
        	null 
    end as name_similarity
from  
    temp_normalized_url
order by 
    rank
;


--=========================
-- Word-to-Word comparison
--=========================
-----------------------------------------------
--Split Words from Each Field into Temp Tables
-----------------------------------------------
-- Step 2.1: Split words from part_value into a temp table
drop table if exists temp_part_value_words;
create temp table 
	temp_part_value_words 
as	
select 
    bestandid,
    part_number,
    part_value,
    word,
    ordinality
   -- regexp_split_to_table(normalized_part_value, '\s+') as word -- Split into words
from
	temp_normalized_url ,
	regexp_split_to_table(normalized_part_value, '\s+') with ordinality as t(word, ordinality)
;

select * from temp_part_value_words;

-- Step 2.2: Split words from original_name into a temp table
drop table if exists temp_original_name_words;
create temp table 
	temp_original_name_words 
as 
select 
    bestandid,
    part_number,
    part_value,
    word,
    ordinality
    --regexp_split_to_table(normalized_original_name, '\s+') as word -- Split into words
from
	temp_normalized_url,
	regexp_split_to_table(normalized_original_name, '\s+') with ordinality as t(word, ordinality)
;

select * from temp_original_name_words;

-- Step 2.3: Split words from original_address into a temp table
drop table if exists temp_original_address_words;
create temp table
	temp_original_address_words 
as 
select 
    bestandid,
    part_number,
    part_value,
    word,
    ordinality
    --regexp_split_to_table(normalized_original_address, '\s+') as word -- Split into words
from
	temp_normalized_url,
	regexp_split_to_table(normalized_original_address, '\s+') with ordinality as t(word, ordinality)
;

select * from temp_original_address_words;

-- Step 2.4: Split words from original_ort into a temp table
drop table if exists temp_original_ort_words;
create temp table 
	temp_original_ort_words 
as 
select 
    bestandid,
    part_number,
    part_value,
    word,
    ordinality
    --regexp_split_to_table(normalized_original_ort, '\s+') as word -- Split into words
from 
	temp_normalized_url,
	regexp_split_to_table(normalized_original_ort, '\s+') with ordinality as t(word, ordinality)
;

select * from temp_original_ort_words;



----------------------------------
--Compare Words and Find Matches
----------------------------------
-- Step 3.1: Compare part_value words with original_name words
drop table if exists temp_name_matches;
create temp table
	temp_name_matches 
as 
select distinct 
    p.bestandid,
    p.part_number,
    p.part_value,
    p.ordinality,
    p.word as matched_name_word,
    n.word as original_name_word
from  
    temp_part_value_words p
join  
    temp_original_name_words n
on  
   	p.bestandid = n.bestandid 
   	and 
   	p.word ilike n.word
;

select * from temp_name_matches;

-- Step 3.2: Compare part_value words with original_address words
drop table if exists temp_address_matches;
create temp table
	temp_address_matches
as
select  distinct
    p.bestandid,
    p.part_number,
    p.part_value,
    p.ordinality,
    p.word as matched_address_word,
    a.word as original_address_word
from  
    temp_part_value_words p
join  
    temp_original_address_words a
on  
    p.bestandid = a.bestandid 
   	and
  	p.word ilike a.word
;

select * from temp_address_matches;

-- Step 3.3: Compare part_value words with original_ort words
drop table if exists temp_ort_matches;
create temp table
	temp_ort_matches
as
select distinct 
    p.bestandid,
    p.part_number,
    p.part_value,
    p.ordinality,
    p.word as matched_ort_word,
    o.word as original_ort_word
from  
    temp_part_value_words p
join  
    temp_original_ort_words o
on  
    p.bestandid = o.bestandid 
    and
  	p.word ilike o.word
;

select * from temp_ort_matches;



---------------------------------------------------------------------------
-- aggregate name matches with DISTINCT to remove duplicates and ordinality
---------------------------------------------------------------------------

-----------------------
-- Name Match
-----------------------
-- Deduplicate matched words for name while keep their order
drop table if exists temp_name_matches_deduplicated;
create temp table
	temp_name_matches_deduplicated 
as 
select distinct on (bestandid, matched_name_word, part_value) -- Deduplicate by bestandid and word
    bestandid,
    matched_name_word,
    part_value,
    part_number,
    ordinality -- keep the original order
from  
    temp_name_matches
order by  
    bestandid, 
    matched_name_word,
    part_value,
    part_number,
    ordinality
 ; 

select * from temp_name_matches_deduplicated;
   
-- Aggregate deduplicated name matches   
drop table if exists temp_aggregated_name_matches;   
create temp table
	temp_aggregated_name_matches 
as 
select
    bestandid,
    part_value,
    part_number,
    STRING_AGG(matched_name_word, ' ' order by ordinality) as matched_name -- Aggregate in correct order
from  
    temp_name_matches_deduplicated
group by  
    bestandid,
    part_value,
    part_number
 ;

select * from temp_aggregated_name_matches;


-----------------------
-- Address Match
-----------------------
-- Deduplicate matched words for address while keep their order
drop table if exists temp_address_matches_deduplicated;
create temp table 
	temp_address_matches_deduplicated 
as 
select distinct on (bestandid, matched_address_word, part_value) -- Deduplicate by bestandid and word
    bestandid,
    matched_address_word,
    part_value,
    ordinality -- keep the original order
from  
    temp_address_matches
order by  
    bestandid, 
    matched_address_word,
    part_value,
    ordinality
;

select * from temp_address_matches_deduplicated;

-- Aggregate deduplicated address matches
drop table if exists temp_aggregated_address_matches;
create temp table 
	temp_aggregated_address_matches 
as 
select 
    bestandid,
    part_value,
    STRING_AGG(matched_address_word, ' ' order by ordinality) as matched_address -- Aggregate in correct order
from  
    temp_address_matches_deduplicated
group by  
    bestandid,
    part_value
 ;

select * from temp_aggregated_address_matches;


-----------------------
-- Ort Match
-----------------------
-- Deduplicate matched words for ort while keep their order
drop table if exists temp_ort_matches_deduplicated;
create temp table 
	temp_ort_matches_deduplicated 
as 
select distinct on (bestandid, matched_ort_word, part_value) -- Deduplicate by bestandid and word
    bestandid,
    matched_ort_word,
    part_value,
    ordinality -- keep the original order
from  
    temp_ort_matches
order by  
    bestandid, 
    matched_ort_word,
    part_value,
    ordinality
; 

select * from temp_ort_matches_deduplicated;


-- Aggregate deduplicated ort matches
drop table if exists temp_aggregated_ort_matches;
create temp table 
	temp_aggregated_ort_matches 
as 
select 
    bestandid,
    part_value,
    STRING_AGG(matched_ort_word, ' ' order by ordinality) as matched_ort -- Aggregate in correct order
from  
    temp_ort_matches_deduplicated
group by  
    bestandid,
    part_value
;

select * from temp_aggregated_ort_matches;



-----------------------------------------------
-- collect matches into the final results table
-----------------------------------------------
drop table if exists temp_final_results_nam;
create temp table
	temp_final_results_nam
as 
select distinct
    n.bestandid,
    n.uid_no,
    n.telefon,
    n.url, 
    n.rank, 
    (select distinct n.part_value from temp_normalized_url where n.part_number = 5) as url_name,
   --anm.part_value as url_name,
    n.original_name,
    COALESCE(anm.matched_name, null) as matched_name
from  
    temp_normalized_url n
left join  
    temp_aggregated_name_matches anm 
on
	n.bestandid = anm.bestandid
where 
	(select distinct n.part_value from temp_normalized_url where n.part_number = 5) is not null
;

drop table if exists temp_final_results_adrs;
create temp table
	temp_final_results_adrs
as 
select distinct
	n.bestandid,
	n.uid_no,
    n.telefon,
    n.url, 
    n.rank, 
	(select distinct n.part_value from temp_normalized_url where n.part_number = 4) as url_address,
   	--aam.part_value as url_address,
    n.original_address,
    COALESCE(aam.matched_address, null) as matched_address
from  
    temp_normalized_url n
left join  
    temp_aggregated_address_matches aam 
on 
	n.bestandid = aam.bestandid
where 
	(select distinct n.part_value from temp_normalized_url where n.part_number = 4) is not null
;
	


drop table if exists temp_final_results_ort;
create temp table
	temp_final_results_ort
as
select 
	n.bestandid,
    n.url, 
    n.uid_no,
    n.telefon,
    n.rank, 
	(select distinct n.part_value from temp_normalized_url where n.part_number = 3) as url_ort,
    --aom.part_value as url_ort,
    n.original_ort,
    COALESCE(aom.matched_ort, null) as matched_ort
from  
    temp_normalized_url n
left join  
    temp_aggregated_ort_matches aom 
on
	n.bestandid = aom.bestandid
where 
	(select distinct n.part_value from temp_normalized_url where n.part_number = 3) is not null
;


drop table if exists temp_final_results;
create table temp_final_results
as
select distinct
	n.bestandid,
	n.uid_no,
    n.telefon,
    n.url, 
    n.rank,
    n.url_name,
    n.original_name,
    n.matched_name,
    a.url_address,
    a.original_address,
    a.matched_address,
    o.url_ort,
    o.original_ort,
    o.matched_ort
from 
	temp_final_results_nam n
join
	temp_final_results_adrs a
on
 n.bestandid = a.bestandid
 and 
 n.rank = a.rank
join 
	temp_final_results_ort o
on
 a.bestandid = o.bestandid
 and 
 a.rank = o.rank
;

select * from temp_final_results;





drop table if exists az_testdata.az_search_url_search_ch;
create table 
	az_testdata.az_search_url_search_ch
as
select distinct  
    bestandid,
    uid_no,
    telefon,
    url,  
    rank, 
    url_name,
    original_name,
    matched_name,  
    url_address,
    original_address,
    matched_address, 
    url_ort,
    original_ort,
    matched_ort  
from  
    temp_final_results
order by  
    rank
;

select count(distinct bestandid) from az_testdata.az_search_url_search_ch;
select * from az_testdata.az_search_url_search_ch;




/*
select 
	s.*
	,u.name
	,u.uid_no
	,u.chid
	,u.status
from 
	az_testdata.az_search_url_search_ch s
left join 
	geo_zefix.unternehmen u
on
	s.original_name ilike u.name
;

*/



























--//////////////////////////////////////////////////////////////////////////////////////////////
/*
select 
    bestandid,
    COUNT(distinct part_value_word) as matched_words_count
from (
    -- Combine all matches into one query using UNION ALL
    select	
    	p.bestandid, p.word as part_value_word
    from
    	temp_part_value_words p
    join	
    	temp_original_name_words n
    on 
    	p.bestandid = n.bestandid 
    	and
    	p.word ilike n.word
    union all 
    select
    	p.bestandid, p.word AS part_value_word
    from
    	temp_part_value_words p
    join
    	temp_original_address_words a
    on
    	p.bestandid = a.bestandid 
    	and	p.word ilike a.word
    union all 
    select
    	p.bestandid, p.word AS part_value_word
    from
    	temp_part_value_words p
    join
    	temp_original_ort_words o
    on
    	p.bestandid = o.bestandid 
    	and 
    	p.word ilike o.word
) as matches
group by bestandid
;


-- Consolidate matches into the final results table
create temp table
	temp_final_results 
as	
select 
    n.bestandid,
    --n.url,
    --n.rank,
    -- Original Name and Matched Name
    nm.original_name_word as original_name,
    STRING_AGG(nm.matched_name_word, ', ') as matched_name,
    -- Original Address and Matched Address
    am.original_address_word as original_address,
    STRING_AGG(am.matched_address_word, ', ') as matched_address,
    -- Original Ort and Matched Ort
    om.original_ort_word as original_ort,
    STRING_AGG(om.matched_ort_word, ', ') as matched_ort
from  
    temp_normalized_url n
left join  
    temp_name_matches nm 
on 
	n.bestandid = nm.bestandid
left join  
    temp_address_matches am 
on
	n.bestandid = am.bestandid
left join  
    temp_ort_matches om 
on
	n.bestandid = om.bestandid
group by  
    n.bestandid, 
    n.url,
    n.rank,
    nm.original_name_word, 
    am.original_address_word, 
    om.original_ort_word
;

SELECT 
    bestandid,
    url, -- Include the URL in the results
    rank, -- Include the rank in the results
    original_name,
    matched_name,
    original_address,
    matched_address,
    original_ort,
    matched_ort
FROM 
    temp_final_results
ORDER BY 
    rank; -- Order by rank for clarity


    
    
--//////////////////// 
-- Consolidate matches into the final results table with reconstructed matched results
create temp table 
	temp_final_results 
as 
select 
    n.bestandid,
    n.url, 
    n.rank, 
    n.original_name,
    STRING_AGG(nm.matched_name_word, ' ') as matched_name, -- Concatenate matched words for name
    n.original_address,
    STRING_AGG(am.matched_address_word, ' ') as matched_address, -- Concatenate matched words for address
    n.original_ort,
    STRING_AGG(om.matched_ort_word, ' ') as matched_ort -- Concatenate matched words for ort
from  
    temp_normalized_url n
left join  
    temp_name_matches nm 
on
	n.bestandid = nm.bestandid
left join  
    temp_address_matches am 
on
	n.bestandid = am.bestandid
left join  
    temp_ort_matches om 
on
	n.bestandid = om.bestandid
group by  
    n.bestandid, 
    n.url,
    n.rank,
    n.original_name, 
    n.original_address, 
    n.original_ort
;
  
*/    
    
    
    
    
    
    
    
    
    
    
