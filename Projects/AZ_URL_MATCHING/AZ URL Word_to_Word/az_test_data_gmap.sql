--==========================
-- AZD Test Data
-- Date: 22.01.2025
--==========================
--(I)Process Google Map Data
--2106
create table geo_afo_tmp.tmp_map_filt
as
select 
	*,
	row_number() over(partition by bestandid order by rank_absolute) as rn
from 
	az_testdata.az_map_url
where
   --	rank_absolute <= 10
   -- and
    domain not like '%search.ch%'
    and
    domain not like '%moneyhouse.ch%'
    and
    domain not like '%local.ch%'
    and
    domain not like '%facebook.com%'
    and
    domain not like '%zugerzeitung.ch%'
;


select * from geo_afo_tmp.tmp_map_filt order by bestandid, rn;

delete from geo_afo_tmp.tmp_map_filt where rn > 10; -- 1712



-- zefix Urls contains lots of information 
select * from az_testdata.az_map_url where domain like '%www.zefix.ch%';
www.zefix.ch


--============================================
-- Step 1: Create a temp table for split URLs
--1385
--===========================================
drop table if exists temp_split_url_gmap;
create temp table 
	temp_split_url_gmap
as	
select     
    bestandid,
    name1 as original_name,           
    google_map_title as google_map_name,
    google_map_adresse as google_map_address,
    google_map_telefon,
    zuststrasse as original_address,
    zustplz as plz,
    zustort as original_ort,
    uid_no,
    telefon,
    url,
    domain,
    rank_absolute as rank,
    -- Clean the URL by removing "http://", "https://", "www.", and ".ch", ".com", ".de"
    trim(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(url, '^https?://(www\.)?', ''), 	-- Remove protocol and "www."
    '\.ch|\.com|\.de', '' ),						-- Remove ".ch", ".com", ".de" wherever they appear
    '/$', '' 	  ), 								-- Remove trailing "/"
    '/+', '/'     ), 								-- Replace multiple "/" with a single "/"
	'\.en',''     ),
    '\.fr',''     ),
    '\.de',''     ),
    '\.it',''     ),
    '\.html','' 					-- remove .html .fr .it .en 
    )) as cleaned_url,
    -- Split the cleaned URL into parts and return as an array
    array_remove(
        string_to_array(
			    regexp_replace(
			    regexp_replace(
			    regexp_replace(
			    regexp_replace(
			    regexp_replace(
			    regexp_replace(
			    regexp_replace(
			    regexp_replace(
			    regexp_replace(url, '^https?://(www\.)?', ''), 	-- Remove protocol and "www."
			    '\.ch|\.com|\.de', '' ),						-- Remove ".ch", ".com", ".de" wherever they appear
			    '/$', '' 	  ), 								-- Remove trailing "/"
			    '/+', '/'     ), 								-- Replace multiple "/" with a single "/"
			    '\.en','' 	  ),
			    '\.fr','' 	  ),
			    '\.de','' 	  ),
			    '\.it','' 	  ),
			    '\.html',''	  ),								-- remove .html .fr .it .en 
			    '/' 		  ), 											-- Split by "/"
        		'' 												-- Remove empty elements from the array
    ) as url_parts
from 
	geo_afo_tmp.tmp_map_filt
--where
  -- 	rank_absolute <= 10
  --  and
  --  domain not like '%search.ch%'
  --  and
  --  domain not like '%moneyhouse.ch%'
  --  and
  --  domain not like '%local.ch%'
  --  and
  --  domain not like '%facebook.com%'
  --  and
  --  domain not like '%zugerzeitung.ch%'
;


select * from temp_split_url_gmap; -- old 2106 --new 2332



--==================================================
-- Step 2: Create a temp table for normalized data
-- 1380  -- '\.', ' ', 'g'),
--==================================================
drop table if exists temp_normalized_url_gmap;
create temp table
	temp_normalized_url_gmap
as 
select  
    bestandid,
    original_name,
    google_map_name,
    original_address,
    plz,
    original_ort,
    uid_no,
    telefon,
    url,
    domain,
    rank,
    cleaned_url,
    url_parts,
    part_number,
    array_agg(	trim(
    			regexp_replace	(
		    	regexp_replace	(
		    	regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		    	regexp_replace	(
		    	regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace(lower(part_value),'\.', ' ', 'g'), -- Replace dots with spaces
		        '\''','','g'	),
		        '-', ' ', 'g' 	),  	-- Replace hyphens with spaces
		        '\+', ' ', 'g'	),  	-- Replace plus with spaces                      
		        '%', ' ', 'g' 	),  	-- Replace percent with spaces                      
		        '&', ' ', 'g' 	), 		-- Replace ampersand with spaces                    
		        '=', ' ', 'g' 	), 		-- Replace equal with spaces                    
		        '\?', ' ', 'g'	), 		-- Replace question mark with spaces
		        ' ag', '', 'g'	), 		-- Remove company entities
		        ' gmbh', '', 'g'),
		        ' sarl', '', 'g'),
		        ' sàrl', '', 'g'),
		        ' snc', '', 'g'	),
		        ' sagl', '', 'g'),
		        ' sas', '', 'g'	),
		        ' sa', '', 'g'	),
		        ' s a g l', '', 'g'),
		        ' spa', '', 'g'	),
		        'ä', 'ae', 'g'	),                  
		        'ö', 'oe', 'g'	),                 
		        'ü', 'ue', 'g'	),                
		        'ß', 'ss', 'g'	),               
		        'é', 'e', 'g' 	),              
		        'è', 'e', 'g' 	),             
		        'ê', 'e', 'g' 	),            
		        'ç', 'c', 'g' 	),
		        'á', 'a', 'g' 	),
		        'à', 'a', 'g' 	)        
    )) as normalized_part_value,
    -- Normalize German special characters and additional signs in original_name
    array_agg(	trim(
    			regexp_replace	(
		    	regexp_replace	(
		    	regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		    	regexp_replace	(
		    	regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace	(
		        regexp_replace(lower(original_name),'\.', ' ', 'g'), -- Replace dots with spaces
		        '\''','','g'	),
		        '-', ' ', 'g' 	),  	-- Replace hyphens with spaces
		        '\+', ' ', 'g'	),  	-- Replace plus with spaces                      
		        '%', ' ', 'g' 	),  	-- Replace percent with spaces                      
		        '&', ' ', 'g' 	), 		-- Replace ampersand with spaces                    
		        '=', ' ', 'g' 	), 		-- Replace equal with spaces                    
		        '\?', ' ', 'g'	), 		-- Replace question mark with spaces
		        ' ag', '', 'g'	), 		-- Remove company entities
		        ' gmbh', '', 'g'),
		        ' sarl', '', 'g'),
		        ' sàrl', '', 'g'),
		        ' snc', '', 'g'	),
		        ' sagl', '', 'g'),
		        ' sas', '', 'g'	),
		        ' sa', '', 'g'	),
		        ' s a g l', '', 'g'),
		        ' spa', '', 'g'	),
		        'ä', 'ae', 'g'	),                  
		        'ö', 'oe', 'g'	),                 
		        'ü', 'ue', 'g'	),                
		        'ß', 'ss', 'g'	),               
		        'é', 'e', 'g' 	),              
		        'è', 'e', 'g' 	),             
		        'ê', 'e', 'g' 	),            
		        'ç', 'c', 'g' 	),
		        'á', 'a', 'g' 	),
		        'à', 'a', 'g' 	) 
    )) as normalized_original_name,
    -- Normalize German special characters and additional signs in original_address
    array_agg(	trim(
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
		        regexp_replace(
		        regexp_replace(
		        regexp_replace(
		        regexp_replace(
		        regexp_replace(
		        regexp_replace(
		        regexp_replace(
		        regexp_replace(lower(original_address),'\.', ' ', 'g'), -- Replace dots with spaces
		        '\''','','g'  ),
		        '-', ' ', 'g' ),  	-- Replace hyphens with spaces
		        '\+', ' ', 'g'),  	-- Replace plus with spaces                      
		        '%', ' ', 'g' ),  	-- Replace percent with spaces                      
		        '&', ' ', 'g' ), 	-- Replace ampersand with spaces                    
		        '=', ' ', 'g' ), 	-- Replace equal with spaces                    
		        '\?', ' ', 'g'), 	-- Replace question mark with spaces                   
		        'ä', 'ae', 'g'),                  
		        'ö', 'oe', 'g'),                 
		        'ü', 'ue', 'g'),                
		        'ß', 'ss', 'g'),               
		        'é', 'e', 'g' ),              
		        'è', 'e', 'g' ),             
		        'ê', 'e', 'g' ),            
		        'ç', 'c', 'g' ),
		        'á', 'a', 'g' ),
		        'à', 'a', 'g' )         
     )) as normalized_original_address,
    -- Normalize German special characters and additional signs in original_ort
    array_agg(	trim(
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
		        regexp_replace(
		        regexp_replace(
		        regexp_replace(
		        regexp_replace(
		        regexp_replace(
		        regexp_replace(
		        regexp_replace(
		        regexp_replace(lower(original_ort),'\.', ' ', 'g'), -- Replace dots with spaces
		        '\''','','g'  ),
		        '-', ' ', 'g' ),  	-- Replace hyphens with spaces
		        '\+', ' ', 'g'),  	-- Replace plus with spaces                      
		        '%', ' ', 'g' ),  	-- Replace percent with spaces                      
		        '&', ' ', 'g' ), 	-- Replace ampersand with spaces                    
		        '=', ' ', 'g' ), 	-- Replace equal with spaces                    
		        '\?', ' ', 'g'), 	-- Replace question mark with spaces                   
		        'ä', 'ae', 'g'),                  
		        'ö', 'oe', 'g'),                 
		        'ü', 'ue', 'g'),                
		        'ß', 'ss', 'g'),               
		        'é', 'e', 'g' ),              
		        'è', 'e', 'g' ),             
		        'ê', 'e', 'g' ),            
		        'ç', 'c', 'g' ),
		        'á', 'a', 'g' ),
		        'à', 'a', 'g' ) 
    )) as normalized_original_ort
from   
    temp_split_url_gmap,
    unnest(url_parts) with ordinality as t(part_value, part_number)
group by 
    bestandid, original_name, google_map_name, original_address, plz, 
    original_ort, uid_no, telefon, url, domain, rank, cleaned_url,url_parts,
    part_number
;



select * from temp_normalized_url_gmap;


--=========================
-- Word-to-Word comparison
--=========================
-----------------------------------------------
--Split Words from Each Field into Temp Tables
-----------------------------------------------
-- Step 2.1: Split words from part_value into a temp table
drop table if exists temp_part_value_words_gmap;
create temp table 
	temp_part_value_words_gmap 
as	
select distinct 
    bestandid,
    original_name,
    original_address,
    original_ort,
    url,
    part_number,
	url_parts,
    word,
    ordinality
from
    temp_normalized_url_gmap,
    lateral unnest(normalized_part_value) as u(normalized_value),
    lateral regexp_split_to_table(trim(normalized_value), '\s+') with ordinality as t(word, ordinality)
order by  
    bestandid,
    url,
    part_number
;


select * from temp_part_value_words_gmap;
--
-- Step 2.2: Split words from original_name into a temp table
drop table if exists temp_original_name_words_gmap;
create temp table 
	temp_original_name_words_gmap 
as 
select distinct
    bestandid,
    url,
    part_number,
	url_parts,
    word,
    ordinality
from
    temp_normalized_url_gmap,
    lateral unnest(normalized_original_name) as u(normalized_name),
    lateral regexp_split_to_table(trim(normalized_name), '\s+') with ordinality as t(word, ordinality)
order by  
    bestandid,
    url,
    part_number
;


select * from temp_original_name_words_gmap;

-- Step 2.3: Split words from original_address into a temp table
--normalized_original_address
drop table if exists temp_original_address_words_gmap;
create temp table
	temp_original_address_words_gmap 
as 
select distinct
    bestandid,
    url,
    part_number,
	url_parts,
    word,
    ordinality
from
    temp_normalized_url_gmap,
    lateral unnest(normalized_original_address) as u(normalized_address),
    lateral regexp_split_to_table(trim(normalized_address), '\s+') with ordinality as t(word, ordinality)
order by  
    bestandid,
    url,
    part_number
;


select * from temp_original_address_words_gmap;

-- Step 2.4: Split words from original_ort into a temp table
--
drop table if exists temp_original_ort_words_gmap;
create temp table 
	temp_original_ort_words_gmap 
as 
select distinct 
    bestandid,
    url,
    part_number,
	url_parts,
    word,
    ordinality
from
    temp_normalized_url_gmap,
    lateral unnest(normalized_original_ort) as u(normalized_ort),
    lateral regexp_split_to_table(trim(normalized_ort), '\s+') with ordinality as t(word, ordinality)
order by  
    bestandid,
    url,
    part_number
;

select * from temp_original_ort_words_gmap;



----------------------------------
--Compare Words and Find Matches
----------------------------------
-- Step 3.1: Compare part_value words with original_name words
drop table if exists temp_name_matches_gmap;
create temp table
	temp_name_matches_gmap
as 
select  distinct 
    p.bestandid,
    p.url,
    p.part_number,
    p.url_parts,
    p.original_name,
    p.ordinality as ordinality,
    p.word as matched_name_word,
    n.word as original_name_word
    --array_agg(distinct p.word) as matched_name_word,
    --array_agg(distinct n.word) as original_name_word
from  
    temp_part_value_words_gmap p
join  
    temp_original_name_words_gmap n
on  
   	p.bestandid = n.bestandid 
   	and 
   	(
        p.word ilike n.word 
        or
        similarity(p.word, n.word) > 0.8
        or
        levenshtein(p.word, n.word) <= 1
    )
   	and 
  	p.url = n.url
;

select * from temp_name_matches_gmap;

-- Step 3.2: Compare part_value words with original_address words
drop table if exists temp_address_matches_gmap;
create temp table
	temp_address_matches_gmap
as
select  
    p.bestandid,
    p.url,
    p.url_parts,
    p.original_address,
    p.ordinality,
    p.word as matched_address_word,
    a.word as original_address_word
from  
    temp_part_value_words_gmap p
join  
    temp_original_address_words_gmap a
on  
    p.bestandid = a.bestandid 
   	and 
   	(
        p.word ilike a.word 
        or
        similarity(p.word, a.word) > 0.8
        or
        levenshtein(p.word, a.word) <= 1
    )
   	and 
  	p.url = a.url
;


select * from temp_address_matches_gmap;

-- Step 3.3: Compare part_value words with original_ort words
drop table if exists temp_ort_matches_gmap;
create temp table
	temp_ort_matches_gmap
as
select  
    p.bestandid,
    p.url,
    --p.url_parts,
    p.original_ort,
    p.ordinality,
    p.word as matched_ort_word,
    o.word as original_ort_word
from  
    temp_part_value_words_gmap p
join  
    temp_original_ort_words_gmap o
on  
    p.bestandid = o.bestandid 
   	and 
   	(
        p.word ilike o.word 
        or
        similarity(p.word, o.word) > 0.8
        or
        levenshtein(p.word, o.word) <= 1
    )
   	and 
  	p.url = o.url
;


select * from temp_ort_matches_gmap;



---------------------------------------------------------------------------
-- aggregate matches and handle duplicates and ordinality
---------------------------------------------------------------------------

-----------------------
-- Name Match
-----------------------
-- Deduplicate matched words for name while keep their order
drop table if exists temp_name_matches_deduplicated_gmap;
create temp table
	temp_name_matches_deduplicated_gmap 
as 
select distinct
    bestandid,
    url,
    original_name,
    matched_name_word,
    ordinality -- keep the original order
from  
    temp_name_matches_gmap
order by  
    bestandid,
    url,
    matched_name_word,
    ordinality
 ; 

select * from temp_name_matches_deduplicated_gmap;
   

-- list to string 
drop table if exists temp_aggregated_name_matches_gmap;   
create temp table
	temp_aggregated_name_matches_gmap
as 
select 
    bestandid,
    url,
    original_name,
    -- Convert the array to a string with ordering
    string_agg(matched_name_word, ' ' order by ordinality) as matched_name
from  
    temp_name_matches_deduplicated_gmap
group by 
    bestandid, url, original_name
;

select * from temp_aggregated_name_matches_gmap;


-----------------------
-- Address Match
-----------------------
-- Deduplicate matched words for address while keep their order
--
drop table if exists temp_address_matches_deduplicated_gmap;
create temp table 
	temp_address_matches_deduplicated_gmap 
as 
select distinct
    bestandid,
    url,
    original_address,
    matched_address_word,
    ordinality -- keep the original order
from  
    temp_address_matches_gmap
order by  
    bestandid,
    url,
    matched_address_word,
    ordinality desc
 ; 

select * from temp_address_matches_deduplicated_gmap;

-- list to string
drop table if exists temp_aggregated_address_matches_gmap;
create temp table 
	temp_aggregated_address_matches_gmap 
as 
select 
    bestandid,
    url,
    original_address,
    -- Convert the array to a string with ordering
    string_agg(matched_address_word, ' ' order by ordinality) as matched_address
from  
    temp_address_matches_deduplicated_gmap
group by 
    bestandid, url, original_address
;


select * from temp_aggregated_address_matches_gmap;

-----------------------
-- Ort Match
-----------------------
-- Deduplicate matched words for ort while keep their order
--
drop table if exists temp_ort_matches_deduplicated_gmap;
create temp table 
	temp_ort_matches_deduplicated_gmap
as 
select distinct 
    bestandid,
    url,
    original_ort,
    matched_ort_word,
    ordinality -- keep the original order
from  
    temp_ort_matches_gmap
order by  
    bestandid,
    url,
    matched_ort_word,
    ordinality
 ; 

select * from temp_ort_matches_deduplicated_gmap;


-- list to string
drop table if exists temp_aggregated_ort_matches_gmap;
create temp table 
	temp_aggregated_ort_matches_gmap 
as 
select 
    bestandid,
    url,
    original_ort,
    -- Convert the array to a string with ordering
    string_agg(matched_ort_word, ' ' order by ordinality) as matched_ort
from  
    temp_ort_matches_deduplicated_gmap
group by 
    bestandid, url, original_ort
;

select * from temp_aggregated_ort_matches_gmap;



-----------------------------------------------
-- collect matches into the final results table
-----------------------------------------------
drop table if exists tmp_final_results_gmap;
create temp table 
	tmp_final_results_gmap
as
select distinct
    n.bestandid,
    n.uid_no,
    n.url, 
    n.domain,
    n.rank, 
    n.telefon,
   	--anm.part_value as url_name,
    n.original_name,
    n.google_map_name,
    COALESCE(anm.matched_name, null) as matched_name,
    --aam.part_value as url_address,
    n.original_address,
    COALESCE(aam.matched_address, null) as matched_address,
    --aom.part_value as url_ort,
    n.original_ort,
    COALESCE(aom.matched_ort, null) as matched_ort
from  
    temp_normalized_url_gmap n
left join  
    temp_aggregated_name_matches_gmap anm 
on
	n.bestandid = anm.bestandid
	and 
	n.url = anm.url
left join  
    temp_aggregated_address_matches_gmap aam 
on 
	n.bestandid = aam.bestandid
	and
	n.url = aam.url
left join  
    temp_aggregated_ort_matches_gmap aom 
on
	n.bestandid = aom.bestandid
	and 
	n.url = aom.url
;

select * from tmp_final_results_gmap;



drop table if exists az_testdata.az_map_url_gmap_v;
create table 
	az_testdata.az_map_url_gmap_v
as
select distinct  
	*
from
	tmp_final_results_gmap
;




select count(distinct bestandid) from az_testdata.az_map_url_gmap_v; --786
select count(distinct bestandid)  from az_testdata.az_map_url_gmap_v where matched_name is not null; -- 1547 --1303 

select * from az_testdata.az_map_url_gmap_v; --old 2104 new 2330



/*
drop table if exists az_testdata.az_map_url_gmap_v2;
create table 
	az_testdata.az_map_url_gmap_v2
as
select distinct  
    bestandid,
    uid_no,
    url,
    domain,
    rank, 
    original_name,
    null as moneyhouse_name,
    null as search_ch_name,
    matched_name,  
    original_address,
    null as search_ch_address,
    matched_address, 
    original_ort,
    null as search_ch_ort,
    matched_ort,
    telefon
from  
    tmp_final_results_gmap
order by  
    rank
;
*/






-- //////////////////////////////
-- query from all_quelle script 
--//////////////////////////////
select * from geo_afo_tmp.az_urls_all;





































































--///////////////////////// OLD VERSION ////////////////////////////////////////////
--============================================
-- Step 1: Create a temp table for split URLs
--1385
--===========================================
drop table if exists temp_split_url_gmap;
create temp table 
	temp_split_url_gmap
as	
select     
    bestandid,
    cid,
    name1 as original_name,
    google_map_title as google_name,
    zuststrasse as original_address,
    google_map_adresse as google_address,
    zustplz as plz,
    zustort as original_ort,
    uid_no,
    telefon,
    google_map_telefon as google_telefon,
    url,
    domain,
    rank_absolute as rank,
    -- Clean the URL by removing "http://", "https://", "www.", and ".ch", ".com", ".de"
    regexp_replace(
        regexp_replace(
            regexp_replace(
                regexp_replace(url, '^https?://(www\.)?', ''), -- Remove protocol and "www."
                '\.ch|\.com|\.de', '' -- Remove ".ch", ".com", ".de" wherever they appear
            ),
            '/$', '' -- Remove trailing "/"
        ),
        '/+', '/' -- Replace multiple "/" with a single "/"
    ) as cleaned_url,
    -- Split the cleaned URL into parts and return as an array
    array_remove(
        string_to_array(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(url, '^https?://(www\.)?', ''), -- Remove protocol and "www."
                        '\.ch|\.com|\.de', '' -- Remove ".ch", ".com", ".de" wherever they appear
                    ),
                    '/$', '' -- Remove trailing "/"
                ),
                '/+', '/' -- Replace multiple "/" with a single "/"
            ),
            '/' -- Split by "/"
        ),
        '' -- Remove empty elements from the array
    ) as url_parts
from   
    az_testdata.az_map_url
where 
    rank_absolute <= 10
    and
    domain not like '%search.ch%'
    and
    domain not like '%moneyhouse.ch%'
    and
    domain not like '%local.ch%'
    and
    domain not like '%facebook.com%'
    and
    domain not like '%zugerzeitung.ch%'
;

select * from temp_split_url_gmap;

--==================================================
-- Step 2: Create a temp table for normalized data
-- 6515
--==================================================
drop table if exists temp_normalized_url_gmap;
create temp table
	temp_normalized_url_gmap
as 
select  
    bestandid,
    original_name,
    google_name,
    original_address,
    plz,
    original_ort,
    uid_no,
    telefon,
    url,
    domain,
    rank,
    cleaned_url,
    url_parts,
    array_agg(regexp_replace(
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
                        regexp_replace(
                          regexp_replace(
                            regexp_replace(
                              regexp_replace(
                                regexp_replace(
                                  lower(part_value), 
                                  '\.', ' ', 'g' -- Replace dots with spaces
                                ), 
                                '-', ' ', 'g' -- Replace hyphens with spaces
                              ), 
                              '\+', ' ', 'g' -- Replace plus with spaces
                            ), 
                            '%', ' ', 'g' -- Replace percent with spaces
                          ), 
                          '&', ' ', 'g' -- Replace ampersand with spaces
                        ), 
                        '=', ' ', 'g' -- Replace equal with spaces
                      ), 
                      '\?', ' ', 'g' -- Replace question mark with spaces
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
    )) as normalized_part_value,
    -- Normalize German special characters and additional signs in original_name
    array_agg(regexp_replace(
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
                        regexp_replace(
                          regexp_replace(
                            regexp_replace(
                              regexp_replace(
                                regexp_replace(
                                  lower(original_name), 
                                  '\.', ' ', 'g' -- Replace dots with spaces
                                ), 
                                '-', ' ', 'g' -- Replace hyphens with spaces
                              ), 
                              '\+', ' ', 'g' -- Replace plus with spaces
                            ), 
                            '%', ' ', 'g' -- Replace percent with spaces
                          ), 
                          '&', ' ', 'g' -- Replace ampersand with spaces
                        ), 
                        '=', ' ', 'g' -- Replace equal with spaces
                      ), 
                      '\?', ' ', 'g' -- Replace question mark with spaces
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
    )) as normalized_original_name,
    -- Normalize German special characters and additional signs in original_address
    array_agg(regexp_replace(
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
                        regexp_replace(
                          regexp_replace(
                            regexp_replace(
                              regexp_replace(
                                regexp_replace(
                                  lower(original_address), 
                                  '\.', ' ', 'g' -- Replace dots with spaces
                                ), 
                                '-', ' ', 'g' -- Replace hyphens with spaces
                              ), 
                              '\+', ' ', 'g' -- Replace plus with spaces
                            ), 
                            '%', ' ', 'g' -- Replace percent with spaces
                          ), 
                          '&', ' ', 'g' -- Replace ampersand with spaces
                        ), 
                        '=', ' ', 'g' -- Replace equal with spaces
                      ), 
                      '\?', ' ', 'g' -- Replace question mark with spaces
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
    )) as normalized_original_address,
    -- Normalize German special characters and additional signs in original_ort
    array_agg(regexp_replace(
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
                        regexp_replace(
                          regexp_replace(
                            regexp_replace(
                              regexp_replace(
                                regexp_replace(
                                  lower(original_ort), 
                                  '\.', ' ', 'g' -- Replace dots with spaces
                                ), 
                                '-', ' ', 'g' -- Replace hyphens with spaces
                              ), 
                              '\+', ' ', 'g' -- Replace plus with spaces
                            ), 
                            '%', ' ', 'g' -- Replace percent with spaces
                          ), 
                          '&', ' ', 'g' -- Replace ampersand with spaces
                        ), 
                        '=', ' ', 'g' -- Replace equal with spaces
                      ), 
                      '\?', ' ', 'g' -- Replace question mark with spaces
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
    )) as normalized_original_ort
from
    temp_split_url_gmap,
    unnest(url_parts) with ordinality as t(part_value, part_number)
group by 
    bestandid, cid, original_name, google_name, original_address, google_address, plz, 
    original_ort, uid_no, telefon, google_telefon, url, domain, rank, cleaned_url,url_parts
;


select * from temp_normalized_url_gmap;


--=========================
-- Word-to-Word comparison
--=========================
-----------------------------------------------
--Split Words from Each Field into Temp Tables
-----------------------------------------------
-- Step 2.1: Split words from part_value into a temp table
drop table if exists temp_part_value_words_gmap;
create temp table 
	temp_part_value_words_gmap 
as	
select 
    bestandid,
    cid,
    original_name,
    original_address,
    original_ort,
    url,
	url_parts,
    word,
    ordinality
from
    temp_normalized_url_gmap,
    lateral unnest(normalized_part_value) as u(normalized_value),
    lateral regexp_split_to_table(normalized_value, '\s+') with ordinality as t(word, ordinality)
;


select * from temp_part_value_words_gmap;
--
-- Step 2.2: Split words from original_name into a temp table
drop table if exists temp_original_name_words_gmap;
create temp table 
	temp_original_name_words_gmap 
as 
select 
    bestandid,
    cid,
    url,
	url_parts,
    word,
    ordinality
from
    temp_normalized_url_gmap,
    lateral unnest(normalized_original_name) as u(normalized_name),
    lateral regexp_split_to_table(normalized_name, '\s+') with ordinality as t(word, ordinality)
;

select * from temp_original_name_words_gmap;

-- Step 2.3: Split words from original_address into a temp table
--normalized_original_address
drop table if exists temp_original_address_words_gmap;
create temp table
	temp_original_address_words_gmap 
as 
select 
    bestandid,
    cid,
    url,
	url_parts,
    word,
    ordinality
from
    temp_normalized_url_gmap,
    lateral unnest(normalized_original_address) as u(normalized_address),
    lateral regexp_split_to_table(normalized_address, '\s+') with ordinality as t(word, ordinality)
;

select * from temp_original_address_words_gmap;

-- Step 2.4: Split words from original_ort into a temp table
--
drop table if exists temp_original_ort_words_gmap;
create temp table 
	temp_original_ort_words_gmap 
as 
select 
    bestandid,
    cid,
    url,
	url_parts,
    word,
    ordinality
from
    temp_normalized_url_gmap,
    lateral unnest(normalized_original_ort) as u(normalized_ort),
    lateral regexp_split_to_table(normalized_ort, '\s+') with ordinality as t(word, ordinality)
;

select * from temp_original_ort_words_gmap;



----------------------------------
--Compare Words and Find Matches
----------------------------------
-- Step 3.1: Compare part_value words with original_name words
drop table if exists temp_name_matches_gmap;
create temp table
	temp_name_matches_gmap
as 
select distinct 
    p.bestandid,
    p.cid,
    p.url,
    p.url_parts,
    p.original_name,
    array_agg(distinct p.ordinality) as ordinality,
    array_agg(distinct p.word) as matched_name_word,
    array_agg(distinct n.word) as original_name_word
from  
    temp_part_value_words_gmap p
join  
    temp_original_name_words_gmap n
on  
   	p.bestandid = n.bestandid 
   	and 
   	p.word ilike n.word
   	and 
  	p.url = n.url
group by 
	p.bestandid,
    p.cid,
    p.url,
    p.url_parts,
    p.original_name
;

select * from temp_name_matches_gmap;

-- Step 3.2: Compare part_value words with original_address words
drop table if exists temp_address_matches_gmap;
create temp table
	temp_address_matches_gmap
as
select distinct 
    p.bestandid,
    p.cid,
    p.url,
    p.url_parts,
    p.original_address,
    array_agg(distinct p.ordinality) as ordinality,
    array_agg(distinct p.word) as matched_address_word,
    array_agg(distinct a.word) as original_address_word
from  
    temp_part_value_words_gmap p
join  
    temp_original_address_words_gmap a
on  
    p.bestandid = a.bestandid 
   	and 
   	p.word ilike a.word
   	and 
  	p.url = a.url
group by 
	p.bestandid,
    p.cid,
    p.url,
    p.url_parts,
    p.original_address
;


select * from temp_address_matches_gmap;

-- Step 3.3: Compare part_value words with original_ort words
drop table if exists temp_ort_matches_gmap;
create temp table
	temp_ort_matches_gmap
as
select distinct 
    p.bestandid,
    p.cid,
    p.url,
    p.url_parts,
    p.original_ort,
    array_agg(distinct p.ordinality) as ordinality,
    array_agg(distinct p.word) as matched_ort_word,
    array_agg(distinct o.word) as original_ort_word
from  
    temp_part_value_words_gmap p
join  
    temp_original_ort_words_gmap o
on  
    p.bestandid = o.bestandid 
   	and 
   	p.word ilike o.word
   	and 
  	p.url = o.url
group by 
	p.bestandid,
    p.cid,
    p.url,
    p.url_parts,
    p.original_ort
;


select * from temp_ort_matches_gmap;



---------------------------------------------------------------------------
-- aggregate name matches with DISTINCT to remove duplicates and ordinality
---------------------------------------------------------------------------

-----------------------
-- Name Match
-----------------------
-- Deduplicate matched words for name while keep their order
drop table if exists temp_name_matches_deduplicated_gmap;
create temp table
	temp_name_matches_deduplicated_gmap 
as 
select 
    bestandid,
    cid,
    url,
    original_name,
    matched_name_word,
    ordinality -- keep the original order
from  
    temp_name_matches_gmap
order by  
    bestandid,
    cid,
    url,
    matched_name_word,
    ordinality
 ; 

select * from temp_name_matches_deduplicated_gmap;
   
-- list to string   
drop table if exists temp_aggregated_name_matches_gmap;   
create temp table
	temp_aggregated_name_matches_gmap
as 
select 
    bestandid,
    cid,
    url,
    original_name,
    -- Convert the array to a string with ordering
    string_agg(t.word, ' ' order by t.ordinality) as matched_name
from  
    temp_name_matches_deduplicated_gmap,
    lateral unnest(matched_name_word) with ordinality as t(word, ordinality)
group by 
    bestandid, cid, url, original_name;


select * from temp_aggregated_name_matches_gmap;


-----------------------
-- Address Match
-----------------------
-- Deduplicate matched words for address while keep their order
--
drop table if exists temp_address_matches_deduplicated_gmap;
create temp table 
	temp_address_matches_deduplicated_gmap 
as 
select 
    bestandid,
    cid,
    url,
    original_address,
    matched_address_word,
    ordinality -- keep the original order
from  
    temp_address_matches_gmap
order by  
    bestandid,
    cid,
    url,
    matched_address_word,
    ordinality
 ; 

select * from temp_address_matches_deduplicated_gmap;

-- list to string
drop table if exists temp_aggregated_address_matches_gmap;
create temp table 
	temp_aggregated_address_matches_gmap 
as 
select 
    bestandid,
    cid,
    url,
    original_address,
    -- Convert the array to a string with ordering
    string_agg(t.word, ' ' order by t.ordinality) as matched_address
from  
    temp_address_matches_deduplicated_gmap,
    lateral unnest(matched_address_word) with ordinality as t(word, ordinality)
group by 
    bestandid, cid, url, original_address
;

select * from temp_aggregated_address_matches_gorg;


-----------------------
-- Ort Match
-----------------------
-- Deduplicate matched words for ort while keep their order
--
drop table if exists temp_ort_matches_deduplicated_gmap;
create temp table 
	temp_ort_matches_deduplicated_gmap
as 
select --distinct on (bestandid, cid, url) -- Deduplicate by bestandid and word
    bestandid,
    cid,
    url,
    original_ort,
    matched_ort_word,
    ordinality -- keep the original order
from  
    temp_ort_matches_gmap
order by  
    bestandid,
    cid,
    url,
    matched_ort_word,
    ordinality
 ; 

select * from temp_ort_matches_deduplicated_gmap;


-- list to string
drop table if exists temp_aggregated_address_matches_gmap;
create temp table 
	temp_aggregated_address_matches_gmap 
as 
select 
    bestandid,
    cid,
    url,
    original_ort,
    -- Convert the array to a string with ordering
    string_agg(t.word, ' ' order by t.ordinality) as matched_ort
from  
    temp_ort_matches_deduplicated_gmap,
    lateral unnest(matched_ort_word) with ordinality as t(word, ordinality)
group by 
    bestandid, cid, url, original_ort
;

select * from temp_aggregated_ort_matches_gmap;



-----------------------------------------------
-- collect matches into the final results table
-----------------------------------------------
drop table if exists temp_final_results_gmap;
create table 
	temp_final_results_gmap
as
select --distinct
    n.bestandid,
    n.url, 
    n.rank, 
   	--anm.part_value as url_name,
    n.original_name,
    COALESCE(anm.matched_name, null) as matched_name,
    --aam.part_value as url_address,
    n.original_address,
    COALESCE(aam.matched_address, null) as matched_address,
    --aom.part_value as url_ort,
    n.original_ort,
    COALESCE(aom.matched_ort, null) as matched_ort
from  
    temp_normalized_url_gmap n
left join  
    temp_aggregated_name_matches_gmap anm 
on
	n.bestandid = anm.bestandid
	and 
	n.url = anm.url
left join  
    temp_aggregated_address_matches_gmap aam 
on 
	n.bestandid = aam.bestandid
	and
	n.url = aam.url
left join  
    temp_aggregated_ort_matches_gmap aom 
on
	n.bestandid = aom.bestandid
	and 
	n.url = aom.url
;


drop table if exists az_testdata.az_map_url_gmap;
create table 
	az_testdata.az_map_url_gmap
as
select --distinct  
	* 
from  
    temp_final_results_gmap
order by  
    rank
;


select count(distinct bestandid) from az_testdata.az_map_url_gmap;
select * from az_testdata.az_map_url_gmap;







