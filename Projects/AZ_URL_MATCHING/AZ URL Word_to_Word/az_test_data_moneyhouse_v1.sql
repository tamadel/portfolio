--==========================
-- AZD Test Data
-- Date: 22.01.2025
-- Update: 07.02.2025
--==========================
--(I)Process Google search Organic "moneyhouse.ch"
--2106
drop table if exists tmp_az_monh_filt;
create temp table tmp_az_monh_filt
as
select 
	*,
	row_number() over(partition by bestandid order by rank_absolute) as rn
from 
	az_testdata.az_search_url
where 
 	type = 'organic' 
 	and
 	domain in ('moneyhouse.ch', 'www.moneyhouse.ch')
;

drop table if exists  az_testdata.az_moneyhouse_urls;
create table 
	az_testdata.az_moneyhouse_urls
as
select 
	*
from 
	tmp_az_monh_filt
where  
	rn = 1
;

select * from az_testdata.az_moneyhouse_urls;

-- zefix Urls contains lots of information 
select * from az_testdata.az_search_url where type = 'organic' and domain like '%zefix%';


--============================================
-- Step 1: Create a temp table for split URLs
--1385
--===========================================
drop table if exists temp_split_url_monyh_ch;
create temp table 
	temp_split_url_monyh_ch
as	
select     
    bestandid,
    name1 as original_name,           
    title as google_organic_name,
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
    regexp_replace(
    regexp_replace(url, '^https?://(www\.)?', ''), 	-- Remove protocol and "www."
    '\.ch|\.com|\.de', '' ),						-- Remove ".ch", ".com", ".de" wherever they appear
    '/$', '' 	  ), 								-- Remove trailing "/"
    '/+', '/'     ), 								-- Replace multiple "/" with a single "/"
	'\.en',''     ),
    '\.fr',''     ),
    '\.de',''     ),
    '\.it',''     ),
    '-[0-9]+$', ''),
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
			    regexp_replace(
			    regexp_replace(url::text, '^https?://(www\.)?', ''), 	-- Remove protocol and "www."
			    '\.ch|\.com|\.de', '' ),								-- Remove ".ch", ".com", ".de" wherever they appear
			    '/$', '' 	  ), 										-- Remove trailing "/"
			    '/+', '/'     ), 										-- Replace multiple "/" with a single "/"
			    '\.en','' 	  ),
			    '\.fr','' 	  ),
			    '\.de','' 	  ),
			    '\.it','' 	  ),
			    '\.html',''	  ),								-- remove .html .fr .it .en
			    '-[0-9]+$', ''), 
			    '/' 		  ), 											-- Split by "/"
        		'' 			  									-- Remove empty elements from the array
    ) as url_parts
from   
    az_testdata.az_search_url
where 
 	type = 'organic' 
 	and
 	domain in ('moneyhouse.ch', 'www.moneyhouse.ch')
;

select * from temp_split_url_monyh_ch;

-- extract the name , address and ort from search.ch urls
drop table if exists temp_url_monyh_ch;
create temp table 
	temp_url_monyh_ch
as	
select 
	bestandid,
    original_name,           
    google_organic_name,
    original_address,
    plz,
    original_ort,
    uid_no,
    telefon,
    url,
    cleaned_url,
    domain,
    rank,
	trim(
	regexp_replace(
	regexp_replace(
	regexp_replace(
	regexp_replace(
	regexp_replace(
	regexp_replace(
	regexp_replace(
	regexp_replace(
	regexp_replace(
	regexp_replace(url, '^.*/', '', 'g'), 	-- Extract the last part after the last slash                               
	'\.', ' ', 'g'), 						-- Replace dots with spaces
	'-', ' ', 'g' ),						-- Replace hyphens with spaces
	'\+', ' ', 'g'),  						-- Replace plus with spaces                      
	'%', ' ', 'g' ),  						-- Replace percent with spaces                        
	'&', ' ', 'g' ),  						-- Replace ampersand with spaces                  
	'=', ' ', 'g' ), 						-- Replace equal with spaces                 
	'\?', ' ', 'g'),						-- Replace question mark with spaces
	'-[0-9]+$', 'g'),             
	'\d+$', ''    )) as moneyhouse_name,		-- Remove trailing digits      
    url_parts
from
	temp_split_url_monyh_ch
;

select * from temp_url_monyh_ch;


--==================================================
-- Step 2: Create a temp table for normalized data
-- 1380  -- '\.', ' ', 'g'),
--==================================================
drop table if exists temp_normalized_url_monyh_ch;
create temp table
	temp_normalized_url_monyh_ch
as 
select  
    bestandid,
    original_name,
	moneyhouse_name,
    google_organic_name,
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
    temp_url_monyh_ch,
    unnest(url_parts) with ordinality as t(part_value, part_number)
group by 
    bestandid, original_name, google_organic_name, original_address, plz, 
    original_ort, uid_no, telefon, url, domain, rank, cleaned_url,url_parts,
    moneyhouse_name, part_number
;



select * from temp_normalized_url_monyh_ch;


--=========================
-- Word-to-Word comparison
--=========================
-----------------------------------------------
--Split Words from Each Field into Temp Tables
-----------------------------------------------
-- Step 2.1: Split words from part_value into a temp table
drop table if exists temp_part_value_words_monyh_ch;
create temp table 
	temp_part_value_words_monyh_ch 
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
    temp_normalized_url_monyh_ch,
    lateral unnest(normalized_part_value) as u(normalized_value),
    lateral regexp_split_to_table(trim(normalized_value) , '\s+') with ordinality as t(word, ordinality)
order by  
    bestandid,
    url,
    part_number
;


select * from temp_part_value_words_monyh_ch;
--
-- Step 2.2: Split words from original_name into a temp table
drop table if exists temp_original_name_words_monyh_ch;
create temp table 
	temp_original_name_words_monyh_ch 
as 
select distinct
    bestandid,
    url,
    part_number,
	url_parts,
    word,
    ordinality
from
    temp_normalized_url_monyh_ch,
    lateral unnest(normalized_original_name) as u(normalized_name),
    lateral regexp_split_to_table(trim(normalized_name), '\s+') with ordinality as t(word, ordinality)
order by  
    bestandid,
    url,
    part_number
;


select * from temp_original_name_words_monyh_ch;

-- Step 2.3: Split words from original_address into a temp table
--normalized_original_address
drop table if exists temp_original_address_words_monyh_ch;
create temp table
	temp_original_address_words_monyh_ch 
as 
select distinct
    bestandid,
    url,
    part_number,
	url_parts,
    word,
    ordinality
from
    temp_normalized_url_monyh_ch,
    lateral unnest(normalized_original_address) as u(normalized_address),
    lateral regexp_split_to_table(trim(normalized_address), '\s+') with ordinality as t(word, ordinality)
order by  
    bestandid,
    url,
    part_number
;


select * from temp_original_address_words_monyh_ch;

-- Step 2.4: Split words from original_ort into a temp table
--
drop table if exists temp_original_ort_words_monyh_ch;
create temp table 
	temp_original_ort_words_monyh_ch 
as 
select distinct 
    bestandid,
    url,
    part_number,
	url_parts,
    word,
    ordinality
from
    temp_normalized_url_monyh_ch,
    lateral unnest(normalized_original_ort) as u(normalized_ort),
    lateral regexp_split_to_table(trim(normalized_ort), '\s+') with ordinality as t(word, ordinality)
order by  
    bestandid,
    url,
    part_number
;

select * from temp_original_ort_words_monyh_ch;



----------------------------------
--Compare Words and Find Matches
----------------------------------
-- Step 3.1: Compare part_value words with original_name words
drop table if exists temp_name_matches_monyh_ch;
create temp table
	temp_name_matches_monyh_ch
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
    temp_part_value_words_monyh_ch p
join  
    temp_original_name_words_monyh_ch n
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
where 
	p.part_number = (
			        select
			        	MAX(part_number)
			        from
			        	temp_part_value_words_monyh_ch 
			        where
			        	bestandid = p.bestandid
			          	and
			          	url = p.url
    )
;

select * from temp_name_matches_monyh_ch;

-- Step 3.2: Compare part_value words with original_address words
drop table if exists temp_address_matches_monyh_ch;
create temp table
	temp_address_matches_monyh_ch
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
    temp_part_value_words_monyh_ch p
join  
    temp_original_address_words_monyh_ch a
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


select * from temp_address_matches_monyh_ch;

-- Step 3.3: Compare part_value words with original_ort words
drop table if exists temp_ort_matches_monyh_ch;
create temp table
	temp_ort_matches_monyh_ch
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
    temp_part_value_words_monyh_ch p
join  
    temp_original_ort_words_monyh_ch o
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


select * from temp_ort_matches_monyh_ch;



---------------------------------------------------------------------------
-- aggregate matches and handle duplicates and ordinality
---------------------------------------------------------------------------

-----------------------
-- Name Match
-----------------------
-- Deduplicate matched words for name while keep their order
drop table if exists temp_name_matches_deduplicated_monyh_ch;
create temp table
	temp_name_matches_deduplicated_monyh_ch 
as 
select distinct
    bestandid,
    url,
    original_name,
    matched_name_word,
    ordinality -- keep the original order
from  
    temp_name_matches_monyh_ch
order by  
    bestandid,
    url,
    matched_name_word,
    ordinality
 ; 

select * from temp_name_matches_deduplicated_monyh_ch;
   

-- list to string 
drop table if exists temp_aggregated_name_matches_monyh_ch;   
create temp table
	temp_aggregated_name_matches_monyh_ch
as 
select 
    bestandid,
    url,
    original_name,
    -- Convert the array to a string with ordering
    string_agg(matched_name_word, ' ' order by ordinality) as matched_name
from  
    temp_name_matches_deduplicated_monyh_ch
group by 
    bestandid, url, original_name
;

select * from temp_aggregated_name_matches_monyh_ch;


-----------------------
-- Address Match
-----------------------
-- Deduplicate matched words for address while keep their order
--
drop table if exists temp_address_matches_deduplicated_monyh_ch;
create temp table 
	temp_address_matches_deduplicated_monyh_ch 
as 
select distinct
    bestandid,
    url,
    original_address,
    matched_address_word,
    ordinality -- keep the original order
from  
    temp_address_matches_monyh_ch
order by  
    bestandid,
    url,
    matched_address_word,
    ordinality desc
 ; 

select * from temp_address_matches_deduplicated_monyh_ch;

-- list to string
drop table if exists temp_aggregated_address_matches_monyh_ch;
create temp table 
	temp_aggregated_address_matches_monyh_ch 
as 
select 
    bestandid,
    url,
    original_address,
    -- Convert the array to a string with ordering
    string_agg(matched_address_word, ' ' order by ordinality) as matched_address
from  
    temp_address_matches_deduplicated_monyh_ch
group by 
    bestandid, url, original_address
;


select * from temp_aggregated_address_matches_monyh_ch;

-----------------------
-- Ort Match
-----------------------
-- Deduplicate matched words for ort while keep their order
--
drop table if exists temp_ort_matches_deduplicated_monyh_ch;
create temp table 
	temp_ort_matches_deduplicated_monyh_ch
as 
select distinct 
    bestandid,
    url,
    original_ort,
    matched_ort_word,
    ordinality -- keep the original order
from  
    temp_ort_matches_monyh_ch
order by  
    bestandid,
    url,
    matched_ort_word,
    ordinality
 ; 

select * from temp_ort_matches_deduplicated_monyh_ch;


-- list to string
drop table if exists temp_aggregated_ort_matches_monyh_ch;
create temp table 
	temp_aggregated_ort_matches_monyh_ch 
as 
select 
    bestandid,
    url,
    original_ort,
    -- Convert the array to a string with ordering
    string_agg(matched_ort_word, ' ' order by ordinality) as matched_ort
from  
    temp_ort_matches_deduplicated_monyh_ch
group by 
    bestandid, url, original_ort
;

select * from temp_aggregated_ort_matches_monyh_ch;


-----------------------------------------------
-- collect matches into the final results table
-----------------------------------------------
drop table if exists tmp_final_results_monyh_ch_v;
create table 
	tmp_final_results_monyh_ch_v
as
select distinct
    n.bestandid,
    n.uid_no,
    n.url, 
    n.rank, 
   	--anm.part_value as url_name,
    n.original_name,
    n.moneyhouse_name,
    COALESCE(anm.matched_name, null) as matched_name,
    --aam.part_value as url_address,
    n.original_address,
    COALESCE(aam.matched_address, null) as matched_address,
    --aom.part_value as url_ort,
    n.original_ort,
    COALESCE(aom.matched_ort, null) as matched_ort
from  
    temp_normalized_url_monyh_ch n
left join  
    temp_aggregated_name_matches_monyh_ch anm 
on
	n.bestandid = anm.bestandid
	and 
	n.url = anm.url
left join  
    temp_aggregated_address_matches_monyh_ch aam 
on 
	n.bestandid = aam.bestandid
	and
	n.url = aam.url
left join  
    temp_aggregated_ort_matches_monyh_ch aom 
on
	n.bestandid = aom.bestandid
	and 
	n.url = aom.url
;

select * from tmp_final_results_monyh_ch_v;




drop table if exists az_testdata.az_search_url_monyh_ch_v;

create table 
	az_testdata.az_search_url_monyh_ch_v
as
select distinct 
	*
from
	tmp_final_results_monyh_ch_v
;


select count(distinct bestandid) from az_testdata.az_search_url_monyh_ch_v; --888
select count(distinct bestandid) from az_testdata.az_search_url_monyh_ch_v where matched_name is not null; --423 --531


select 
	bestandid  
	,count(*)
from
	az_testdata.az_search_url_monyh_ch_v
group by
	bestandid
having 
	count(*) > 1
;

































/*
drop table if exists az_testdata.az_search_url_monyh_ch_v;
create table 
	az_testdata.az_search_url_monyh_ch_v
as
select distinct  
    bestandid,
    uid_no,
    url,  
    rank,
   -- null as map_rank,
    original_name,
    moneyhouse_name,
    null as search_ch_name,
    matched_name,  
    original_address,
    null as search_ch_address,
    matched_address, 
    original_ort,
    null as search_ch_ort,
    matched_ort  
from  
    tmp_final_results_monyh_ch
order by  
    rank
;
*/








/*
drop table if exists az_testdata.az_uid_num_match_v4;
create table az_testdata.az_uid_num_match_v4
as
select 
	t0.bestandid,
	t0.url,
	t0.rank,
	t0.original_name,
	t0.moneyhouse_name,
	t0.matched_name,
	t1.name,
	t0.uid_no as uid_no_old,
	t1.uid_no as uid_no_new,
	t1.chid,
	t1.status 
from 
	az_testdata.az_search_url_monyh_ch_v2 t0
join
	geo_zefix.unternehmen t1
on
	lower(t0.original_name) = lower(t1.name)
;

--where 
	--matched_name is not null
--;



-- lower(t0.moneyhouse_name) like '%'|| lower(t1.name) ||'%'

select * from az_testdata.az_uid_num_match_v4
where matched_name is null;


select * from az_testdata.az_search_url_monyh_ch_v1;

--=================================================================


--az_testdata.az_url_data_all_v2

 
select 
 *
from
	geo_zefix.unternehmen
;



select 
	bestandid,
	uid_no_new, 
	original_name, 
	LOWER(REGEXP_REPLACE(moneyhouse_name, '[^a-zA-Z\s]', '', 'g')) as normalize_name1, 
	LOWER(REGEXP_REPLACE(name, '[^a-zA-Z\s]', '', 'g')) as normalize_name2
from 
	az_testdata.az_url_data_all_uid_v1

*/








--/////////////////////////////////////////////////////////
-- DRAFT
--/////////////////////////////////////////////////////////
-----------------------------------------------
-- Optimized Word-to-Word Comparison
-----------------------------------------------
/*
-- Step 1: Create a temporary table for normalized words to reduce redundant processing
drop table if exists temp_normalized_words_monyh_ch;
create temp table temp_normalized_words_monyh_ch as
select 
    bestandid, 
    url,
    part_number, 
    url_parts, 
    field_type,
    word, 
    ordinality
from (
    select bestandid, url, part_number, url_parts, 'name' as field_type, 
           word, generate_series(1, array_length(regexp_split_to_array(word, '\s+'), 1)) as ordinality
    from temp_normalized_url_monyh_ch,
         lateral unnest(normalized_original_name) as word
    union all
    select bestandid, url, part_number, url_parts, 'address', 
           word, generate_series(1, array_length(regexp_split_to_array(word, '\s+'), 1))
    from temp_normalized_url_monyh_ch,
         lateral unnest(normalized_original_address) as word
    union all
    select bestandid, url, part_number, url_parts, 'ort', 
           word, generate_series(1, array_length(regexp_split_to_array(word, '\s+'), 1))
    from temp_normalized_url_monyh_ch,
         lateral unnest(normalized_original_ort) as word
) t;

-- Step 2: Compare words with improved fuzzy matching
drop table if exists temp_name_matches_monyh_ch;
create temp table temp_name_matches_monyh_ch as 
select distinct p.bestandid, p.url, p.part_number, p.url_parts, p.original_name,
       p.ordinality, p.word as matched_name_word, n.word as original_name_word
from temp_part_value_words_monyh_ch p
join temp_normalized_words_monyh_ch n
on p.bestandid = n.bestandid 
   and n.field_type = 'name'
   and (
       p.word ilike n.word 
       or similarity(p.word, n.word) > 0.7
       or levenshtein(p.word, n.word) <= 1
   )
and p.url = n.url
where p.part_number = 4;

-- Step 3: Aggregate matches while maintaining order
drop table if exists temp_aggregated_name_matches_monyh_ch;
create temp table temp_aggregated_name_matches_monyh_ch as 
select bestandid, url, original_name,
       string_agg(matched_name_word, ' ' order by ordinality) as matched_name
from temp_name_matches_monyh_ch
group by bestandid, url, original_name;

-- Repeat Steps for Address and Ort

-- Address Matching
drop table if exists temp_address_matches_monyh_ch;
create temp table temp_address_matches_monyh_ch as 
select distinct p.bestandid, p.url, p.url_parts, p.original_address, 
       p.ordinality, p.word as matched_address_word, a.word as original_address_word
from temp_part_value_words_monyh_ch p
join temp_normalized_words_monyh_ch a
on p.bestandid = a.bestandid 
   and a.field_type = 'address'
   and (
       p.word ilike a.word 
       or similarity(p.word, a.word) > 0.7
       or levenshtein(p.word, a.word) <= 1
   )
and p.url = a.url;

-- Aggregate Address Matches
drop table if exists temp_aggregated_address_matches_monyh_ch;
create temp table temp_aggregated_address_matches_monyh_ch as 
select bestandid, url, original_address,
       string_agg(matched_address_word, ' ' order by ordinality) as matched_address
from temp_address_matches_monyh_ch
group by bestandid, url, original_address;

-- Ort Matching
drop table if exists temp_ort_matches_monyh_ch;
create temp table temp_ort_matches_monyh_ch as 
select distinct p.bestandid, p.url, p.original_ort, 
       p.ordinality, p.word as matched_ort_word, o.word as original_ort_word
from temp_part_value_words_monyh_ch p
join temp_normalized_words_monyh_ch o
on p.bestandid = o.bestandid 
   and o.field_type = 'ort'
   and (
       p.word ilike o.word 
       or similarity(p.word, o.word) > 0.7
       or levenshtein(p.word, o.word) <= 1
   )
and p.url = o.url;

-- Aggregate Ort Matches
drop table if exists temp_aggregated_ort_matches_monyh_ch;
create temp table temp_aggregated_ort_matches_monyh_ch as 
select bestandid, url, original_ort,
       string_agg(matched_ort_word, ' ' order by ordinality) as matched_ort
from temp_ort_matches_monyh_ch
group by bestandid, url, original_ort;


--/////////////////////////////////////////////////////////

*/






























