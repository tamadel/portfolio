--==========================
-- AZD Test Data
-- Date: 22.01.2025
--==========================
--(I)Process Google search Organic "search.ch"
--2106
select 
	* 
from 
	az_testdata.az_search_url
where 
    type = 'organic' 
    and
   	domain in ('search.ch', 'www.search.ch')
;

-- zefix Urls contains lots of information 
select * from az_testdata.az_search_url where type = 'organic' and domain like '%zefix%';



--============================================
-- Step 1: Create a temp table for split URLs
--1385
--===========================================
drop table if exists temp_split_url_sear_ch;
create temp table 
	temp_split_url_sear_ch
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
    regexp_replace(
    regexp_replace(
    regexp_replace(url, '^https?://(www\.)?', ''), 	-- Remove protocol and "www."
    '\.ch|\.com|\.de', '' ),						-- Remove ".ch", ".com", ".de" wherever they appear
    '/$', '' 	  ), 								-- Remove trailing "/"
    '/+', '/'     ), 								-- Replace multiple "/" with a single "/"
    '\\?was=',''  ),
    '\+', ' '     ),
	'\?', ''	  ),
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
			  	regexp_replace(
			    regexp_replace(
			    regexp_replace(
			    regexp_replace(url, '^https?://(www\.)?', ''), 	-- Remove protocol and "www."
			    '\.ch|\.com|\.de', '' ),						-- Remove ".ch", ".com", ".de" wherever they appear
			    '/$', '' 	  ), 								-- Remove trailing "/"
			    '/+', '/'     ), 								-- Replace multiple "/" with a single "/"
			    '\\?was=',''  ),
			    '\+', ' '     ),
			    '\?', ''	  ),
			    '\.en','' 	  ),
			    '\.fr','' 	  ),
			    '\.de','' 	  ),
			    '\.it','' 	  ),
			    '\.html',''	  ),								-- remove .html .fr .it .en 
			    '/' 		  ), 											-- Split by "/"
        		'' 												-- Remove empty elements from the array
    ) as url_parts
from   
    az_testdata.az_search_url
where 
    type = 'organic' 
    and
   	domain in ('search.ch', 'www.search.ch')
;

select * from temp_split_url_sear_ch where bestandid in ('1185653' , '1558283');

-- extract the name , address and ort from search.ch urls
drop table if exists temp_url_sear_ch;
create temp table 
	temp_url_sear_ch
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
	regexp_replace(
	regexp_replace(
	regexp_replace(
  	regexp_replace(
  	regexp_replace(
	regexp_replace(
	regexp_replace(
    regexp_replace(split_part(cleaned_url, '/', -1), '\.', ' ', 'g'), -- Replace dots with spaces
    '-', ' ', 'g' ),   	-- Replace hyphens with spaces  
    '\+', ' ', 'g'),	-- Replace plus with spaces       
    '%', ' ', 'g' ), 	-- Replace percent with spaces     
    '&', ' ', 'g' ), 	-- Replace ampersand with spaces    
    '=', ' ', 'g' ), 	-- Replace equal with spaces 
    '\?', ' ', 'g'),	-- Replace question mark with spaces
    'ä', 'ae', 'g'),
    'ö', 'oe', 'g'),
  	'ü', 'ue', 'g'),
    'ß', 'ss', 'g'),
    'é', 'e', 'g' ),
    'è', 'e', 'g' ),
    'ê', 'e', 'g' ),
    'ç', 'c', 'g' ),
    'á', 'a', 'g' ),
    'à', 'a', 'g' 
    )) as search_ch_name,
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
	regexp_replace(
	regexp_replace(
	regexp_replace(
  	regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(split_part(cleaned_url, '/', -2), '\.', ' ', 'g'), -- Replace dots with spaces
    '-', ' ', 'g' ),   	-- Replace hyphens with spaces  
    '\+', ' ', 'g'),	-- Replace plus with spaces       
    '%', ' ', 'g' ), 	-- Replace percent with spaces     
    '&', ' ', 'g' ), 	-- Replace ampersand with spaces    
    '=', ' ', 'g' ), 	-- Replace equal with spaces 
    '\?', ' ', 'g'),	-- Replace question mark with spaces
    'ä', 'ae', 'g'),
    'ö', 'oe', 'g'),
  	'ü', 'ue', 'g'),
    'ß', 'ss', 'g'),
    'é', 'e', 'g' ),
    'è', 'e', 'g' ),
    'ê', 'e', 'g' ),
    'ç', 'c', 'g' ),
    'á', 'a', 'g' 	),
    'à', 'a', 'g' 
    )) as search_ch_address,
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
	regexp_replace(
	regexp_replace(
	regexp_replace(
	regexp_replace(
  	regexp_replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(split_part(cleaned_url, '/', -3), '\.', ' ', 'g'), -- Replace dots with spaces
    '-', ' ', 'g' ),   	-- Replace hyphens with spaces  
    '\+', ' ', 'g'),	-- Replace plus with spaces       
    '%', ' ', 'g' ), 	-- Replace percent with spaces     
    '&', ' ', 'g' ), 	-- Replace ampersand with spaces    
    '=', ' ', 'g' ), 	-- Replace equal with spaces 
    '\?', ' ', 'g'),	-- Replace question mark with spaces
    'ä', 'ae', 'g'),
    'ö', 'oe', 'g'),
  	'ü', 'ue', 'g'),
    'ß', 'ss', 'g'),
    'é', 'e', 'g' ),
    'è', 'e', 'g' ),
    'ê', 'e', 'g' ),
    'ç', 'c', 'g' ),
    'á', 'a', 'g' ),
    'à', 'a', 'g' 
    )) as search_ch_ort,
    url_parts
from
	temp_split_url_sear_ch
;

select * from temp_url_sear_ch where bestandid in ('1185653' , '1558283');







--==================================================
-- Step 2: Create a temp table for normalized data
-- 1380  -- '\.', ' ', 'g'),
--==================================================
drop table if exists temp_normalized_url_sear_ch;
create temp table
	temp_normalized_url_sear_ch
as 
select  
    bestandid,
    original_name,
	search_ch_name,
    google_organic_name,
    original_address,
    search_ch_address,
    plz,
    original_ort,
    search_ch_ort,
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
    array_agg( trim(
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
    temp_url_sear_ch,
    unnest(url_parts) with ordinality as t(part_value, part_number)
group by 
    bestandid, original_name, google_organic_name, original_address, plz, 
    original_ort, uid_no, telefon, url, domain, rank, cleaned_url,url_parts,
    search_ch_name, search_ch_address, search_ch_ort, part_number
;



select * from temp_normalized_url_sear_ch where bestandid in ('1185653' , '1558283');


--=========================
-- Word-to-Word comparison
--=========================
-----------------------------------------------
--Split Words from Each Field into Temp Tables
-----------------------------------------------
-- Step 2.1: Split words from part_value into a temp table
drop table if exists temp_part_value_words_sear_ch;
create temp table 
	temp_part_value_words_sear_ch 
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
    temp_normalized_url_sear_ch,
    lateral unnest(normalized_part_value) as u(normalized_value),
    lateral regexp_split_to_table(trim(normalized_value), '\s+') with ordinality as t(word, ordinality)
order by  
    bestandid,
    url,
    part_number
;


select * from temp_part_value_words_sear_ch where bestandid in ('1185653' , '1558283');
--
-- Step 2.2: Split words from original_name into a temp table
drop table if exists temp_original_name_words_sear_ch;
create temp table 
	temp_original_name_words_sear_ch 
as 
select distinct
    bestandid,
    url,
    part_number,
	url_parts,
    word,
    ordinality
from
    temp_normalized_url_sear_ch,
    lateral unnest(normalized_original_name) as u(normalized_name),
    lateral regexp_split_to_table(trim(normalized_name), '\s+') with ordinality as t(word, ordinality)
order by  
    bestandid,
    url,
    part_number
;


select * from temp_original_name_words_sear_ch where bestandid in ('1185653' , '1558283');


-- Step 2.3: Split words from original_address into a temp table
--normalized_original_address
drop table if exists temp_original_address_words_sear_ch;
create temp table
	temp_original_address_words_sear_ch 
as 
select distinct
    bestandid,
    url,
    part_number,
	url_parts,
    word,
    ordinality
from
    temp_normalized_url_sear_ch,
    lateral unnest(normalized_original_address) as u(normalized_address),
    lateral regexp_split_to_table(trim(normalized_address), '\s+') with ordinality as t(word, ordinality)
order by  
    bestandid,
    url,
    part_number
;


select * from temp_original_address_words_sear_ch where bestandid in ('1185653' , '1558283');

-- Step 2.4: Split words from original_ort into a temp table
--
drop table if exists temp_original_ort_words_sear_ch;
create temp table 
	temp_original_ort_words_sear_ch 
as 
select distinct 
    bestandid,
    url,
    part_number,
	url_parts,
    word,
    ordinality
from
    temp_normalized_url_sear_ch,
    lateral unnest(normalized_original_ort) as u(normalized_ort),
    lateral regexp_split_to_table(trim(normalized_ort), '\s+') with ordinality as t(word, ordinality)
order by  
    bestandid,
    url,
    part_number
;

select * from temp_original_ort_words_sear_ch;



----------------------------------
--Compare Words and Find Matches
----------------------------------
-- Step 3.1: Compare part_value words with original_name words
drop table if exists temp_name_matches_sear_ch;
create temp table
	temp_name_matches_sear_ch
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
    temp_part_value_words_sear_ch p
join  
    temp_original_name_words_sear_ch n
on  
   	p.bestandid = n.bestandid 
   	and 
   	(
        p.word ilike n.word 
       	or
       	similarity(p.word, n.word) > 0.8	-- (Similarity) If you want stricter matching, increase the threshold (e.g., 0.7 or 0.8).
       	or
       	levenshtein(p.word, n.word) <= 1  	-- (Levenshtein) If you want stricter matching, reduce it to ≤1 or =0 (exact match). If you want more flexibility, increase to ≤3.
    )
   	and 
  	p.url = n.url
where 
	p.part_number = (
			        select
			        	MAX(part_number) 
			        from
			        	temp_part_value_words_sear_ch 
			        where
			        	bestandid = p.bestandid
			          	and
			          	url = p.url
    )
;

select * from temp_name_matches_sear_ch where bestandid in ('1178263' , '1179972'); --bestandid in ('1185653' , '1558283');

-- Step 3.2: Compare part_value words with original_address words
drop table if exists temp_address_matches_sear_ch;
create temp table
	temp_address_matches_sear_ch
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
    temp_part_value_words_sear_ch p
join  
    temp_original_address_words_sear_ch a
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
where 
	p.part_number = (
			        select
			        	MAX(part_number) - 1
			        from
			        	temp_part_value_words_sear_ch 
			        where
			        	bestandid = p.bestandid
			          	and
			          	url = p.url
    )
;


select * from temp_address_matches_sear_ch where bestandid in ('1178263' , '1179972'); --bestandid in ('1185653' , '1558283');

-- Step 3.3: Compare part_value words with original_ort words
drop table if exists temp_ort_matches_sear_ch;
create temp table
	temp_ort_matches_sear_ch
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
    temp_part_value_words_sear_ch p
join  
    temp_original_ort_words_sear_ch o
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
where 
	p.part_number = (
			        select
			        	MAX(part_number) - 2
			        from
			        	temp_part_value_words_sear_ch 
			        where
			        	bestandid = p.bestandid
			          	and
			          	url = p.url
    )
;


select * from temp_ort_matches_sear_ch;



---------------------------------------------------------------------------
-- aggregate matches and handle duplicates and ordinality
---------------------------------------------------------------------------

-----------------------
-- Name Match
-----------------------
-- Deduplicate matched words for name while keep their order
drop table if exists temp_name_matches_deduplicated_sear_ch;
create temp table
	temp_name_matches_deduplicated_sear_ch 
as 
select distinct
    bestandid,
    url,
    original_name,
    matched_name_word,
    ordinality -- keep the original order
from  
    temp_name_matches_sear_ch
order by  
    bestandid,
    url,
    matched_name_word,
    ordinality
 ; 

select * from temp_name_matches_deduplicated_sear_ch where bestandid in ('1185653' , '1558283');
   

-- list to string 
drop table if exists temp_aggregated_name_matches_sear_ch;   
create temp table
	temp_aggregated_name_matches_sear_ch
as 
select 
    bestandid,
    url,
    original_name,
    -- Convert the array to a string with ordering
    string_agg(matched_name_word, ' ' order by ordinality) as matched_name
from  
    temp_name_matches_deduplicated_sear_ch
group by 
    bestandid, url, original_name
;

select * from temp_aggregated_name_matches_sear_ch where bestandid in ('1185653' , '1558283');


-----------------------
-- Address Match
-----------------------
-- Deduplicate matched words for address while keep their order
--
drop table if exists temp_address_matches_deduplicated_sear_ch;
create temp table 
	temp_address_matches_deduplicated_sear_ch 
as 
select distinct
    bestandid,
    url,
    original_address,
    matched_address_word,
    ordinality -- keep the original order
from  
    temp_address_matches_sear_ch
order by  
    bestandid,
    url,
    matched_address_word,
    ordinality desc
 ; 

select * from temp_address_matches_deduplicated_sear_ch;

-- list to string
drop table if exists temp_aggregated_address_matches_sear_ch;
create temp table 
	temp_aggregated_address_matches_sear_ch 
as 
select 
    bestandid,
    url,
    original_address,
    -- Convert the array to a string with ordering
    string_agg(matched_address_word, ' ' order by ordinality) as matched_address
from  
    temp_address_matches_deduplicated_sear_ch
group by 
    bestandid, url, original_address
;


select * from temp_aggregated_address_matches_sear_ch;

-----------------------
-- Ort Match
-----------------------
-- Deduplicate matched words for ort while keep their order
--
drop table if exists temp_ort_matches_deduplicated_sear_ch;
create temp table 
	temp_ort_matches_deduplicated_sear_ch
as 
select distinct 
    bestandid,
    url,
    original_ort,
    matched_ort_word,
    ordinality -- keep the original order
from  
    temp_ort_matches_sear_ch
order by  
    bestandid,
    url,
    matched_ort_word,
    ordinality
 ; 

select * from temp_ort_matches_deduplicated_sear_ch;


-- list to string
drop table if exists temp_aggregated_ort_matches_sear_ch;
create temp table 
	temp_aggregated_ort_matches_sear_ch 
as 
select 
    bestandid,
    url,
    original_ort,
    -- Convert the array to a string with ordering
    string_agg(matched_ort_word, ' ' order by ordinality) as matched_ort
from  
    temp_ort_matches_deduplicated_sear_ch
group by 
    bestandid, url, original_ort
;

select * from temp_aggregated_ort_matches_sear_ch;



-----------------------------------------------
-- collect matches into the final results table
-----------------------------------------------
drop table if exists tmp_final_results_sear_ch_v;
create table 
	tmp_final_results_sear_ch_v
as
select distinct
    n.bestandid,
    n.uid_no,
    n.url, 
    n.rank, 
   	--anm.part_value as url_name,
    n.original_name,
    n.search_ch_name,
    COALESCE(anm.matched_name, null) as matched_name,
    --aam.part_value as url_address,
    n.original_address,
    n.search_ch_address,
    COALESCE(aam.matched_address, null) as matched_address,
    --aom.part_value as url_ort,
    n.original_ort,
    n.search_ch_ort,
    COALESCE(aom.matched_ort, null) as matched_ort
from  
    temp_normalized_url_sear_ch n
left join  
    temp_aggregated_name_matches_sear_ch anm 
on
	n.bestandid = anm.bestandid
	and 
	n.url = anm.url
left join  
    temp_aggregated_address_matches_sear_ch aam 
on 
	n.bestandid = aam.bestandid
	and
	n.url = aam.url
left join  
    temp_aggregated_ort_matches_sear_ch aom 
on
	n.bestandid = aom.bestandid
	and 
	n.url = aom.url
;


select * from tmp_final_results_sear_ch_v where bestandid in ('1185653' , '1558283');


drop table if exists az_testdata.az_search_url_sear_ch_v;
create table 
	az_testdata.az_search_url_sear_ch_v
as
select distinct
	* 
from 
	tmp_final_results_sear_ch_v;


select count(distinct bestandid) from az_testdata.az_search_url_sear_ch_v; --793
select count(distinct bestandid) from az_testdata.az_search_url_sear_ch_v  where matched_name is not null; --249 --351
select * from az_testdata.az_search_url_sear_ch_v;



















/*
--============================================
-- URL Decode Function
--============================================
CREATE OR REPLACE FUNCTION url_decode(encoded_text TEXT) 
RETURNS TEXT AS $$
DECLARE
    decoded_text TEXT := encoded_text;
    percent_match TEXT;
    hex_value TEXT;
    utf_char TEXT;
BEGIN
    -- Recursively replace %XX encoded characters
    LOOP
        -- Find the first %XX pattern (ensuring exactly 2 hex digits)
        percent_match := substring(decoded_text FROM '%([0-9A-Fa-f]{2})');

        -- Exit if no more %XX patterns are found
        EXIT WHEN percent_match IS NULL;

        -- Extract only the valid two-digit hex part (avoids odd-length errors)
        hex_value := substring(percent_match FROM 2 FOR 2);

        -- Validate that hex_value is exactly 2 hex digits before decoding
        IF hex_value ~ '^[0-9A-Fa-f]{2}$' THEN
            -- Convert hex to actual UTF-8 character
            utf_char := convert_from(decode(hex_value, 'hex'), 'UTF8');

            -- Replace %XX in the string with its actual character
            decoded_text := replace(decoded_text, percent_match, utf_char);
        ELSE
            -- If invalid, just remove the `%`
            decoded_text := replace(decoded_text, percent_match, '');
        END IF;

    END LOOP;

    -- Replace `+` with spaces to restore original formatting
    RETURN replace(decoded_text, '+', ' ');
END;
$$ LANGUAGE plpgsql IMMUTABLE;


--////////////////////////////////////////////
CREATE OR REPLACE FUNCTION url_decode(encoded_text TEXT)
RETURNS TEXT AS $$
DECLARE
    decoded_text TEXT := '';
    bytea_val BYTEA;
BEGIN
    -- Replace %XX with \xXX to create a valid bytea hex string
    decoded_text := replace(encoded_text, '%', '\x');

    -- Convert the hex string to bytea
    BEGIN
        bytea_val := decode(decoded_text, 'hex');
    EXCEPTION WHEN others THEN
        -- If decoding fails, return the original text
        RETURN encoded_text;
    END;

    -- Convert bytea to UTF-8 text
    decoded_text := convert_from(bytea_val, 'UTF8');

    -- Replace `+` with spaces to restore original formatting
    RETURN replace(decoded_text, '+', ' ');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

--//////////////////////////////////////////////

select
url_decode(url::text) AS cleaned_value 
FROM 
    temp_split_url_sear_ch 
WHERE 
    bestandid in ('1500227', '6276360');

*/





































--////////////////////////DRAFT///////////////////////////////////////////

--========================
-- Web Scraping Data
--========================
select count(distinct bestand_id) from web_scraping.search_ch;
select * from web_scraping.search_ch;
select * from az_testdata.az_search_url_sear_ch_v1;



create table geo_afo_tmp.tmp_scrap_search_ch
as
select distinct 
	t1.bestandid 
	,t1.uid_no 
	,t1.url 
	,t1."rank" 
	,t1.original_name
	,t1.search_ch_name
	,t2.company_name as scrap_name
	,t1.matched_name
	,t1.original_address 
	,t1.search_ch_address
	,t2.address as scrap_address 
	,t1.matched_address
	,t2.phone
	,t1.original_ort 
	,t1.search_ch_ort 
	,t1.matched_ort 
from
	az_testdata.az_search_url_sear_ch_v1 t1
left join 
	web_scraping.search_ch t2
on
	t1.bestandid = t2.bestand_id::text 
	and
	t1.url = t2.url
;

select 
	bestandid 
	,original_name 
	,scrap_name 
	,phone
	,original_address 
	,scrap_address 
	,url 
from 
	geo_afo_tmp.tmp_scrap_search_ch
where 
	scrap_name is not null  
;























