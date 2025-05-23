--==========================
-- AZD Test Data
-- Date: 20.01.2025
--==========================
--(II)Process Moneyhouse.ch Data
--1623
select 
	* 
from 
	az_testdata.az_search_url
where 
 	type = 'organic' 
 	and
 	domain in ('moneyhouse.ch', 'www.moneyhouse.ch')
;

-- Step 1: Create a temp table for split URLs
drop table if exists temp_split_url_mh;
create temp table 
	temp_split_url_mh 
as	
select   
    bestandid,
    name1 as original_name,
    zuststrasse as original_address,
    zustplz as plz,
    zustort as original_ort,
    uid_no,
    telefon,
    url,
    domain,
    rank_absolute as rank,
    regexp_replace(url, '^https?://', '') as cleaned_url,
    array_remove(string_to_array(regexp_replace(url, '^https?://', ''), '/'), '') as url_parts,
    case  
        when
        	regexp_replace(url, '^https?://', '') ~ '[0-9]+$' 
        then  
            regexp_replace(regexp_replace(url, '^https?://', ''), '.*[^0-9]([0-9]+)$', '\1')
        else null  
    end as last_url_num 
from  
    az_testdata.az_search_url
where 
 	type = 'organic' 
 	and
 	domain in ('moneyhouse.ch', 'www.moneyhouse.ch')
;

select * from az_testdata.az_search_url;
select * from temp_split_url_mh;


-- Step 2: Create a temp table for normalized data
drop table if exists temp_normalized_url_mh;
create temp table
	temp_normalized_url_mh 
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
    last_url_num,
    part_number,
    part_value,
    -- Normalize German and French special characters, and replace hyphens in part_value
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
                          lower(part_value), -- Convert to lowercase
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
      ) AS normalized_part_value,
    -- Normalize German and French special characters in original_name
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
      ) as normalized_original_name
from  
    temp_split_url_mh,
    unnest(url_parts) with ordinality as t(part_value, part_number)
;

select * from temp_normalized_url_mh;


--=========================
-- Word-to-Word Comparison (Focus on Name)
--=========================
-----------------------------------------------
--Split Words from Each Field into Temp Tables
-----------------------------------------------
-- Split words from part_value into a temp table
drop table if exists temp_part_value_words_mh;
create temp table  
    temp_part_value_words_mh 
as 	
select  
    bestandid,
    part_number,
    part_value,
    word,
    ordinality
from 
    temp_normalized_url_mh,
    regexp_split_to_table(normalized_part_value, '\s+') with ordinality as t(word, ordinality)
;

select * from temp_part_value_words_mh;

-- Split words from original_name into a temp table
drop table if exists temp_original_name_words_mh;
create temp table  
    temp_original_name_words_mh 
as  
select  
    bestandid,
    part_number,
    part_value,
    word,
    ordinality
from 
    temp_normalized_url_mh,
    regexp_split_to_table(normalized_original_name, '\s+') WITH ORDINALITY AS t(word, ordinality)
;

select * from temp_original_name_words_mh;


----------------------------------
--Compare Words and Find Matches
----------------------------------
-- Compare part_value words with original_name words
drop table if exists temp_name_matches_mh;
create temp table 
    temp_name_matches_mh 
as  
select  distinct
    p.bestandid,
    p.part_number,
    p.part_value,
    p.ordinality,
    p.word as matched_name_word,
    n.word as original_name_word
from   
    temp_part_value_words_mh p
join   
    temp_original_name_words_mh n
on   
    p.bestandid = n.bestandid 
    and  
    p.word ilike n.word
;


select * from temp_name_matches_mh;

---------------------------------------------------------------------------
-- aggregate name matches with DISTINCT to remove duplicates and keep ordinality
---------------------------------------------------------------------------
-- Deduplicate matched words for name while keeping their order
drop table if exists temp_name_matches_deduplicated_mh;
create temp table
    temp_name_matches_deduplicated_mh 
as  
select distinct on (bestandid, matched_name_word) 
    bestandid,
    part_value,
    matched_name_word,
    ordinality -- Keep the original order
from   
    temp_name_matches_mh
order by   
    bestandid, 
    matched_name_word, 
    part_value,
    ordinality
;


select * from temp_name_matches_deduplicated_mh;



-- Aggregate deduplicated name matches
drop table if exists temp_aggregated_name_matches_mh;   
create temp table
    temp_aggregated_name_matches_mh 
as  
select  
    bestandid,
    part_value,
    STRING_AGG(matched_name_word, ' ' order by ordinality) as matched_name 
from   
    temp_name_matches_deduplicated_mh
group by   
    bestandid,
    part_value
;

select * from temp_aggregated_name_matches_mh;


-----------------------------------------------
-- collect matches into the final results table
-----------------------------------------------
-- Final Results
drop table if exists temp_final_results_mh;
create temp table
    temp_final_results_mh 
as  
select  distinct
   	n.bestandid,
    n.uid_no,
    n.telefon,
    n.url, 
    n.rank,
    regexp_replace(anm.part_value, '-[0-9]+$', '') AS url_name,
    n.original_name,
    COALESCE(anm.matched_name, null) as matched_name,
    n.last_url_num
from   
    temp_normalized_url_mh n
left join   
    temp_aggregated_name_matches_mh anm 
on 
    n.bestandid = anm.bestandid
;

select * from temp_final_results_mh;

-- Final Results
drop table if exists az_testdata.az_search_url_moneyhouse;
create table 
	az_testdata.az_search_url_moneyhouse
as
select   
    bestandid,
    uid_no,
    telefon,
    url,
    rank,  
    url_name,
    original_name,
    matched_name,
    last_url_num
from   
    temp_final_results_mh
order by   
    rank
;
--6205648



select * from az_testdata.az_search_url_moneyhouse;



--=====================
--unternehmen
--=====================
DROP table if exists
	geo_zefix.unternehmen;

create table
	geo_zefix.unternehmen
as
SELECT 
	*
FROM 
	dblink(
		'geo_database',
		$POSTGRES$
			select
				* 
			from
				geo_zefix.unternehmen
		$POSTGRES$
	) AS unternehmen (
		uid_no varchar(250)
		,deletiondate date 
		,ehraid int4 
		,"name" varchar(500) 
		,registryofcommeceid int4 
		,sogsdate date 
		,status varchar(15) 
		,chid varchar(250) 
		,purpose text 
		,address varchar(255) 
		,idlegalseat int4 
		,legalseat varchar(255) 
		,legalform int4 
		,legalform_de varchar(250) 
		,legalform_it varchar(250) 
		,legalform_fr varchar(250) 
		,legalform_en varchar(250) 
		,gueltig_von date 
		,gueltig_bis date 
		,"timestamp" timestamp
	)
;

select * from geo_zefix.unternehmen;




select 
	mh.*
	,s.*
	,u.name
	,u.uid_no
	,u.chid
	,u.status
from 
	az_testdata.az_search_url_moneyhouse mh
left join 
	az_testdata.az_search_url_search_ch s
on
	mh.bestandid = s.bestandid  
left join 
	geo_zefix.unternehmen u
on
	mh.original_name ilike u.name
;













