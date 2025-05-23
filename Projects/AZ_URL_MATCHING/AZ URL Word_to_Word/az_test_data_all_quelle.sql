
drop table if exists
	az_testdata.az_testdaten_url_recherche;
create table
	az_testdata.az_url_recherche
as
select  
	*
from  
	dblink(
		'geo_database',
		$POSTGRES$
			select
				* 
			from
				az_testdata.az_testdaten_url_recherche
		$POSTGRES$
	) as az_url_recherche (
			bestandid text 
			,name1 varchar(50) 
			,name2 varchar(50) 
			,name3 varchar(50) 
			,zusatzzeile varchar(50) 
			,zuststrasse varchar(50) 
			,zustplz numeric 
			,zustort varchar(50) 
			,uid_no varchar(50) 
			,jurname1 varchar(50) 
			,jurname2 varchar(50) 
			,jurname3 varchar(50) 
			,jurname4 varchar(50) 
			,jurname5 varchar(50) 
			,telefon text 
			,"bemerkung ph" varchar(128) 
			,title text 
			,keyword text
	)
;

select * from az_testdata.az_url_recherche;

--===============================
-- AZD URL Data all Quelle
--===============================
drop table if exists az_testdata.az_url_data_all_v3;
create table 
	az_testdata.az_url_data_all_v3
as
select 
	*,
	'search.ch' as quelle
from 
	az_testdata.az_search_url_sear_ch_v2 --1380
union all 
select 
	*,
	'moneyhouse.ch' as quelle
from 
	az_testdata.az_search_url_monyh_ch_v2 --1619
union all
select
	*,
	'google_organic' as quelle
from 
	az_testdata.az_search_url_all_org_v2 --v2: 7055 v1: 6776
union all
select
	*,
	'google_map' as quelle
from 
	az_testdata.az_map_url_gmap_v2
;

select * from az_testdata.az_url_data_all_v3;




	
-----------------------	
-- update: 21.02.2025
-----------------------
-- Function to normalize the string
create or replace function normalize_string(input_text TEXT)
returns TEXT as $$
begin 
    -- Remove all characters except alphanumeric and spaces
    return regexp_replace(input_text, '[^a-zA-Z0-9\s]', '', 'g');
end;
$$ language plpgsql immutable;




drop table if exists geo_afo_tmp.az_urls_all_v;

create table geo_afo_tmp.az_urls_all_test
as
select distinct 
	az.bestandid  			 		as id
	,az.uid_no  					as az_uid_no
	,az.keyword 		 			as az_name
	,az.zuststrasse		 			as az_address
	,az.zustort		 				as az_ort
	,sc.url 						as search_url
	,sc."rank" 						as search_rank
	,sc.search_ch_name 				as search_ch_name
	,sc.matched_name 				as search_matched_nam
	,sc.search_ch_address 			as search_ch_address
	,sc.matched_address 			as search_matched_adrs
	,sc.search_ch_ort 				as search_ch_ort
	,sc.matched_ort 				as search_matched_ort
	,mh.url 						as moneyhouse_url
	,mh."rank" 						as moneyhouse_rank
	,mh.moneyhouse_name 			as moneyhouse_name
	,mh.matched_name 				as moneyhouse_matched_nam
	,org.url 						as organic_url
	,org."rank" 					as organic_rank
	,org.domain 					as organic_domain
	,org.matched_name  				as organic_matched_nam
	,org.matched_address 			as organic_matched_adrs
	,org.matched_ort 				as organic_matched_ort
	,mp.url 						as map_url
	,mp."rank" 						as map_rank
	,mp.domain 						as map_domain
	,mp.matched_name 				as map_matched_nam
	,mp.matched_address 			as map_matched_adrs
	,mp.matched_ort 				as map_matched_ort
	--,ROW_NUMBER() over (partition by az.bestandid, sc.url  	order by sc."rank" 	) 			as search_row_num_r
    --,ROW_NUMBER() over (partition by az.bestandid, mh.url  	order by mh."rank" 	) 			as moneyhouse_row_num_r
    --,ROW_NUMBER() over (partition by az.bestandid, org.url  order by org."rank" ) 			as organic_row_num_r
    --,ROW_NUMBER() over (partition by az.bestandid, mp.url  	order by mp."rank" 	) 			as map_row_num_r
from 
	az_testdata.az_url_recherche az
left join
	az_testdata.az_search_url_monyh_ch_v mh
on
	az.bestandid = mh.bestandid 
left join
	az_testdata.az_search_url_sear_ch_v sc
on
	az.bestandid = sc.bestandid 
left join 
	az_testdata.az_search_url_all_org_v org
on 
	az.bestandid = org.bestandid
left join 
	az_testdata.az_map_url_gmap_v mp
on
	az.bestandid = mp.bestandid 
order by 
	search_rank,
	moneyhouse_rank,
	organic_rank,
	map_rank
;



select count(distinct id) from geo_afo_tmp.az_urls_all_v;	
select * from geo_afo_tmp.az_urls_all; -- 40809
select * from geo_afo_tmp.az_urls_all_v; -- 40809


select 
	count(distinct id)
from
	geo_afo_tmp.az_urls_all_v
where 
	--search_matched_nam is null 				--507
	--moneyhouse_matched_nam is null  			--582
	--organic_matched_nam is null 				--760
	map_matched_nam is null 					--681
;	
	
	




select * from geo_zefix.unternehmen;

--uid_no = 'CHE108084403'

-- Add indexes if they don't exist
drop index if exists geo_afo_tmp.az_name_idx;
CREATE INDEX IF NOT EXISTS az_name_idx ON geo_afo_tmp.az_urls_all_v (normalize_string(lower(az_name)));

drop index if exists geo_zefix.name_idx;
CREATE INDEX IF NOT EXISTS name_idx ON geo_zefix.unternehmen (normalize_string(lower(name)));


drop table if exists az_testdata.az_urls_all_uid;
create table az_testdata.az_urls_all_uid_v
as
select 
	az.*
	,ut.name 						as name_afo
	,ut.uid_no 						as uid_no_afo
	,ut.chid 						as ch_id_afo
	,ut.status 						as status_afo
from 
	geo_afo_tmp.az_urls_all_v az
left join 
	geo_zefix.unternehmen ut
on
	normalize_string(lower(az.az_name)) = normalize_string(lower(ut.name))
	or 
	normalize_string(lower(az.moneyhouse_name)) = normalize_string(lower(ut.name)) 
;



select * from az_testdata.az_urls_all_uid_v; -- 46443
select count(distinct id) from az_testdata.az_urls_all_uid_v; --999 


-- there are differents uid for one name depending on az_name and moneyhouse.ch_name
-- I think this would help to filiter out the correct url ex:id = '1188138' or id = '1185589'  
select 
	id,
	count(*) as n_count
from
	az_testdata.az_urls_all_uid_v
group by
	id 
having 
	count(*)>1
order by 
	n_count desc
;


select 
	id,
	count(*) as n_count
from
	geo_afo_tmp.az_urls_all
group by
	id 
having 
	count(*)>1
order by 
	n_count desc
;



select 
	* 
from 
	az_testdata.az_urls_all_uid
where 
	 id not in (select bestandid from az_testdata.az_url_recherche)
;


select count(distinct bestandid) from az_testdata.az_url_recherche;
select * from az_testdata.az_url_recherche;
--===================================================================


-- clean up some duplication 
drop table if exists az_testdata.az_all_urls_uid_v1;
create table 
	az_testdata.az_all_urls_uid_v1
as
with RankedUrls as (
    select  
	    id 
		,az_uid_no 
		,az_name 
		,az_address 
		,az_ort 
		,search_url 
		,search_rank
		,search_ch_name 
		,search_matched_nam 
		,search_ch_address 
		,search_matched_adrs 
		,search_ch_ort 
		,search_matched_ort 
		,moneyhouse_url 
		,moneyhouse_rank
		,moneyhouse_name 
		,moneyhouse_matched_nam 
		,organic_url 
		,organic_rank
		,organic_domain 
		,organic_matched_nam 
		,organic_matched_adrs 
		,organic_matched_ort 
		,map_url 
		,map_rank
		,map_domain 
		,map_matched_nam 
		,map_matched_adrs 
		,map_matched_ort 
		,name_afo
		,uid_no_afo
		,ch_id_afo
		,status_afo  
		,ROW_NUMBER() OVER (PARTITION BY id, search_rank ORDER BY search_url) 					as search_row_num_r
        ,ROW_NUMBER() OVER (PARTITION BY id, moneyhouse_rank ORDER BY moneyhouse_url) 			as moneyhouse_row_num_r
        ,ROW_NUMBER() OVER (PARTITION BY id, organic_rank ORDER BY organic_url) 				as organic_row_num_r
        ,ROW_NUMBER() OVER (PARTITION BY id, map_rank ORDER BY map_url) 						as map_row_num_r
        --,ROW_NUMBER() over (partition by id, search_url order by search_rank asc) 				as search_row_num_u
        --,ROW_NUMBER() over (partition by id, moneyhouse_url order by moneyhouse_rank asc) 		as moneyhouse_row_num_u
        --,ROW_NUMBER() over (partition by id, organic_url order by organic_rank asc) 				as organic_row_num_u
        --,ROW_NUMBER() over (partition by id, map_url order by map_rank asc) 						as map_row_num_u
    from
    	az_testdata.az_urls_all_uid_v
)
select  
	 id 
	,az_uid_no 
	,az_name 
	,az_address 
	,az_ort 
	,search_url 
	,search_rank
	,search_ch_name 
	,search_matched_nam 
	,search_ch_address 
	,search_matched_adrs 
	,search_ch_ort 
	,search_matched_ort 
	,moneyhouse_url 
	,moneyhouse_rank
	,moneyhouse_name 
	,moneyhouse_matched_nam 
	,organic_url 
	,organic_rank
	,organic_domain 
	,organic_matched_nam 
	,organic_matched_adrs 
	,organic_matched_ort 
	,map_url 
	,map_rank
	,map_domain 
	,map_matched_nam 
	,map_matched_adrs 
	,map_matched_ort 
	,name_afo
	,uid_no_afo
	,ch_id_afo
	,status_afo 
from
	RankedUrls
where  
    search_row_num_r = 1 
    or
    moneyhouse_row_num_r = 1 
    or 
    organic_row_num_r = 1 
    or 
    map_row_num_r = 1 
;




select * from geo_afo_tmp.az_urls_all_v where id = '1007563';
select * from az_testdata.az_urls_all_uid_v where id = '1007563';
select * from az_testdata.az_all_urls_uid where id = '1007563';
select * from az_testdata.az_all_urls_uid_v where id = '1007563';
select * from az_testdata.az_all_urls_uid_v1 where id = '1007563';



select * from geo_afo_tmp.az_urls_all_v where id = '240';
select * from az_testdata.az_urls_all_uid_v where id = '240';
select * from az_testdata.az_all_urls_uid where id = '240';
select * from az_testdata.az_all_urls_uid_v where id = '240';
select * from az_testdata.az_all_urls_uid_v1 where id = '240';



select * from az_testdata.az_all_urls_uid; -- 10360
select * from az_testdata.az_all_urls_uid_v; --6173
select * from az_testdata.az_all_urls_uid_v1; -- 10685
select count(distinct id) from az_testdata.az_all_urls_uid; --999 
select count(distinct id) from az_testdata.az_all_urls_uid_v; --999 
select count(distinct id) from az_testdata.az_all_urls_uid_v1; --999




select count(distinct search_rank) from az_testdata.az_all_urls_uid_v1; 			--86
select count(distinct moneyhouse_rank) from az_testdata.az_all_urls_uid_v1; 		--90
select count(distinct organic_rank) from az_testdata.az_all_urls_uid_v1; 			--10
select count(distinct map_rank) from az_testdata.az_all_urls_uid_v1; 				--10

select 
	id,
	count(*) as n_count
from
	az_testdata.az_all_urls_uid
group by
	id 
having 
	count(*)>1
order by 
	n_count desc
;


--===============================================

select 
	* 
from 
	az_testdata.az_all_urls_uid_v1
where 
	id = '1003328'
;






--==========================================================
-- Update
-- Date: 25.02.2025 bis 27.02.2025
--==========================================================

-------------------------------------------------------------
--(I)moneyhouse.ch "az_testdata.az_search_url_monyh_ch_v"
-------------------------------------------------------------
drop table if exists az_testdata.az_moneyhouse_urls;
create table 
	az_testdata.az_moneyhouse_urls
as
select 
	*
from(
		select 
			*,
			row_number() over(partition by bestandid order by rank) as mon_rn
		from 
			az_testdata.az_search_url_monyh_ch_v
	)
where
	matched_name is not null
	and
	mon_rn = 1
order by 
	bestandid,
	rank
;



select * from az_testdata.az_moneyhouse_urls where bestandid in ('6190845', '6197306', '6199910', '6205124', '6205648', '6206232', '6209535', '6220626', '6241809', '6275537'); 

-- manuell 6190845 6197306 6199910 6205124 6205648 6206232 6209535 6220626 6241809 6275537


------------------------------------------------------------
--(II) search.ch "az_testdata.az_search_url_sear_ch_v"
------------------------------------------------------------
drop table if exists az_testdata.az_search_ch_urls;
create table 
	az_testdata.az_search_ch_urls
as
select 
	*
from(
		select 
			*,
			row_number() over(partition by bestandid order by rank) as serch_rn
		from 
			az_testdata.az_search_url_sear_ch_v
	)
where 
	matched_name is not null
	and
	serch_rn = 1
order by 
	bestandid,
	rank
;


select * from az_testdata.az_search_ch_urls where matched_address = '' bestandid in ('1771839', '1530009', '1185295', '1982974', '26007', '616444', '6197306', '6199910', '6206232', '6276360', '6241809', '6276360')

--test 1178263 1179972
--manuell 1771839  1530009  1185295  1982974  26007 616444 6197306  6199910  6206232 6276360 6241809 6276360 
-- sàrl bestandid in ('1185450' , '1185561' ,  '1185709')




-----------------------------------------------------------
--(III) Organic "az_testdata.az_search_url_all_org_v"
-----------------------------------------------------------
drop table if exists az_testdata.az_organic_urls;
create table 
	az_testdata.az_organic_urls
as
select 
	*
from(
		select 
			*,
			regexp_replace(
    		regexp_replace(
        	regexp_replace(
            regexp_replace(domain, '^www\.', '', 'i'), 
            '\.(ch|com|fr|it|net|org|li)$', '', 'i' ), 
        	'-', ' ', 'g'), 
    		'\s+', '', 'g'
			) as normalized_domain,
			row_number() over(partition by bestandid order by rank) as org_rn
		from 
			az_testdata.az_search_url_all_org_v
	)
where
	(
		lower(original_name) ilike '%' || lower(normalized_domain) || '%'
		or 
		lower(matched_name) ilike '%' || lower(normalized_domain) || '%'
	)
	--and 
	--(
	--	matched_name is not null 
	--	or 
	--	matched_address is not null
	--)
	and
	org_rn = 1
order by 
	bestandid,
	rank
;


select * from az_testdata.az_organic_urls; -- old 234  -- new 668



-------------------------------------------------
--(IV) Map "az_testdata.az_map_url_gmap_v"
-------------------------------------------------
drop table if exists az_testdata.az_map_urls;
create table 
	az_testdata.az_map_urls
as
select 
	*
from(
		select 
			*,
			regexp_replace(
    		regexp_replace(
        	regexp_replace(
            regexp_replace(domain, '^www\.', '', 'i'), 
            '\.(ch|com|fr|it|net|org|li)$', '', 'i' ), 
        	'-', ' ', 'g'), 
    		'\s+', '', 'g'
			) as normalized_domain,
			row_number() over(partition by bestandid order by rank) as map_rn
		from 
			az_testdata.az_map_url_gmap_v
	)
where 
	--(
	--	lower(original_name) ilike '%' || lower(normalized_domain) || '%'
	--	or 
	--	lower(matched_name) ilike '%' || lower(normalized_domain) || '%'
	--)
	--and 
	(
		matched_name is not null 
		or 
		matched_address is not null
	)
	and
	map_rn = 1
order by 
	bestandid,
	rank
;

select * from az_testdata.az_map_urls; --old 214 --new 367

---------------------------------------------------------------------------
-- URLs aus allen Quellen werden in einer einzigen Tabelle zusammengeführt.
---------------------------------------------------------------------------
drop table if exists geo_afo_tmp.az_urls_all_best;
create table geo_afo_tmp.az_urls_all_best
as
select distinct 
	az.bestandid  			 		as id
	,az.uid_no  					as az_uid_no
	,az.title 		 				as az_name
	,az.zuststrasse		 			as az_address
	,az.zustort		 				as az_ort
	,sc.url 						as search_url
	,sc."rank" 						as search_rank
	,sc.search_ch_name 				as search_ch_name
	,sc.matched_name 				as search_matched_nam
	,sc.search_ch_address 			as search_ch_address
	,sc.matched_address 			as search_matched_adrs
	,sc.search_ch_ort 				as search_ch_ort
	,sc.matched_ort 				as search_matched_ort
	,mh.url 						as moneyhouse_url
	,mh."rank" 						as moneyhouse_rank
	,mh.moneyhouse_name 			as moneyhouse_name
	,mh.matched_name 				as moneyhouse_matched_nam
	,org.url 						as organic_url
	,org."rank" 					as organic_rank
	,org.domain 					as organic_domain
	,org.normalized_domain 			as organic_norm_domain
	,org.matched_name  				as organic_matched_nam
	,org.matched_address 			as organic_matched_adrs
	,org.matched_ort 				as organic_matched_ort
	,mp.url 						as map_url
	,mp."rank" 						as map_rank
	,mp.domain 						as map_domain
	,mp.normalized_domain 			as map_norm_domain
	,mp.matched_name 				as map_matched_nam
	,mp.matched_address 			as map_matched_adrs
	,mp.matched_ort 				as map_matched_ort
from 
	az_testdata.az_url_recherche az
left join
	az_testdata.az_moneyhouse_urls mh --(I)
on
	az.bestandid = mh.bestandid 
left join
	az_testdata.az_search_ch_urls sc --(II)
on
	az.bestandid = sc.bestandid 
left join 
	az_testdata.az_organic_urls org --(III)
on 
	az.bestandid = org.bestandid
left join 
	az_testdata.az_map_urls mp --(IV)
on
	az.bestandid = mp.bestandid 
order by 
	search_rank,
	moneyhouse_rank,
	organic_rank,
	map_rank
;




-- all ranks
select * from geo_afo_tmp.az_urls_all;

-- best rank
select * from geo_afo_tmp.az_urls_all_best;

--------------------------------------------------------------
-- combine best urls and its rank with overall rank = 1
--------------------------------------------------------------
drop table if exists geo_afo_tmp.az_urls_best_combined;
create table 
	geo_afo_tmp.az_urls_best_combined
as
select
	* 
from (
	    select  
	        *,
	        -- Prioritize organic_url, then map_url, then the rest
	        case  
	            when 
	            		organic_url is not null
						            and 
						            	(
											lower(az_name) ilike '%' || lower(organic_norm_domain) || '%'
											or 
											lower(organic_matched_nam) ilike '%' || lower(organic_norm_domain) || '%'
										)
	            then 	organic_url
	            when 
            			map_url 	is not null
						           	and 
						            	(
											lower(az_name) ilike '%' || lower(organic_norm_domain) || '%'
											or 
											lower(map_matched_nam) ilike '%' || lower(map_norm_domain) || '%'
										)
	            then 	map_url
	         -- Compare search_url and moneyhouse_url ranks if organic and map are null
	            when
	            	search_url is not null and moneyhouse_url is not null  
	            then  
	                case  
	                    when search_rank <= moneyhouse_rank then search_url
	                    else moneyhouse_url
	                end    
	           	when 
	            		moneyhouse_url 	is not null 
	            then 	moneyhouse_url  
	            when 
	            		search_url 		is not null
	            then 	search_url
	            else null
	        end as combined_url,
	        -- Prioritize ranks in the same order
	        case  
	           -- when organic_rank 		is not null  	then organic_rank
	            when map_rank 			is not null  	then map_rank
	            when organic_rank 		is not null 
	            and  search_rank 		is not null 
	            and  moneyhouse_rank 	is not null  	then LEAST(search_rank, moneyhouse_rank, organic_rank)
	            when moneyhouse_rank 	is not null   	then moneyhouse_rank
	            when search_rank 		is not null    	then search_rank
	            else null  
	        end as combined_rank,
	        -- Compute overall rank giving priority to organic, map, search, and moneyhouse
	        ROW_NUMBER() over (
	            partition by id 
	            order by  
	                case  
	                    when organic_rank 		is not null then organic_rank
	                    when map_rank 			is not null then map_rank
	                    when organic_rank 		is not null 
	                    and  search_rank 		is not null 
	                    and  moneyhouse_rank 	is not null then LEAST(search_rank, moneyhouse_rank, organic_rank)
	                    when moneyhouse_rank 	is not null then moneyhouse_rank
	                    when search_rank 		is not null then search_rank
	                    else null  
	                end asc nulls last,  -- Prioritize the lowest non-null rank
	                organic_rank 						asc nulls last,
	                map_rank 							asc nulls last,
	                LEAST(search_rank, moneyhouse_rank, organic_rank) asc nulls last,
	                moneyhouse_rank 					asc nulls last, 
	                search_rank 						asc nulls last
	        ) as overall_rank
	    from  
	        geo_afo_tmp.az_urls_all_best
	) ranked
where  
    overall_rank = 1
;



select * from geo_afo_tmp.az_urls_best_combined;



-- Function to normalize the string
create or replace function normalize_string(input_text TEXT)
returns TEXT as $$
begin 
    -- Remove all characters except alphanumeric and spaces
    return regexp_replace(input_text, '[^a-zA-Z0-9\s]', '', 'g');
end;
$$ language plpgsql immutable;


-- TEST
select normalize_string(organic_domain) from az_testdata.az_urls_all_uid_best; 






-- add uid_no from afo unternehmen Layer 
drop table if exists az_testdata.az_urls_all_uid_best;
create table az_testdata.az_urls_all_uid_best
as
select 
	az.*
	,ut.name 						as name_afo
	,ut.uid_no 						as uid_no_afo
	,ut.chid 						as ch_id_afo
	,ut.status 						as status_afo
	--,ut.deletiondate 				as deletion_date
from 
	geo_afo_tmp.az_urls_best_combined az
left join 
	geo_zefix.unternehmen ut
on
	--(
	normalize_string(lower(az.az_name)) = normalize_string(lower(ut.name))
	or 
	normalize_string(lower(az.moneyhouse_name)) = normalize_string(lower(ut.name)) 
	--)
	--or 
	--normalize_string(lower(az.az_uid_no)) = normalize_string(lower(ut.uid_no))
;

   




select 
	id,
 	az_uid_no,
 	az_name,
 	uid_no_afo,
 	name_afo
from 
	az_testdata.az_urls_all_uid_best
where 
	uid_no_afo is null -- 269
	and
	az_uid_no is not null -- 739 -- null 332
;


/*
update az_testdata.az_urls_all_uid_best az
set 
	name_afo 	= ut.name 
	,uid_no_afo	= ut.uid_no
	,ch_id_afo	= ut.chid 
	,status_afo	= ut.status
from 
	geo_zefix.unternehmen ut
where  
	az.uid_no_afo is null -- 269
	and
	az.az_uid_no is not null --332
	and 
	trim(normalize_string(lower(az.az_uid_no))) = trim(normalize_string(lower(ut.uid_no)))
;
*/

select * from az_testdata.az_urls_all_uid_best where az;
select id, count(*) from az_testdata.az_urls_all_uid_best group by id having count(*) > 1;
select * from geo_zefix.unternehmen;
select normalize_string(az_uid_no) from az_testdata.az_urls_all_uid_best;
   


-- Duplication in the data because company name has matched with more than on uid_no from AFO layer 
-- There are some cases where az_uid_no matches uid_no from AFO, but the firma_name is different from the one in AZ Data.  
-- By checking the Moneyhouse URL, I found that the page has the correct company name, as in the Unternehmen layer from AFO.  
-- Example:  
--   AZ Name: BHR Partner AG  
--   Moneyhouse URL: https://www.moneyhouse.ch/de/company/bhr-partner-ag-2284882171  
--   az_uid_no: CHE-101.151.576 matches uid_no_afo: CHE101151576, but the name is different.  
--   Name in AFO: SIHRO Immo AG  
-- 




-- fix the duplicates 
drop table if exists tmp_id_az_not_afo;
create temp table tmp_id_az_not_afo
as
select 
	*
from 
	az_testdata.az_urls_all_uid_best 
where 
	id in (
						select 
							id
						from 
							az_testdata.az_urls_all_uid_best 
						group by 
							id 
						having 
							count(*) > 1
	)
	and
	trim(normalize_string(lower(az_uid_no))) = trim(normalize_string(lower(uid_no_afo)))
	and 
	status_afo = 'CANCELLED'
;


delete from az_testdata.az_urls_all_uid_best 
where 
	trim(normalize_string(lower(az_uid_no))) = trim(normalize_string(lower(uid_no_afo))) 
	and 
	status_afo = 'CANCELLED'
	and 
	id in (select id from tmp_id_az_not_afo)
;


delete from az_testdata.az_urls_all_uid_best
where 
	id in ('6188902', '6206321', '6233312')
	and 
	status_afo in ('BEING_CANCELLED', 'CANCELLED')
;
	
-- 6188902 	BEING_CANCELLED	
-- 6206321	CANCELLED BEING_CANCELLED	
-- 6233312	CANCELLED	




select distinct id from az_testdata.az_urls_all_uid_best 
where 
	id in (
			select 
				id
			from 
				az_testdata.az_urls_all_uid_best 
			group by 
				id 
			having 
				count(*) > 1
		)


---------------
-- Final table 
---------------
select * from az_testdata.az_urls_all_uid_best order by id, combined_rank;
		

		
		

-- az_uid_no = uid_no_afo
-- canceled
--1649697
--2382555
--513046
--1080114
--1649697
--2382555
--4329327


-- az_uid_no <> uid_no_afo
/*
4430464
1304896
678343
589655
4254936
249216
638904
714970
243309
416353
715200
2321780
571751
54425
1258414
1264706
270273
243246
2297535
96519
1267648
1196365
526215
501375
1181970
513046
61278
*/


--original_name like '%Buch-Paradies AG%'
   
--  null every where it needs to be tested
/*
288408
6205230
1092359
1225888
351070
1181990
1172500
6216438
1184889
355044
361050
6189196
547916
530284
391791  
*/  
   
   
   
   
   






























































	

	

/*
  
--==================================
-- DRAFT DRAFT DRAFT DRAFT 
--==================================

-- Function to normalize the string
CREATE OR REPLACE FUNCTION normalize_string(input_text TEXT)
RETURNS TEXT AS $$
BEGIN 
    RETURN regexp_replace(
        --regexp_replace(
          --  regexp_replace(
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
                                                        lower(input_text), 
                                                        'ä', 'ae', 'g'
                                                    ), 
                                                    'ö', 'oe', 'g'
                                                ), 
                                                'ü', 'ue', 'g'
                                            ), 
                                            'ß', 'ss', 'g'
                                        ), 
                                        'é|è|ê|ë', 'e', 'g'
                                    ), 
                                    'á|à|â', 'a', 'g'
                                ), 
                                'î|ï', 'i', 'g'
                            ), 
                            'ô', 'o', 'g'
                        ), 
                        'ù|û', 'u', 'g'
                    ), 
                    'ç', 'c', 'g'
                ), 
                '[^a-z0-9\\s]', '', 'g'
            ), 
            '\s+', ' ', 'g'  -- Normalize multiple spaces
        ), 
        '^\s+|\s+$', ' ', 'g'  -- Trim leading/trailing spaces
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;


  
  
--==============
-- Bestes
--==============

DROP TABLE IF EXISTS geo_afo_tmp.az_urls_bests_address;

--CREATE TABLE geo_afo_tmp.az_urls_bests_address AS

WITH source_urls AS (
    -- Find the best URL from EACH source, with name and address match filtering
    SELECT
        id,
        az_uid_no,
        az_name,
        az_address,
        az_ort,
        organic_domain,
        map_domain,
        case 
            when search_url is not null 
                 and lower(az_name) ilike '%' || search_matched_nam || '%'
                 and lower(az_address) ilike '%' || search_matched_adrs || '%' then search_url
            else null 
        end as best_search_url,
        --case 
          --  when search_rank is not null  
            --     and az_name ilike '%' || search_matched_nam || '%'
              --   and az_address ilike '%' || search_matched_adrs || '%' then search_rank
            --else null 
        --end as best_
        search_rank,
        case 
            when moneyhouse_url is not null 
                 and lower(az_name) ilike '%' || moneyhouse_matched_nam || '%' then moneyhouse_url
             --    AND az_address ILIKE '%' || moneyhouse_matched_adrs || '%' THEN moneyhouse_url
            else null  
        end as best_moneyhouse_url,
        --CASE
          --  WHEN moneyhouse_rank IS NOT NULL
            --     AND az_name ILIKE '%' || moneyhouse_matched_nam || '%' THEN moneyhouse_rank
               --  AND az_address ILIKE '%' || moneyhouse_matched_adrs || '%' THEN moneyhouse_rank
            --ELSE NULL
        --END AS best_
        moneyhouse_rank,
        case 
            when organic_url is not null 
                 and lower(az_name) ilike '%' || organic_matched_nam || '%'
                 or lower(az_address) ilike '%' || organic_matched_adrs || '%' 
                 or lower(az_name) ilike '%' || organic_domain || '%' 				then organic_url
            else null 
        end as best_organic_url,
       -- CASE
         --   WHEN organic_rank IS NOT NULL
           --      AND az_name ILIKE '%' || organic_matched_nam || '%'
             --    AND az_address ILIKE '%' || organic_matched_adrs || '%' THEN organic_rank
            --ELSE NULL
        --END AS best_
        organic_rank,
        case 
            when map_url is not null 
                 and lower(az_name) ilike '%' || map_matched_nam || '%'
                 or lower(az_address) ilike '%' || map_matched_adrs || '%' 
                 or lower(az_name) ilike '%' || map_domain || '%'					then map_url
            else null 
        end as best_map_url,
       -- CASE
         --   WHEN map_rank IS NOT NULL
           --      AND az_name ILIKE '%' || map_matched_nam || '%'
             --    AND az_address ILIKE '%' || map_matched_adrs || '%' THEN map_rank
            --ELSE NULL
        --END AS best_
        map_rank,
        name_afo,
        uid_no_afo,
        ch_id_afo,
        status_afo,
        row_number() over(partition by id order by search_rank) 		as search_rn,
        row_number() over(partition by id order by moneyhouse_rank) 	as moneyhouse_rank_rn,
        row_number() over(partition by id order by organic_rank) 		as organic_rn,
        row_number() over(partition by id order by map_rank) 			as map_rn
    from 
        az_testdata.az_urls_all_uid
),
ranked_urls as (
    -- Rank ALL URLs (including the best from each source) to find the overall best
    select 
        id,
        az_uid_no,
        az_name,
        az_address,
        az_ort,
        best_search_url,
        search_rank,
        best_moneyhouse_url,
        moneyhouse_rank,
        best_organic_url,
        organic_rank,
        organic_domain,
        best_map_url,
        map_domain,
        map_rank,
        name_afo,
        uid_no_afo,
        ch_id_afo,
        status_afo,
        -- Combine all URLs into a single column for ranking
        CASE
            when best_search_url 		is not null then best_search_url
            when best_moneyhouse_url 	is not null then best_moneyhouse_url
            when best_organic_url 		is not null then best_organic_url
            when best_map_url 			is not null then best_map_url
            else null 
        end as combined_url,
        -- Combine all ranks for ranking
        case 
            when search_rank 		is not null 	and search_rn = 1 				then search_rank
            when moneyhouse_rank 	is not null 	and moneyhouse_rank_rn = 1		then moneyhouse_rank
            when organic_rank 		is not null 	and organic_rn = 1				then organic_rank
            when map_rank 			is not null 	and map_rn = 1					then map_rank
            else null 
        end as combined_rank,
        ROW_NUMBER() over (partition by id order by 
	        case 
	            when search_rank 		is not null 	and search_rn = 1 				then search_rank
	            when moneyhouse_rank 	is not null 	and moneyhouse_rank_rn = 1		then moneyhouse_rank
	            when organic_rank 		is not null 	and organic_rn = 1				then organic_rank
	            when map_rank 			is not null 	and map_rn = 1					then map_rank
	            else null 
            end asc nulls last, -- Lowest rank first
            search_rank 		asc nulls last,
            moneyhouse_rank 	asc nulls last,
            organic_rank 		asc nulls last,
            map_rank 			asc nulls last 
        ) as overall_rank
    from 
        source_urls
)
-- Final SELECT:  Get the best from each source AND the overall best
select	
    id,
    az_uid_no,
    az_name,
    az_address,
    az_ort,
    search_rank,
    best_search_url,
    moneyhouse_rank,
    best_moneyhouse_url,
    organic_rank,
    best_organic_url,
    organic_domain,
    map_rank,
    best_map_url,
    map_domain,
    combined_url 				as overall_best_url,  -- The overall best URL
    name_afo,
    uid_no_afo,
    ch_id_afo,
    status_afo
from	
    ranked_urls
where
	overall_rank = 1;  -- Only keep the overall best

*/


--===================
-- Update
--===================

/*
DROP TABLE IF EXISTS geo_afo_tmp.az_urls_deduped;

CREATE TABLE geo_afo_tmp.az_urls_deduped AS
SELECT DISTINCT ON (id)
    id,
    az_uid_no,
    az_name,
    az_address,
    az_ort,
    -- Coalesce to prioritize URLs based on rank:
    COALESCE(
        CASE WHEN search_url IS NOT NULL THEN search_url END,
        CASE WHEN moneyhouse_url IS NOT NULL THEN moneyhouse_url END,
        CASE WHEN organic_url IS NOT NULL THEN organic_url END,
        CASE WHEN map_url IS NOT NULL THEN map_url END
    ) AS best_url,
    -- Take lowest of all ranks where applicable
    LEAST(search_rank, moneyhouse_rank, organic_rank, map_rank) AS best_rank,
    -- You may want to also capture the source of the best_url.  This is optional.
    CASE
        WHEN search_url IS NOT NULL AND search_url = COALESCE(search_url, moneyhouse_url, organic_url, map_url) THEN 'search'
        WHEN moneyhouse_url IS NOT NULL AND moneyhouse_url = COALESCE(moneyhouse_url, organic_url, map_url, search_url) THEN 'moneyhouse'
        WHEN organic_url IS NOT NULL AND organic_url = COALESCE(organic_url, map_url,search_url, moneyhouse_url) THEN 'organic'
        WHEN map_url IS NOT NULL AND map_url = COALESCE(map_url, search_url, moneyhouse_url, organic_url) THEN 'map'
        ELSE NULL  -- Should not happen, but handle just in case
    END AS best_url_source,
    name_afo,
    uid_no_afo,
    ch_id_afo,
    status_afo
FROM
    az_testdata.az_urls_all_uid
ORDER BY
    id,  -- Deduplication is based on this
    --Prioritize search, moneyhouse, organic, map by rank.
    CASE
        WHEN search_rank IS NOT NULL THEN search_rank
        WHEN moneyhouse_rank IS NOT NULL THEN moneyhouse_rank
        WHEN organic_rank IS NOT NULL THEN organic_rank
        WHEN map_rank IS NOT NULL THEN map_rank
        ELSE NULL
    END ASC,   -- Rank ASC (lower is better)
    search_rank ASC NULLS LAST,
    moneyhouse_rank ASC NULLS LAST,
    organic_rank ASC NULLS LAST,
    map_rank ASC NULLS LAST;
*/   
   




















































--/////////////////////////////////////////////////////////////////////////////
-- DRAFT
--////////////////////////////////////////////////////////////////////////////
-------------------
-- Table for Peter
-------------------


create temp table tmp_for_match 
as 
select
	bestandid 
	,uid_no 
	,url 
	,"rank"
	,original_name 
	,LOWER(trim(REGEXP_REPLACE(original_name, '[^a-zA-Z\s]', '', 'g'))) as norm_orginal_nam
	,moneyhouse_name 
	,LOWER(trim(REGEXP_REPLACE(moneyhouse_name, '[^a-zA-Z\s]', '', 'g'))) as norm_moneyhouse_nam
	,search_ch_name 
	,matched_name 
	,original_address 
	,search_ch_address 
	,matched_address 
	,original_ort 
	,search_ch_ort 
	,matched_ort 
	,quelle 
	,mh_uid_num 
from
	az_testdata.az_url_data_all_v2
;

select * from tmp_for_match;


create temp table tmp_for_match_uid 
as 
select
	name
	,LOWER(trim(REGEXP_REPLACE(name, '[^a-zA-Z\s]', '', 'g'))) as norm_nam
	,uid_no as uid_no_new
	,chid
	,status 
from
	geo_zefix.unternehmen
;



drop table if exists az_testdata.az_url_data_all_uid;
create table az_testdata.az_url_data_all_uid_v2
as
select 
	t0.*,
	t1.name,
	t1.uid_no_new,
	t1.chid,
	t1.status 
from 
	tmp_for_match t0
left join
	tmp_for_match_uid t1
on
	t0.norm_orginal_nam = t1.norm_nam
	or
	t0.norm_moneyhouse_nam = t1.norm_nam
; 




select * from az_testdata.az_url_data_all_uid_v2 where matched_name is not null;




select
	*
	--LOWER(REGEXP_REPLACE(moneyhouse_name, '[^a-zA-Z\s]', '', 'g')) as normalize_name1, 
	--LOWER(REGEXP_REPLACE(name, '[^a-zA-Z\s]', '', 'g')) as normalize_name2
from 
	az_testdata.az_url_data_all_uid_v1
where 
	uid_no_new is not null
	--matched_name is not null
;




alter table az_testdata.az_url_data_all_v2
add column mh_uid_num text;

update az_testdata.az_url_data_all_uid_v2
set 
	mh_uid_num = uid_no_new
where 
	quelle = 'moneyhouse.ch'
	and
	matched_name is not null
;


select * from az_testdata.az_url_data_all;

--final table for Peter 
drop table if exists az_testdata.az_urls
create temp table tmp_az_urls
as
select
 *
from 
	tmp_az_urls --az_testdata.az_urls
;
drop table if exists az_testdata.az_urls;
create table az_testdata.az_urls
as
select 
	bestandid 
	,url 
	,"rank"
	,trim(original_name) as original_name
	,trim(moneyhouse_name) as moneyhouse_name
	,trim(search_ch_name) as search_ch_name
	,trim(matched_name) as matched_name
	,unternehmen_name
	,original_uid
	,uid_unternehmen
	--,mh_uid_num as uid_moneyhouse_name
	,trim(original_address) as original_address
	,trim(search_ch_address) as search_ch_address
	,trim(matched_address) as matched_address 
	,trim(original_ort) as original_ort
	,trim(search_ch_ort) as search_ch_ort
	,trim(matched_ort) as matched_ort
	,quelle 
	,chid 
	,status 
from 
	tmp_az_urls
;


select 
	*
from 
	az_testdata.az_urls
where 
	matched_name is null 
	and 
	quelle = 'search.ch'
;

-- search.ch has 385 no match
-- moneyhouse.ch has 851

select 
	url, 
	original_name, 
	search_ch_name,
	original_address,
	search_ch_address,
	matched_name,
	matched_address, 
	search_ch_ort,
	matched_ort, 
	original_ort  
from 
	az_testdata.az_urls 
where 
	lower(search_ch_ort) <> lower(original_ort)
	and 
	quelle = 'search.ch'
	and 
	search_ch_ort like 'was %'
; 



select * from az_testdata.az_urls where quelle = 'moneyhouse.ch';
















/*
 * regexp_replace(
	regexp_replace(
	regexp_replace(
	regexp_replace(
	regexp_replace(
	regexp_replace(
	regexp_replace(
	regexp_replace(
	regexp_replace(url, '^.*/', '', 'g'), -- Extract the last part after the last slash                               
	'\.', ' ', 'g'), 	-- Replace dots with spaces
	'-', ' ', 'g' ),	-- Replace hyphens with spaces
	'\+', ' ', 'g'),  	-- Replace plus with spaces                      
	'%', ' ', 'g' ),  	-- Replace percent with spaces                        
	'&', ' ', 'g' ),  	-- Replace ampersand with spaces                  
	'=', ' ', 'g' ), 	-- Replace equal with spaces                 
	'\?', ' ', 'g'),	-- Replace question mark with spaces             
	'\d+$', '' )		-- Remove trailing digits             
	where 
    quelle = 'moneyhouse.ch'
*/
