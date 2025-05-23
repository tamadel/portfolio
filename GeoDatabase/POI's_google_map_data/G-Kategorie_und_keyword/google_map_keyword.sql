--======================================================================
-- Erste Entwurf: tabelle mit die häfigste Kategory als Keyword benutzen
--======================================================================
drop table if exists google_maps_dev_test.entwurf_googl_map_keyword;
create table google_maps_dev_test.entwurf_googl_map_keyword
as
select 
    poi_typ,
    category_en,
    category_de,
    n_count
from (
    select 
        case 
            when keyword like '% %' then -- Check if there's at least one space
                regexp_replace(keyword, '\s+\S+$', '')
            else 
                keyword -- If no space is present, return the original text
        end as poi_typ,
        category_en,
        category_de,
        n_count,
        ROW_NUMBER() over (partition by category_de order by n_count desc) as rn
    from 
        google_maps_dev.google_category_count 
    where 
        n_count <= 218 AND n_count > 5
) as g0
where 
    rn = 1
    and 
    n_count = (
		        select
		        	max(n_count)
		        from
		        	google_maps_dev.google_category_count as g1
		        where
		        	g1.category_de = g0.category_de
		            and  
		            g1.n_count <= 218 and g0.n_count > 5
    			)
order by 
    poi_typ,
    n_count desc;



select 
	*
from 
	google_maps_dev.google_category_count;


select 
	category_de,
	count(*)
from 
	google_maps_dev_test.entwurf_googl_map_keyword
group by
	category_de 
having 
	count(*)>1
;


--query for the script
select distinct
	poi_typ,
	category_de,
	n_count
from 
	google_maps_dev_test.entwurf_googl_map_keyword
where 
	poi_typ = 'Autohändler'
order by
	poi_typ,
	n_count desc 
;
    
--=========================================
-- Finale Tabelle für Automatisierung
--========================================    
-- adding poi_typ_id
drop table if exists tmp_google_map_poy_id;
create temp table tmp_google_map_poy_id
as
select distinct
	t1.poi_typ_id,
	t0.*
from 
	google_maps_dev_test.entwurf_googl_map_keyword t0
left join
	geo_afo_prod.mv_lay_poi_aktuell t1
on 
	t0.poi_typ = t1.poi_typ
order by 
	poi_typ,
	n_count desc
;
-- create table for the automation script 
drop table if exists geo_afo_prod.meta_poi_google_maps_category;
create table geo_afo_prod.meta_poi_google_maps_category
as
select
	category_de as google_map_kw,
	poi_typ_id,
	poi_typ
from
	tmp_google_map_poy_id
;

alter table
	geo_afo_prod.meta_poi_google_maps_category
	rename column last_ts_run to  last_run_ts
;

alter table
	geo_afo_prod.meta_poi_google_maps_category
	rename column "next_run_Date" to next_run_date
;

-- add the needed column 
alter table 
	geo_afo_prod.meta_poi_google_maps_category 
	--add column periodicity text,	
 	--add column last_ts_run Timestamp,
 	alter column next_run_date type Timestamp
 	using next_run_date::Date
;
--add some timestamp to test the "next_ts_run"
update
	geo_afo_prod.meta_poi_google_maps_category
set
	last_ts_run = CURRENT_TIMESTAMP;

--add some data in periodicity for test
update geo_afo_prod.meta_poi_google_maps_category
set periodicity = case 
    when 
    	google_map_kw = 'Pflegeheim' then 'MONTHLY'
    when 
    	google_map_kw = 'Mechaniker' then 'WEEKLY'
    when 
    	google_map_kw = 'Bar' then 'DAILY'
    when 
    	google_map_kw = 'Restaurant' then 'YEARLY'
    else 
    	periodicity -- Keeps the existing value if no condition matches
end;
	
-- update the next_ts_run column based on the periodicity for test 
-- main one will be calculated in python
update
	geo_afo_prod.meta_poi_google_maps_category
set 
	next_run_date = case  
	    when  
	    	periodicity = 'DAILY' then last_run_ts + INTERVAL '1 day'
	    when  
	    	periodicity = 'WEEKLY' then last_run_ts + INTERVAL '1 week'
	    when  
	    	periodicity = 'MONTHLY' then last_run_ts + INTERVAL '1 month'
	    when  
	    	periodicity = 'YEARLY' then last_run_ts + INTERVAL '1 year'
	    else  
	    	null
	end
;

-- update periodicity to test Python script
update
	geo_afo_prod.meta_poi_google_maps_category
set 
	periodicity = case 
		when
			google_map_kw in ('Autohändler','Autowerkstatt', 'Occasionshändler', 'Spenglerei', 'Pneugeschäft')
		then 
			 'YEARLY'
		else 
			null
	end,
	last_run_ts = null
;	
	


-- test the results 
select 
	*
from 
	geo_afo_prod.meta_poi_google_maps_category_v1
where
	google_map_kw in ('Autohändler','Autowerkstatt', 'Occasionshändler', 'Spenglerei', 'Pneugeschäft')
;




-- test duplication
select
	google_map_kw,
	count(*)
from 
	geo_afo_prod.meta_poi_google_maps_category
group by
	google_map_kw
having 
	count(*) > 1;

--===================================================================
-- Neu nach die Tabelle von Peter: Finale Tabelle für Automatisierung
-- Neue Strukture von Poi Kategorie und Poi_typ
--===================================================================
create table geo_afo_prod.meta_poi_google_maps_category
as
select distinct
	hauptkategorie_neu,
	kategorie_neu,
	poi_typ_neu
from 
	google_maps_dev.google_map_category_hierarchy
order by
	hauptkategorie_neu 
;

alter table 
	geo_afo_prod.meta_poi_google_maps_category 
	add column periodicity text,	
 	add column last_run_ts Timestamp,
 	add column next_run_date Date
;

--add some timestamp to test the "next_ts_run"
update
	geo_afo_prod.meta_poi_google_maps_category
set
	last_run_ts = CURRENT_TIMESTAMP;


-- update the next_ts_run column based on the periodicity for test 
-- main one will be calculated in python
update
	geo_afo_prod.meta_poi_google_maps_category
set 
	next_run_date = case  
	    when  
	    	periodicity = 'DAILY' then last_run_ts + INTERVAL '1 day'
	    when  
	    	periodicity = 'WEEKLY' then last_run_ts + INTERVAL '1 week'
	    when  
	    	periodicity = 'MONTHLY' then last_run_ts + INTERVAL '1 month'
	    when  
	    	periodicity = 'YEARLY' then last_run_ts + INTERVAL '1 year'
	    else  
	    	null
	end
;

select distinct
    poi_typ_neu
    ,hauptkategorie_neu
    ,kategorie_neu
from 
	geo_afo_prod.meta_poi_google_maps_category
where 
	hauptkategorie_neu = 'Mobilität'
	and 
	kategorie_neu in ('Fahrzeughandel und -werkstatt', 'Fahrzeugvermietung')
;




select 
	*
from 
	geo_afo_prod.meta_poi_google_maps_category
where 
	hauptkategorie_neu = 'Mobilität'
	and 
	kategorie_neu in ('Fahrzeughandel und -werkstatt', 'Fahrzeugvermietung')
;




select 
	*
from 
	geo_afo_prod.meta_poi_google_maps_category
where 
	hauptkategorie_neu like '%Gastro%'
;



select 
	*
from 
	geo_afo_prod.mv_lay_plz6_aktuell 
where 
	gemeinde = 'Cham'
;












--/////////////////////////////// Experiment //////////////////////////////////////////////

/*
select 
	category_de,
	count(*)
from(
		select 
		    case  
		        when keyword like '% %' then -- Check if there's at least one space
		            regexp_replace(keyword, '\s+\S+$', '')
		        else 
		            keyword -- If no space is present, return the original text
		    end as poi_typ,
		    category_en,
		    category_de,
		    n_count
		from  
		    google_maps_dev.google_category_count as g0
		where  
		    n_count <= 218 and n_count > 5
		    and n_count = (
		        select
		        	max(n_count)
		        from
		        	google_maps_dev.google_category_count as g1
		        where
		        	g1.category_de = g0.category_de
		            and  
		            g1.n_count <= 218 and g0.n_count > 5
		    )
	) t
group by 
	category_de
having 
	count(*) > 1
;*/


/*
 * select
	case 
        when keyword like '% %' then -- Check if there's at least one space
            regexp_replace(keyword, '\s+\S+$', '')
        else 
            keyword -- If no space is present, return the original text
    end as poi_typ,
	category_en,
	category_de,
	n_count
from 
	google_maps_dev.google_category_count 
where 
	n_count < 218 and n_count > 5
order by 
	keyword,
	n_count desc
;

 */



    
    
    
    