-----------------------------------------------
-- Project: Die Post Finance
-- Datum: 16.02.2025
-- Update: 
-----------------------------------------------
 -- Tabelle auf Serverless migrieren
drop table if exists
	geo_afo_prod.imp_gmd_geo_neu;
create table
	google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot
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
				google_maps_dev_abgleich.poi_abgleich_google_finanzdienstleistung_tot
		$POSTGRES$
	) as imp_gmd_geo_neu (
			poi_id varchar(1000) 
			,hauskey numeric 
			,poi_typ_id numeric 
			,poi_typ text 
			,google_poi_typ varchar(1000) 
			,category_ids varchar(1000) 
			,company_group_id numeric 
			,company_group text 
			,company_id numeric 
			,company text 
			,company_unit text 
			,company_brand text 
			,bezeichnung_lang text 
			,bezeichnung_kurz text 
			,adresse text 
			,adress_lang varchar(1000) 
			,plz4 numeric 
			,plz4_orig numeric 
			,ort text 
			,google_strasse varchar(1000) 
			,google_strasse_std varchar(1000) 
			,google_hausnum varchar(1000) 
			,google_plz4 varchar(1000) 
			,google_ort varchar(1000) 
			,gwr_strasse varchar(1000) 
			,gwr_hausnum varchar(1000) 
			,gwr_plz4 int4 
			,gwr_ort varchar(1000) 
			,plz6 varchar(1000) 
			,gemeinde varchar(1000) 
			,gmd_nr varchar(1000) 
			,url varchar(10000) 
			,"domain" varchar(1000) 
			,geo_point_lv95 public.geometry(point, 2056) 
			,quelle varchar(255) 
			,dubletten_nr varchar(1000)
	)
;



-- create temp table for test 
drop table if exists geo_afo_tmp.tmp_finanzdienstleistung_test_ta;
create table 
	geo_afo_tmp.tmp_finanzdienstleistung_test_ta
as
select 
	*
from 
	google_maps_dev.poi_abgleich_google_finanzdienstleistung_tot
;



---------------------
-- inspect data set 
---------------------


select 
	*
from 
	geo_afo_tmp.tmp_finanzdienstleistung_test_ta
where 
	(
		(
			lower(company) like 'post%'
			and   
			lower(company) like '%finance'
		)
		or  
		(
			lower(url) like '%places.post.ch%'
			and  
			lower(url) like '%001pffil%'
		)
	)
	and 
	lower(company) not like '%postomat%' 
	and 
	lower(company) not like '%geldautomat%' 
	and 
	lower(company) not like '%bancomat%' 
	and  
	lower(company) not like '%atm%' 
	and 
	lower(company) not like '%bankautomat%'
	and   
	company_group_id <> 687
;


------------------------
-- Google Data
-----------------------
--(I) Post Finance
--official website 	>> 90 
--google 			>> 170
select 
	*
from 
	geo_afo_tmp.tmp_finanzdienstleistung_test_ta
where 
	(
		(
			lower(company) like 'post%'
			and   
			lower(company) like '%finance'
		)
		or  
		(
			lower(url) like '%places.post.ch%'
			and  
			lower(url) like '%001pffil%'
		)
	)
	and 
	lower(company) not like '%postomat%' 
	and 
	lower(company) not like '%geldautomat%' 
	and 
	lower(company) not like '%bancomat%' 
	and  
	lower(company) not like '%atm%' 
	and 
	lower(company) not like '%bankautomat%'
	and   
	company_group_id <> 687
;


--------------------
-- AFO Data
--------------------
-- 90
select 
	*
from 
	geo_afo_tmp.tmp_finanzdienstleistung_test_ta
where 
	(
		(
			lower(company) like 'post%'
			and   
			lower(company) like '%finance'
		)
		or  
		(
			lower(url) like '%places.post.ch%'
			and  
			lower(url) like '%001pffil%'
		)
	)
	and 
	lower(company) not like '%postomat%' 
	and 
	lower(company) not like '%geldautomat%' 
	and 
	lower(company) not like '%bancomat%' 
	and  
	lower(company) not like '%atm%' 
	and 
	lower(company) not like '%bankautomat%'
	--and   
	--quelle = 'GOOGLE' --'AFO' 
	and
	dubletten_nr is not null 
;

-- AFO matches 		>> 78
-- Google matches 	>> 71




----------------
-- Cross-Query
----------------
SELECT 
	dubletten_nr::VARCHAR(100),
	poi_id::VARCHAR(100),
	quelle,
	company,
	bezeichnung_lang,
	adresse,
	adress_lang,
	plz4,
	ort
FROM 
	(
		SELECT 
			* 
		FROM 
			geo_afo_tmp.tmp_finanzdienstleistung_test_ta
		WHERE 
			dubletten_nr IN 
			( 
				SELECT 
					dubletten_nr
				FROM 
					geo_afo_tmp.tmp_finanzdienstleistung_test_ta
				WHERE 
					(
						(
							lower(company) like 'post%'
							and   
							lower(company) like '%finance'
						)
						or  
						(
							lower(url) like '%places.post.ch%'
							and  
							lower(url) like '%001pffil%'
						)
					)
					and 
					lower(company) not like '%postomat%' 
					and 
					lower(company) not like '%geldautomat%' 
					and 
					lower(company) not like '%bancomat%' 
					and  
					lower(company) not like '%atm%' 
					and 
					lower(company) not like '%bankautomat%'
					AND
					dubletten_nr IS NOT NULL
			)
		UNION 
		SELECT 
			* 
		FROM 
			geo_afo_tmp.tmp_finanzdienstleistung_test_ta
		WHERE 
					(
						(
							lower(company) like 'post%'
							and   
							lower(company) like '%finance'
						)
						or  
						(
							lower(url) like '%places.post.ch%'
							and  
							lower(url) like '%001pffil%'
						)
					)
					and 
					lower(company) not like '%postomat%' 
					and 
					lower(company) not like '%geldautomat%' 
					and 
					lower(company) not like '%bancomat%' 
					and  
					lower(company) not like '%atm%' 
					and 
					lower(company) not like '%bankautomat%'
					AND
					dubletten_nr IS NULL
			) AS un_table
ORDER BY 
	dubletten_nr
	,plz4
	,adresse
	,quelle
;



