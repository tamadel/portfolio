-----------------------------------------------
-- Project: Die Post - Postomat
-- Datum: 20.02.2025
-- Update: 
-----------------------------------------------
-- (II) Postomat
-- official website 	>> 693
-- google 				>> 723
select 
	*
from 
	geo_afo_tmp.tmp_finanzdienstleistung_test_ta
where 
	(
		(
			lower(company) like 'post%'
			and   
			lower(company) like '%postomat%'
		)
		or  
		(
			lower(url) like '%places.post.ch%'
			and  
			lower(url) like '%004pstmat%'
		)
	)
	and   
	company_group_id <> 687
;


--------------------
-- AFO Data
--------------------
-- 0
select 
	*
from 
	geo_afo_tmp.tmp_finanzdienstleistung_test_ta
where 
	(
		(
			lower(company) like 'post%'
			and   
			lower(company) like '%postomat%'
		)
		or  
		(
			lower(url) like '%places.post.ch%'
			and  
			lower(url) like '%004pstmat%'
		)
	)
	and   
	quelle = 'AFO' --'GOOGLE' -- 
	--and
	--dubletten_nr is not null 
;

-- AFO matches 		>> 0
-- Google matches 	>> 7




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
								lower(company) like '%postomat%'
							)
							or  
							(
								lower(url) like '%places.post.ch%'
								and  
								lower(url) like '%004pstmat%'
							)
						)
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
								lower(company) like '%postomat%'
							)
							or  
							(
								lower(url) like '%places.post.ch%'
								and  
								lower(url) like '%004pstmat%'
							)
						)
					AND
					dubletten_nr IS NULL
			) AS un_table
ORDER BY 
	dubletten_nr
	,plz4
	,adresse
	,quelle
;












