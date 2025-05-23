--============================
-- coop pronto
--============================
-- Tabelle von geo_database 
drop table if exists
	google_maps_dev.google_map_food_v1;
create table
	google_maps_dev.google_map_food_v1
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
				google_maps_dev.google_map_food_v1
		$POSTGRES$
	) as google_map_food_v1 (
			cid text 
			,bezeichnung text 
			,category_en_ids text 
			,category_de_ids text 
			,poi_typ text 
			,kategorie text 
			,hauptkategorie text 
			,korr_strasse text 
			,korr_hausnum text 
			,korr_plz4 text 
			,korr_ort text 
			,telefon text 
			,adresse text 
			,google_strasse text 
			,google_strasse_std text 
			,google_hausnum text 
			,google_plz4 text 
			,google_ort text 
			,gwr_strasse varchar(60) 
			,gwr_strasse_std text 
			,gwr_hausnum text 
			,gwr_plz4 int4 
			,gwr_ort varchar(100) 
			,distance float8 
			,plz6 text 
			,plz4 text 
			,ort text 
			,gemeinde text 
			,gmd_nr numeric 
			,url text 
			,"domain" text 
			,anz_fotos int4 
			,google_bewertung text 
			,anz_bewertungen text 
			,relevant numeric 
			,status text 
			,opening_times text 
			,geo_point_google public.geometry 
			,geo_point_gwr public.geometry 
			,category_ids jsonb 
			,category_ids_de jsonb
	)
;


-------------------------------
-- Gematchte Food Tabelle als Basis
-------------------------------

SELECT 
	* 
FROM 
	google_maps_dev.food_test_ta
WHERE 
	poi_typ_id = 202;
	
-- 2629 Convenience POIs mitgenommen
/*
SELECT 
	* 
FROM 
	tamer_test_google.meta_branding 
WHERE 
	poi_typ_id = 202;

-- 25 Brands vorhanden, als Tests brauchen wir aber die "wichtigsten"
*/
SELECT 
	company,
	COUNT(*)
FROM 
	geo_afo_prod.mv_lay_poi_aktuell 
WHERE 
	poi_typ_id = 202
GROUP BY
	company
;

-- Ausgewählt werden die grössten 8, die über 100 Standorte gemäss AFO POIs haben

-- k kiosk
-- Migrolino
-- Coop pronto
-- Avec
-- Agrola
-- Eni Suisse S.A.
-- BP Switerland
-- Spar (Spar Express)

-- k kiosk wird rausgenommen

-- Fokus auf Migrolino, Coop pronto und avec
SELECT 
	COUNT(*) 
FROM 
	geo_afo_prod.mv_lay_poi_aktuell 
WHERE 
	lower(company) = 'coop pronto';

-- 293 Standorte gemäss AFO

-- Gemäss Webseite existieren 346 Filialen in der Schweiz


SELECT 
	* 
FROM 
	google_maps_dev.food_test_ta
WHERE 
	LOWER(company) LIKE '%coop pronto%'
AND 
	company_group_id <> 48;

-- 327 Treffer

-- Viele jedoch mit company "Coop Pronto"

-- Die meisten Filialen werden mit domain avec.ch oder www.avec.ch betitelt

-- Nächster Schritt, rausfiltern von Coop Pronto Filialen

SELECT 
	* 
FROM 
	google_maps_dev.food_test_ta
WHERE 
	LOWER(company) LIKE '%coop pronto%'
AND 
	company_group_id <> 48
--AND 
	--LOWER(company) NOT LIKE '%avec%'
AND 
	LOWER(company) NOT LIKE '%migros%'
AND
	LOWER(company) NOT LIKE '%migrol%'
AND 
	quelle = 'GOOGLE';

-- 655 Treffer

-- 328 AFO 

-- 327 Google

-- 49 mit avec station-service aber domain hat coop pronto
-- 8 mit con stazione di servizio
-- 
------------------------------------------------
-- Testen mit der URL oder Domain
------------------------------------------------


SELECT 
	*
FROM 
	google_maps_dev.food_test_ta
WHERE 
	domain IN ('www.coop-pronto.ch', 'coop-pronto.ch')
	AND 
	quelle = 'GOOGLE';

-- Mit der URL können 322 Standorte gefunden werden.
-- Die Frage ist, ob man die weiteren noch finden kann

SELECT 
	* 
FROM 
	google_maps_dev.food_test_ta
WHERE 
	domain NOT IN ('www.coop-pronto.ch', 'coop-pronto.ch')
	AND 
	quelle = 'GOOGLE'
	AND 
	LOWER(company) LIKE '%coop pronto%';

-- Nur noch eine weitere gefunden mit domain "www.pronto.ch" aber url hat coop pronto . Fazit: Nicht vollständig mit der Food Suche
-- die folgende coop pronto hat kein Domain >> nullS
--2955451984828406323
--364199203250912728
--13143369926846691571
--1772472263607921947
--7263305117586760743



--=============================
-- Standort scraping 
--=============================

select * from web_scraping.coop_pronto;

create table geo_afo_tmp.tmp_coop_pronto_google
as 
select 
	* 
from  
	google_maps_dev.food_test_ta
where  
	LOWER(company) like '%coop pronto%'
and  
	company_group_id <> 48;


-- alter table web_scraping.coop_pronto add column geo_point_lv95 geometry;
--update web_scraping.coop_pronto
--set geo_poly_lv95 = ST_Transform( ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), 2056);



-- find Match
-- 256 match von google 

drop table if exists geo_afo_tmp.tmp_coopt_match_google;

create table geo_afo_tmp.tmp_coopt_match_google
as
select  
    t1.poi_id,
    t1.company 												as google_name,
    trim(regexp_replace(t1.company, 
    '^(coop pronto avec station-service|Coop Pronto Shop mit Tankstelle|Coop Pronto con stazione di servizio|coop pronto shop|coop pronto| -)\s*', 
    '', 
    'i')) 													as norm_company,
    t2."name" 												as coop_name,
    t1.adresse 												as google_adresse, 
    t2.address 												as coop_adresse,
    t1.plz4 												as google_plz4,
    t1.ort 													as google_ort,
    t2.plz 													as coop_plz4,
    t2.ort 													as coop_ort,
    t1.domain 												as google_domain,  
    t1.url 													as google_url,
    t2.url 													as coop_url,
    ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) 		as distance,
    t1.geo_point_lv95 										as geo_google,
    t2.geo_point_lv95 										as geo_coop
from  
    geo_afo_tmp.tmp_coop_pronto_google t1
join  
    web_scraping.coop_pronto t2
ON 
    --t1.geo_point_lv95 && ST_Expand(t2.geo_point_lv95, 20)
    --or
    trim(regexp_replace(t1.company, 
    '^(coop pronto avec station-service|Coop Pronto Shop mit Tankstelle|Coop Pronto con stazione di servizio|coop pronto shop|coop pronto| -)\s*', 
    '', 
    'i')) = t2.name 
    --or
    --t1.adresse ilike t2.address
    or
    t1.url = t2.url
--where  
  --  ST_Distance(t1.geo_point_lv95, t2.geo_point_lv95) <= 30
order by 
	distance desc
;


--18349421782081120331
--7797797645674105174

select * from geo_afo_tmp.tmp_coopt_match_google;
select * from web_scraping.coop_pronto;
select * from geo_afo_tmp.tmp_coop_pronto_google;

-- coop pronto
-- Coop Pronto avec station-service
-- Coop Pronto con statione di servizio
-- Coop Pronto Shop


select
	t1.company,
	trim(regexp_replace(t1.company, 
    '^(coop pronto avec station-service|Coop Pronto Shop mit Tankstelle|Coop Pronto con stazione di servizio|coop pronto shop|coop pronto| -)\s*', 
    '', 
    'i')) as norm_company,
    adresse,
    url
from
	geo_afo_tmp.tmp_coop_pronto_google t1
where
	trim(regexp_replace(t1.company, 
    '^(coop pronto avec station-service|Coop Pronto Shop mit Tankstelle|Coop Pronto con stazione di servizio|coop pronto shop|coop pronto| -)\s*', 
    '', 
    'i')) not in (
    				select 
    					name
    				from
    					web_scraping.coop_pronto
    )
;


select 
	*
from 
	web_scraping.coop_pronto
where 
	name not in (
					select
						trim(regexp_replace(company, 
					    '^(coop pronto avec station-service|Coop Pronto Shop mit Tankstelle|Coop Pronto con stazione di servizio|coop pronto shop|coop pronto| -)\s*', 
					    '', 
					    'i'))
					from
						geo_afo_tmp.tmp_coop_pronto_google
	)
;
-- Zürich Seebach 			>> exists as Coop Pronto
-- Thun am Bahnhof 			>> exists as Coop Pronto
-- Vaduz FL 				>> not exists 
-- Umiken 					>> not exists
-- Lenzburg 				>> not exists
-- Ittigen Talgut Zentrum 	>> not exists
-- Eschen FL  				>> not exists
-- Balzers FL 				>> not exists 
-- Buchs Rheinstrasse		>> not exists


-- cids that exists in google but has no match in coop_web
select 
 	poi_id,
    company 									as google_name,
    adresse 									as google_adresse, 
    plz4 										as google_plz4,
    ort 										as google_ort,
    domain 										as google_domain,
    url 										as google_url,
    geo_point_lv95 								as geo_google
 from
 	geo_afo_tmp.tmp_coop_pronto_google
 where 
 	poi_id not in (
 					select 
 						poi_id
 					from
 						geo_afo_tmp.tmp_coopt_match_google
 	)
;



select 
	poi_id
	,count(*)
from 
	geo_afo_tmp.tmp_coopt_match_google
group by
	poi_id
having 
	count(*) > 1
;




select 
	* 
from 
	web_scraping.coop_pronto
where 
	name like '%Oerlikon Bahnhof%'
;


select 
	*
from 
	geo_afo_tmp.tmp_coop_pronto_google
where 
	company like '%Oerlikon Bahnhof%'
;


--==================================================================
--  BEMERKUNG --
--==================================================================
-- es gibt fälle wo company in google nur "Coop Pronto" sind -- company in ('Coop Pronto Shop' , 'coop pronto', 'Coop Pronto', 'Coop Pronto')
--2360385097928608878 	>> Thun am Bahnhof
--1772472263607921947 	>> Mägenwil
--10697373255198086902 	>> Zürich Seebach
--2187834814072412753   >> Shop mit Tankstelle Menziken,   Hauptstrasse 14
--4472857601117357857	>> Bellinzona Süd , A2 Chiasso-Gotthard/San Bernadino
--8001902384472846296	>> Bellinzona Nord -- weder adresse noch name falsch auf google "Via Galbisio 60, 6503 Bellinzona" aber auf coop ist "A2 Gotthard/San Bernadino-Chiasso, 6500 Bellinzona"
--13143369926846691571  >> Zürich Wehntalerstrasse


-- viele geo_point bei coop_web sind nicht correct Bsp.: poi_id = '12253698720156513457'  coop Zweisimmen



--18319634771074135403 	-- google name "Emmenbrücke - Rothenburgstr." 	coop_name "Emmenbrücke Rothenburgstr."
--12364346295560473721 	-- google name "Emmenbrücke - Seetalstr." 		coop name "Emmenbrücke Seetalstr."
--2955451984828406323  	-- google name "Winterthur Wülflingenstrasse" 	coop name "Winterthur Wülflingestrasse"  
--9506917425328539817  	-- google name "Kriens - Pilatusmarkt" 			coop name "Kriens Pilatusmarkt"
--8928158914438868012	-- google name "Bern Bümpliz"  					coop name "Bern Morgenstrasse" 
--2388074278698565893	-- google name "Interlaken"  					coop name "Interlaken Zentrum" 


--poi_id = '1378180220319118264' und poi_id = '4795875494184771963' hat gematchet mit zwei Coop filiali Biel
-- Biel Bahnhof einmal mit "Biel Bahnhof" und einmal mit "Biel Bahnhofspassage"
-- grund: t1.geo_point_lv95 && ST_Expand(t2.geo_point_lv95, 20)


-- zwei verschiedene coop pront mit der selebe adresse auf google -- google hat falsche adresse 
-- poi_id in( '18349421782081120331', '7797797645674105174')


--13885466858131262121 hat ein match aber falsche adresse bei Google


--coop pronto vs google 
-- Zürich Seebach 			>> exists as Coop Pronto
-- Thun am Bahnhof 			>> exists as Coop Pronto
-- Vaduz FL 				>> not exists 
-- Umiken 					>> not exists
-- Lenzburg 				>> not exists
-- Ittigen Talgut Zentrum 	>> not exists
-- Eschen FL  				>> not exists
-- Balzers FL 				>> not exists 
-- Buchs Rheinstrasse		>> not exists








































--===========================
-- MIGROLINO
--===========================
select * from web_scraping.migrolino;


select 
	*
from 
	web_scraping.migrolino t1
left join
	web_scraping.migrolino_adresse t



alter table web_scraping.migrolino
add column phone text;


update web_scraping.migrolino t1
set 
	address = t2.address
	from 
		web_scraping.migrolino_adresse t2
	where 
		t1.url ilike t2.url
;



update web_scraping.migrolino t1
set
	opening_times = t2.opening_times
	from 
		web_scraping.migrolino_test_corrected_v0 t2
	where 
		t1.url ilike t2.url
		and 
		t1."name" = t2."name" 
;



update web_scraping.migrolino t1
set
	phone = t2.phone
	from 
		web_scraping.migrolino_test_corrected_v1 t2
	where 
		t1.url ilike t2.url
;


alter table web_scraping.migrolino
add column geo_point_lv95 geometry;


update web_scraping.migrolino
set
	geo_point_lv95 = ST_Transform( ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), 2056)
;

