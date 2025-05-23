------------------------------------------------
--
--Convenience muss vollumfänglich und vollständig drin sein
--Zuerst müssen wir schauen, ob der Convenience auch dabei ist
--Die Google Daten werden aus dem Food genommen, da "Convenience" nicht explizit gesucht wurde
--
------------------------------------------------
--select * from google_maps_dev_abgleich.google_abgleich_food;
--select * from webgis_layers.v_pois_aldi_update;
--select * from webgis_layers.v_pois_aldi;

--=============================
-- Move to serverless
--=============================
DROP table if exists
	google_maps_dev.food_test_ta;
--google_maps_dev_abgleich.google_abgleich_food
create table
	google_maps_dev.food_test_ta
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
				google_maps_dev_abgleich.food_test_sp 
		$POSTGRES$
	) AS food_test_ta (
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

select * from google_maps_dev.food_test_ta;


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

SELECT 
	* 
FROM 
	tamer_test_google.meta_branding 
WHERE 
	poi_typ_id = 202;

-- 25 Brands vorhanden, als Tests brauchen wir aber die "wichtigsten"

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
-- taar (taar Express)

-- k kiosk wird rausgenommen

-- Fokus auf Migrolino, Coop pronto und avec


--------------------------
-- Coop pronto Recherche
--------------------------

select
	COUNT(*) 
FROM 
	geo_afo_prod.mv_lay_poi_aktuell 
WHERE 
	company = 'Coop pronto'
and 
	poi_typ_id = 1309 ;
-- 328 Standorte gemäss AFO mit poi_typ_id = 202 (Convenience)
-- 260 Standorte gemäss AFO mit poi_typ_id = 1309 (Tankstelle)
-- Gemäss Webseite existieren 329 Filialen in der Schweiz

--select * from geo_afo_prod.mv_lay_poi_aktuell where company = 'Coop pronto' and poi_typ_id = 1309;




SELECT 
	* 
FROM 
	google_maps_dev.food_test_ta
WHERE 
	LOWER(company) LIKE '%coop pronto%'
AND 
	company_group_id <> 48;

-- 327 Treffer

-- 49 mit company "avec station"

-- Die alle Filialen werden mit domain www.coop-pronto.ch betitelt 
-- ausser poi_id = '2187834814072412753' hat www.coop.ch domain und url endet mit 'pronto-menziken.html' aber führt zu coop pronto



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

-- Offensichtlich hat Google nicht alle coop pronto Standorte mit Food erfasst, eventuell haben einige kein Food?

-- 208 mit gasstation 
-- 49  mit avec station-service

------------------------------------------------
-- Testen mit der URL oder Domain
------------------------------------------------


SELECT 
	* 
FROM 
	google_maps_dev.food_test_ta
WHERE 
	DOMAIN IN ('www.coop-pronto.ch','coop-pronto.ch')
	AND 
	quelle = 'GOOGLE';

-- Mit der URL können 322 Standorte gefunden werden.
-- Die Frage ist, ob man die weiteren noch finden kann

SELECT 
	* 
FROM 
	google_maps_dev.food_test_ta
WHERE 
	DOMAIN NOT IN ('www.coop-pronto.ch','coop-pronto.ch') 
	AND 
	quelle = 'GOOGLE'
	AND 
	LOWER(company) LIKE '%coop pronto%';

-- Nur noch 1 weitere gefunden. Fazit: Nicht vollständig mit der Food Suche

select 
	*
from  
	google_maps_dev.food_test_ta
where  
	domain is null
	and  
	quelle = 'GOOGLE'
	and  
	LOWER(company) like '%coop pronto%';

-- noch 5 weitere gefunden. sie haben kein domain noch url 

--------------------------------------------------------------------------------------
-- Weitere Suche wird nun in anderen Kategorien gesucht,
-- Es wird untersucht ob wir gewisse Brands über die URL finden können
--------------------------------------------------------------------------------------

-- Kann ich statt BRAND via Kategorie auch einfach die URL und Kanton suchen?

-- Nein, Teilweise sind Brands so nicht zu finden. Business API ist ein Versuch wert.



select 
	*
from 
	google_maps_dev.google_abgleich_food
where 
	lower(title) like '%coop pronto%'
;


-- compare to web scraping 
drop table if exists tmp_coop_pronto;
create temp table tmp_coop_pronto
as
select 
	t1.poi_id
	,t1.company
	,trim(regexp_replace(t1.company, 'Coop Pronto( Shop| avec station-service| Shop mit Tankstelle| con stazione di servizio)?', '', 'g')) as g_name
	,t0.name
	,t1.adresse
	,t0.address
	,t1.plz4
	,t1.ort
	,t1.url
	,t1.domain
	,t1.geo_point_lv95
from 
	google_maps_dev.food_test_ta t1
left join 
	web_scraping.coop_pronto_ch t0
on
	t0.plz = t1.plz4
	and 
	t0.ort = t1.ort
	and 
	lower(trim(REGEXP_REPLACE(t1.company, 'Coop Pronto( Shop| avec station-service| Shop mit Tankstelle| con stazione di servizio)?', '', 'g'))) = lower(t0.name) 
WHERE 
	LOWER(t1.company) LIKE '%coop_pronto%'
AND 
	t1.company_group_id <> 48;



select * from tmp_coop_pronto;


select 
	*
from 
	web_scraping.coop_pronto_ch
where 
	name not in (
					select 
						"name" 
					from
						tmp_coop_pronto
					where 
					 name is not null
	)
;	




















