------------------------------------------------
--
--Convenience muss vollumfänglich und vollständig drin sein
--Zuerst müssen wir schauen, ob der Convenience auch dabei ist
--Die Google Daten werden aus dem Food genommen, da "Convenience" nicht explizit gesucht wurde
--
------------------------------------------------

-------------------------------
-- Gematchte Food Tabelle als Basis
-------------------------------

SELECT 
	* 
FROM 
	google_maps_dev_abgleich.food_test_sp
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
-- Spar (Spar Express)

-- k kiosk wird rausgenommen

-- Fokus auf Migrolino, Coop pronto und avec

--------------------------
--------------------------
--------------------------
-- AVEC Recherche
--------------------------
--------------------------
--------------------------

SELECT 
	COUNT(*) 
FROM 
	geo_afo_prod.mv_lay_poi_aktuell 
WHERE 
	company = 'Avec';

-- 293 Standorte gemäss AFO

-- Gemäss Webseite existieren 346 Filialen in der Schweiz


SELECT 
	* 
FROM 
	google_maps_dev_abgleich.food_test_sp
WHERE 
	LOWER(company) LIKE '%avec%'
AND 
	company_group_id <> 83;

-- 629 Treffer

-- Viele jedoch mit company "Coop Pronto"

-- Die meisten Filialen werden mit domain avec.ch oder www.avec.ch betitelt

-- Nächster Schritt, rausfiltern von Coop Pronto Filialen

SELECT 
	* 
FROM 
	google_maps_dev_abgleich.food_test_sp
WHERE 
	LOWER(company) LIKE '%avec%'
AND 
	company_group_id <> 83
AND 
	LOWER(company) NOT LIKE '%coop%'
AND 
	LOWER(company) NOT LIKE '%migros%'
AND
	LOWER(company) NOT LIKE '%migrol%'
AND 
	quelle = 'GOOGLE';

-- 580 Treffer

-- 293 AFO 

-- 288 Google

-- Offensichtlich hat Google nicht alle Avec Standorte mit Food erfasst, eventuell haben einige kein Food?

-- Avec hat keine insite Möglichkeit

------------------------------------------------
-- Testen mit der URL oder Domain
------------------------------------------------


SELECT 
	*
FROM 
	google_maps_dev_abgleich.food_test_sp
WHERE 
	domain IN ('www.avec.ch','avec.ch')
	AND 
	quelle = 'GOOGLE';

-- Mit der URL können 322 Standorte gefunden werden.
-- Die Frage ist, ob man die weiteren noch finden kann

SELECT 
	* 
FROM 
	google_maps_dev_abgleich.food_test_sp
WHERE 
	domain NOT IN ('www.avec.ch','avec.ch')
	AND 
	quelle = 'GOOGLE'
	AND 
	LOWER(company) LIKE '%avec%';

-- Nur noch 3 bis 4 weitere gefunden. Fazit: Nicht vollständig mit der Food Suche

-- 

--------------------------------------------------------------------------------------
-- Weitere Suche wird nun in anderen Kategorien gesucht,
-- Es wird untersucht ob wir gewisse Brands über die URL finden können
--------------------------------------------------------------------------------------

-- Kann ich statt BRAND via Kategorie auch einfach die URL und Kanton suchen?

-- Nein, Teilweise sind Brands so nicht zu finden. Business API ist ein Versuch wert.


--------------------------------------
-- Suche bei der Tankstelle
--------------------------------------

-- Da die Suche nicht komplett war, müssen wir bei Avec noch die Tankstellen hinzufügen,
-- welche in der Kategorie "Mobility" zu finden sind.

-- Offensichtlich gab es keine Tankstellen
	
-- Nächster Schritt: Via URL alle Business suchen

---------------------------------------
-- Business API Nutzung um zu schauen, ob mehr Infos rauskommen
---------------------------------------

SELECT 
	* 
FROM 	
	tamer_test_google.avec_business_api_results;

CREATE TABLE 
	tamer_test_google.avec_business_api_results
( 
	title VARCHAR(1000),
	category VARCHAR(1000),
	category_ids VARCHAR(1000),
	cid VARCHAR(255),
	address_full VARCHAR(300),
	address VARCHAR(200),
	city VARCHAR(100),
	zip VARCHAR(20),
	url VARCHAR(1000),
	domain VARCHAR(300),
	total_photos VARCHAR(20),
	latitude FLOAT8,
	longitude FLOAT8
);

TRUNCATE 
	tamer_test_google.avec_business_api_results;

CREATE TABLE 
	tamer_test_google.avec_business_api_results_title
AS
SELECT 
	* 
FROM 
	tamer_test_google.avec_business_api_results;


SELECT 
	* 
FROM 
	tamer_test_google.avec_business_api_results_url;

-- Hier wurden nun aus dem Business API alle rausgenommen, die die URL haben. Vergleich, wie viele davon bei uns fehlen


-- Als erstes machen wir eine Liste der vermeintlichen Avecs, welche wir aus dem Google Maps crawl haben

CREATE TABLE 
	tamer_test_google.avec_cids
AS
SELECT 
	poi_id
FROM 
	google_maps_dev_abgleich.food_test_sp
WHERE 
	DOMAIN IN ('www.avec.ch','avec.ch')
	AND 
	quelle = 'GOOGLE';
	

-- Die mit anderer oder ohne URL

INSERT INTO 
	tamer_test_google.avec_cids
(poi_id)
VALUES
(418234459372179804),
(15059307106311092251),
(13821836044243330778),
(544212974814241757),
(12047838312452430486),
(15041134501479660050),
(9741547439711035007),
(13392976475177283832),
(6927653743317585725),
(13007527716227427286),
(5269164419647412531);


-- Kontrolle CIDs

SELECT 
	* 
FROM 
	tamer_test_google.avec_cids;


SELECT 
	* 
FROM 
	tamer_test_google.avec_business_api_results_url
WHERE 
	cid NOT IN (
		SELECT poi_id FROM tamer_test_google.avec_cids
	)
AND 
	title LIKE 'avec%';
	

INSERT INTO 
	tamer_test_google.avec_cids
SELECT 
	cid 
FROM 
	tamer_test_google.avec_business_api_results_url
WHERE 
	cid NOT IN (
		SELECT poi_id FROM tamer_test_google.avec_cids
	)
AND 
	title LIKE 'avec%';
	

-- Es fehlen noch 3, die nicht drin waren, Teilweise unter Tankstelle oder General Store zu finden.
-- Beginnen mit "Avec" aber können ggf. auch anders heissen.

-- 

SELECT 
	* 
FROM 
	tamer_test_google.avec_business_api_results
WHERE 
	cid NOT IN (
		SELECT poi_id FROM tamer_test_google.avec_cids
	);


INSERT INTO 
	tamer_test_google.avec_cids
(poi_id)
VALUES
(288528843533675126);




SELECT * FROM tamer_test_google.avec_business_api_results WHERE cid = '288528843533675126';





--------------------------
--------------------------
--------------------------
-- Migrolino Recherche
--------------------------
--------------------------
--------------------------


SELECT 
	*
FROM 
	geo_afo_prod.mv_lay_poi_aktuell 
WHERE 
	company = 'Migrolino';

-- 330 Standorte gemäss AFO, darunter sind Migrolino und Mio zu finden
-- Jede URL hat die exakte URL

-- Gemäss Webseite existieren 321 Migrolino und 52 Mio Filialen = TOTAL 373 Filialen


SELECT 
	* 
FROM 
	google_maps_dev_abgleich.food_test_sp
WHERE 
	LOWER(company) LIKE '%migrolino%'
AND 
	company_group_id <> 83
AND 
	quelle = 'GOOGLE';

-- 378 wurden bei Google im Food gefunden

-- 314 davon haben migrolino URL (ohne genauen Standort)
-- 51 haben mio-shops.ch als URL (ohne genauen Standort)
-- 10 haben eine Picadilly URL der Tankstelle
-- 2 keine URL, jedoch sicher Migrolino

--> Sind doppelte vorhanden, z.B. bei den Piccadilly?

--> Nach PLZ ordnen

SELECT 
	* 
FROM 
	google_maps_dev_abgleich.food_test_sp
WHERE 
	LOWER(company) LIKE '%migrolino%'
AND 
	(url LIKE '%migrolino%'
	OR 
	url LIKE '%mio-shops%'
	OR 
	url IS NULL)
AND
	company_group_id <> 83
AND 
	quelle = 'GOOGLE'
;

-- Jeder Piccadilly ist separat drinnen als Tankstelle, diese können entfernt werden.

-- Piccadillys raus -- 368 Treffer

-- Testen was Business dabei rausbekommt

DROP TABLE IF EXISTS 
	tamer_test_google.migrolino_business_api_results;

CREATE TABLE 
	tamer_test_google.migrolino_business_api_results
( 
	title VARCHAR(1000),
	category VARCHAR(1000),
	category_ids VARCHAR(1000),
	cid VARCHAR(255),
	address_full VARCHAR(300),
	address VARCHAR(200),
	city VARCHAR(100),
	zip VARCHAR(20),
	url VARCHAR(1000),
	domain VARCHAR(300),
	total_photos VARCHAR(20),
	latitude FLOAT8,
	longitude FLOAT8,
	search_term VARCHAR(200),
	search_mode VARCHAR(10)
);



----------------------------------------
-- Business Resultate
----------------------------------------

-- Migrolino URL Search

SELECT * FROM tamer_test_google.migrolino_business_api_results WHERE search_term = '%migrolino.ch%';


-- 317 Resultate -> Einige fehlen, und einige doppelt wie gas_station

SELECT * FROM tamer_test_google.migrolino_business_api_results WHERE search_term = '%migrolino.ch%' AND category NOT IN ('Petrol Station','Gas station');

-- 313 Treffer

-- Jedoch sind einige doppelt drin
-- gas_station, wholesaler ist mit Supermarkt zusammen mit der Migrolino URL -> Weg damit


-- Abgleich mit Food Treffern


SELECT 
	* 
FROM 
	google_maps_dev_abgleich.food_test_sp
WHERE 
	LOWER(url) LIKE '%migrolino.ch%'
AND
	company_group_id <> 83
AND 
	quelle = 'GOOGLE'
;

-- 316 Treffer

-- Abgleichen, welche im Business nicht drin sind abd im Google Maps, via URL, danach das umgekehrte

SELECT 
	* 
FROM 
	google_maps_dev_abgleich.food_test_sp
WHERE 
	LOWER(url) LIKE '%migrolino.ch%'
AND
	company_group_id <> 83
AND 
	quelle = 'GOOGLE'
AND 
	poi_id NOT IN 
	( 
		SELECT 
			cid 
		FROM 
			tamer_test_google.migrolino_business_api_results 
		WHERE 
			search_term = '%migrolino.ch%' 
			AND 
			category NOT IN ('Petrol Station','Gas station')
	)
;
	
-- 8 sind nicht in den Businesses drin
-- Kontrolle, welche dieser 8 überhaupt noch offen.

--> Alle 8 sind gemäss Migrolino Webseite noch offen. Daher ist davon auszugehen, dass nicht mehr alle aus dem Business Search aktiv sind. Weil kummuliert
-- 3 zu viel da sind.

-- Umgekehrter Search, welche Business Results kommen nicht bei Food vor.

SELECT 
	* 
FROM 
	tamer_test_google.migrolino_business_api_results 
WHERE 
	search_term = '%migrolino.ch%' 
	AND 
	category NOT IN ('Petrol Station','Gas station')
AND 
	cid NOT IN 
	( 
		SELECT 
			poi_id 
		FROM 
			google_maps_dev_abgleich.food_test_sp
		WHERE 
			LOWER(url) LIKE '%migrolino.ch%'
		AND
			company_group_id <> 83
		AND 
			quelle = 'GOOGLE'
	)
;

-- 5 Resultate

--> Sind alle geschlossen. Man kann davon ausgehen, dass die Business Search veraltet ist und die Food Search BESSER funktioniert.
--> Die Business Search wird nun für die nächste Zeit ausgesetzt.
-- Damit können wir sagen, dass wir 316, der 321 Treffer der Migrolino finden. Reicht für das Matching und die manuelle Nachbearbeitung.


----------------------
-- mio-shops Search
----------------------

-- Wir konzentrieren uns hier nur auf die Google Maps Resulate, wir erwarten 52 Resultate gemäss mio-shops / Migrolino Webseite

SELECT 
	* 
FROM 
	google_maps_dev_abgleich.food_test_sp
WHERE 
	LOWER(company) LIKE '%migrolino%'
AND 
	(url LIKE '%migrolino%'
	OR 
	url LIKE '%mio-shops%'
	OR 
	url IS NULL)
AND
	company_group_id <> 83
AND 
	quelle = 'GOOGLE'
;


-- Wir haben 52 Punkte mit "mio by migrolino" im Titel, davon haben 51 mio-shops.ch in der URL, einer leer.
-- Man kann davon ausgehen, dass es komplett ist. Wir suchen aber noch andere Titel via URL

SELECT 
	* 
FROM 
	google_maps_dev_abgleich.food_test_sp
WHERE 
	LOWER(url) LIKE '%mio-shops%'
AND
	company_group_id <> 83
AND 
	quelle = 'GOOGLE'
;

-- 51 Treffer, bestätigt, da einer keine URL hatte

--------------------------------------------------------------------
-- In-Site Modul testen
--------------------------------------------------------------------

-- Wir versuchen nun ein Script zu schreiben, welches eine insite Search auslöst.
-- Dabei ist zu beachten, ob wir alle kriegen, oder einige fehlen
-- Diese können dafür genutzt werden, Standorte IDU zu machen
-- Mio und Migrolino sind leider zu fest vernetzt, daher müssen wir beide zusammen suchen

-- 300 bei Google
-- 319 bei Bing
-- 42 bei Yahoo

-- Nächster Test: Local Finder SERP API