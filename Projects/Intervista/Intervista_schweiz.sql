


---------------------------------------------------------------------------------------------------------------------
--
-- SCHWEIZWEITE AUSWAHL
--
---------------------------------------------------------------------------------------------------------------------


-------------------------------------------
-- Schienennetz / Autobahnen importieren importieren --> lv95
-------------------------------------------

DROP TABLE IF EXISTS 
	intervista.bahnschienen;

CREATE TABLE 
	intervista.bahnschienen
AS
SELECT
	id,
	ST_SetSRID(geom,2056) AS geom,
	"Name" AS name,
	"TUAbkuerzung" AS provider
FROM 
	public."schienennetz_2056_de — Schienennetz_LV95_V1_3.Schienennetz.Ne";

DROP TABLE IF EXISTS 
	intervista.autobahnen;

CREATE TABLE 
	intervista.autobahnen
AS 
SELECT 
	* 
FROM 
	geo_afo_prod.mv_lay_str_freq_aktuell 
WHERE 
	str_type IN (1,5);


-- Hier analysieren, wieviel Buffer notwendig sein sollte
-- Je 10 meter


SELECT 
	ST_BUFFER(geom,15)
FROM 
	intervista.bahnschienen;
	
SELECT 
	ST_BUFFER(geo_line_lv95,15)
FROM 
	intervista.autobahnen;
	





--------------------------------------------------------------------------
--
-- KONTINGENT 85'000 Punkte
--
--------------------------------------------------------------------------

DROP TABLE IF EXISTS 
	intervista.pois;

CREATE TABLE 
	intervista.pois
AS
SELECT 
	*
FROM 
	geo_afo_prod.mv_lay_poi_aktuell 
WHERE 
	poi_typ_id IN 
	(
		1301, --Bhf
		1304, -- Tram
		504, --EKZ
		1206, -- Tertiäre Stufe Schulen
		1207, -- Zweitbildung nicht-tertiäre Stufe
		1205, -- Sekundarstufe 2
		1203, -- Primar
		1204, -- Sekundarstufe 1
		402, -- Apotheke
		403, -- Drogerie
		406, -- Spitäler
		202, -- Convenience
		207, -- Food
		303, -- Eishockey
		310 -- Sportstadion
	);


-- 23'133 Punkte

-- Nachfüllen mit ca der Hälfte wichtiger POIs

--------
-- BUS
--------

SELECT 
	COUNT(*) 
FROM 
	geo_afo_prod.mv_lay_poi_aktuell 
WHERE 
	poi_typ_id = 1302; --Bus
	
-- 22'721

-------------
-- NON FOOD
-------------

SELECT 
	COUNT(*) 
FROM 
	geo_afo_prod.mv_lay_poi_aktuell 
WHERE 
	poi_typ_id BETWEEN 500 AND 525;
	
-- 8'088

---------------------------
-- RESTAURANTS + FAST FOOD
---------------------------

SELECT 
	COUNT(*) 
FROM 
	geo_afo_prod.mv_lay_poi_aktuell 
WHERE 
	poi_typ_id IN (1004,1006);

-- 15'900

-----------------------
-- Strassenabschnitte
-----------------------

SELECT 
	COUNT(*) 
FROM 
	geo_afo_prod.mv_lay_str_freq_aktuell;


-----------------------------------------------
-----------------------------------------------
-- Bushaltestellen sortieren
-----------------------------------------------
-----------------------------------------------

DROP TABLE IF EXISTS 
	intervista.bushaltestellen;

CREATE TABLE 
	intervista.bushaltestellen 
AS
SELECT 
	b.poi_id
	,b.geo_point_lv95
	, ST_Buffer(b.geo_point_lv95, 100) AS buffered_geom
FROM 
	geo_afo_prod.mv_lay_poi_aktuell AS b
where
	b.poi_typ_id  = 1302 --Bus;



-- Create a temporary table to hold the intersection between buffered bus stops and poi

CREATE INDEX 
	bus_geom_index 
ON 
	intervista.bushaltestellen 
USING GIST 
	(buffered_geom);

DROP TABLE IF EXISTS
	intervista.intersected_pois_bus;

CREATE TABLE 
	intervista.intersected_pois_bus 
AS
SELECT 
	b.poi_id AS bus_stop_id
	,b.geo_point_lv95 AS bus_geom
	,b.buffered_geom
	,p.*
FROM 
	intervista.bushaltestellen AS b 
JOIN 
	geo_afo_prod.mv_lay_poi_aktuell AS p 
ON 
	ST_Intersects(b.buffered_geom, p.geo_point_lv95)
where
	P.poi_typ_id <> 1302;

-- tamer:

-- Kontrolle wieviel dieser Bus Stops überhaupt eine Überschneidung aufweisen

SELECT 
	COUNT(DISTINCT bus_stop_id)
FROM 
	intervista.intersected_pois_bus;

-- Hier hast du nur die Busse, die auch Überschneidungen haben, um das ganze vollständig zu haben, solltest du eine Tabelle
-- führen, die alle Busstops beinhaltet, so kannst du kontrollieren, welche Busse z.B, 0 Überschneidungen haben und
-- schauen, ob dies plausibel ist, oder nicht

ALTER TABLE 
	intervista.bushaltestellen 
ADD COLUMN 
	anz_pois INT;

UPDATE 
	intervista.bushaltestellen t0
SET 
	anz_pois = t1.poi_count
FROM
	( 
		SELECT 
			bus_stop_id,
			COUNT(*) AS poi_count
		FROM 
			intervista.intersected_pois_bus 
		GROUP BY 
			bus_stop_id
	) t1
WHERE
	t0.poi_id = t1.bus_stop_id;

-- Nicht alle haben eine Anzahl POIs rundherum

UPDATE 
	intervista.bushaltestellen
SET 
	anz_pois = 0 
WHERE 
	anz_pois IS NULL;
	
------------------------------------------
-- Anzahl Bushaltestellen rausfischen
-------------------------------------------

SELECT 
	COUNT(*) 
FROM 
	intervista.bushaltestellen 
WHERE 
	anz_pois > 0;
	


-----------------------------------------------
-----------------------------------------------
-- Strassenabschnitte sortieren
-----------------------------------------------
-----------------------------------------------

DROP TABLE IF EXISTS 
	intervista.strassenabschnitte;

CREATE TABLE 
	intervista.strassenabschnitte 
AS 
SELECT 
	t0.*,
	ST_Buffer(t0.geo_line_lv95, 30) AS buffered_geom
FROM 
	geo_afo_prod.mv_lay_str_freq_aktuell t0
;

CREATE INDEX 
	street_geom_index 
ON 
	intervista.strassenabschnitte 
USING GIST 
	(buffered_geom);

-- Create a temporary table to hold the intersection between buffered street sections and points of interest

DROP TABLE IF EXISTS 
	intervista.intersected_pois_street;

CREATE TABLE 
	intervista.intersected_pois_street 
AS
SELECT 
	s.gid AS street_section_id
	, s.*
	, p.*
FROM 
	intervista.strassenabschnitte AS s
JOIN 
	geo_afo_prod.mv_lay_poi_aktuell AS p 
ON 
	ST_Intersects(s.buffered_geom, p.geo_point_lv95)
;


ALTER TABLE 
	intervista.strassenabschnitte 
ADD COLUMN IF NOT EXISTS
	anz_pois INT;

UPDATE 
	intervista.strassenabschnitte t0
SET 
	anz_pois = t1.poi_count
FROM
	( 
		SELECT 
			street_section_id,
			COUNT(*) AS poi_count
		FROM 
			intervista.intersected_pois_street 
		GROUP BY 
			street_section_id
	) t1
WHERE
	t0.gid = t1.street_section_id;

-- Nicht alle haben eine Anzahl POIs rundherum

UPDATE 
	intervista.strassenabschnitte
SET 
	anz_pois = 0 
WHERE 
	anz_pois IS NULL;


-- 2. Areal hinzufügen

ALTER TABLE 
	intervista.strassenabschnitte 
ADD COLUMN IF NOT EXISTS 
	centroid_point GEOMETRY,
ADD COLUMN IF NOT EXISTS 
	areal_2 GEOMETRY;

UPDATE 
	intervista.strassenabschnitte 
SET 
	centroid_point = ST_ClosestPoint(geo_line_lv95, ST_Centroid(geo_line_lv95));

UPDATE 
	intervista.strassenabschnitte 
SET 
	areal_2 = ST_BUFFER(centroid_point,20);
	
------------------------------------------
-- Anzahl Strassenabschnitte rausfischen
-------------------------------------------

SELECT 
	COUNT(*) 
FROM 
	intervista.strassenabschnitte 
WHERE 
	anz_pois > 3;
	



-----------------------------------------------------
--
-- AREALE HINZUFÜGEN
--
-----------------------------------------------------


-- Folgende Poi_typ_id's haben Areale:

-- 310, 406, 705, 706, 1205, 1206, 1301, 1304

-- Kontrolle welche Bus Stops Areale haben

SELECT 
	*
FROM 
	geo_afo_prod.lay_poi_geo_hist 
WHERE 
	poi_id IN
	( 
		SELECT 
			poi_id 
		FROM 
			intervista.bushaltestellen 
	)
	AND 
	uuid_swisstopo IS NOT NULL;


-- Hinzufügen der SWISSTOPO_UUID bei den POIs

ALTER TABLE 
	intervista.pois 
ADD COLUMN IF NOT EXISTS 
	uuid_swisstopo VARCHAR(200),
ADD COLUMN IF NOT EXISTS 
	areal GEOMETRY;


ALTER TABLE 
	intervista.bushaltestellen 
ADD COLUMN IF NOT EXISTS 
	uuid_swisstopo VARCHAR(200),
ADD COLUMN IF NOT EXISTS 
	areal GEOMETRY;


UPDATE 
	intervista.pois t0
SET 
	uuid_swisstopo = t1.uuid_swisstopo 
FROM 
	geo_afo_prod.lay_poi_geo_hist t1
WHERE 
	t0.poi_id = t1.poi_id;

UPDATE 
	intervista.bushaltestellen t0
SET 
	uuid_swisstopo = t1.uuid_swisstopo 
FROM 
	geo_afo_prod.lay_poi_geo_hist t1
WHERE 
	t0.poi_id = t1.poi_id;


-- Kontrolle der UUID Swisstopo

-- Areale hinzufügen

ALTER TABLE 
	intervista.pois 
ADD COLUMN 
	from_areal BOOLEAN DEFAULT FALSE;

UPDATE 
	intervista.pois 
SET 
	from_areal = FALSE;



-- Schulen (>0)

UPDATE 
	intervista.pois t0
SET 
	areal = t1.geom,
	from_areal = TRUE
FROM 
	swisstopo.v_swisstlm3d_tlm_schulareale t1
WHERE 
	t0.uuid_swisstopo = t1.uuid;

-- Nutzungsareal (>0)

UPDATE 
	intervista.pois t0
SET 
	areal = t1.geom,
	from_areal = TRUE
FROM 
	swisstopo.v_swisstlm3d_tlm_nutzungsareal t1
WHERE 
	t0.uuid_swisstopo = t1.uuid
	
-- Freizeitareal (0)

UPDATE 
	intervista.pois t0
SET 
	areal = t1.geom,
	from_areal = TRUE
FROM 
	swisstopo.v_swisstlm3d_tlm_freizeitareal t1
WHERE 
	t0.uuid_swisstopo = t1.uuid;

-- Geländename (0)

UPDATE 
	intervista.pois t0
SET 
	areal = t1.geom,
	from_areal = TRUE 
FROM 
	swisstopo.v_swisstlm3d_tlm_gelaendename t1
WHERE 
	t0.uuid_swisstopo = t1.uuid
;

-- Verkehrsareal (0)

UPDATE 
	intervista.pois t0
SET 
	areal = t1.geom,
	from_areal = TRUE 
FROM 
	swisstopo.v_swisstlm3d_tlm_verkehrsareal t1
WHERE 
	t0.uuid_swisstopo = t1.uuid
;


-----------------------------------------------------------
--
-- Hinzufügen der Areale wo keine vorhanden
--
-----------------------------------------------------------

-- Ideen:
	
-- EKZ höhere Buffer (50 m)
-- Schulen verfügbare Buffer, ansonsten auch höhere Buffer 100m
-- Strassen -> Buffer 30m
-- BHF -> Areal finden, ansonsten 100m
-- Bushaltestellen -> 20m
-- Tram -> 20m
-- Spitäler -> Areal oder 100m
-- Stadien -> Areal oder 200m
-- Food und Conv -> 30m

-- EKZ

UPDATE 
	intervista.pois
SET 
	areal = ST_Buffer(geo_point_lv95, 50)
WHERE 
	areal IS NULL
	AND 
	poi_typ_id = 504;

-- Schulen wo kein Areal 

UPDATE 
	intervista.pois
SET 
	areal = ST_Buffer(geo_point_lv95, 50)
WHERE 
	from_areal = FALSE
	AND 
	poi_typ_id IN (1203,1204,1205,1206,1207);

-- Spitäler (75m)

UPDATE 
	intervista.pois
SET 
	areal = ST_Buffer(geo_point_lv95, 75)
WHERE 
	from_areal = FALSE
	AND 
	poi_typ_id IN (406);

-- Bahnhöfe (100m)

UPDATE 
	intervista.pois
SET 
	areal = ST_Buffer(geo_point_lv95, 100)
WHERE 
	areal IS NULL
	AND 
	poi_typ_id IN (1301);

-- Tramhaltestellen (25m)

UPDATE 
	intervista.pois
SET 
	areal = ST_Buffer(geo_point_lv95, 25)
WHERE 
	areal IS NULL
	AND 
	poi_typ_id IN (1304);

-- Food / Convenience / Drogerie / Apotheken (30m)

UPDATE 
	intervista.pois
SET 
	areal = ST_Buffer(geo_point_lv95, 30)
WHERE 
	areal IS NULL
	AND 
	poi_typ_id IN (202,207,402, 403);

-- Stadien und Sport (200m)

UPDATE 
	intervista.pois
SET 
	areal = ST_Buffer(geo_point_lv95, 200)
WHERE 
	areal IS NULL
	AND 
	poi_typ_id IN (303,310);

-- Alle?

SELECT 
	*
FROM 
	intervista.pois 
WHERE 
	areal IS NULL;
;


-------------------
-- Areal Bushaltestellen
-------------------

UPDATE 
	intervista.bushaltestellen
SET 
	areal = ST_Buffer(geo_point_lv95, 25)
;

---------------------------------------
-- Erste Zusammenstellung POIs und Busse
---------------------------------------

DROP TABLE IF EXISTS 
	intervista.erste_auswahl;

CREATE TABLE 
	intervista.erste_auswahl
AS 
SELECT 
	CONCAT('p_', poi_id::TEXT) AS id,
	st_snaptogrid(areal,0.01) AS areal_raw
FROM 
	intervista.pois;
	
INSERT INTO 
	intervista.erste_auswahl 
SELECT 
	CONCAT('p_', poi_id::TEXT) AS id,
	st_snaptogrid(areal,0.01) AS areal_raw 
FROM 
	intervista.bushaltestellen
WHERE 
	anz_pois > 0;
	
INSERT INTO 
	intervista.erste_auswahl 
SELECT 
	CONCAT('s_', gid::TEXT) AS id,
	st_snaptogrid(areal_2,0.01) AS areal_raw
FROM 
	intervista.strassenabschnitte
WHERE 
	anz_pois > 3;


SELECT
	COUNT(*)
FROM 
	intervista.erste_auswahl;

-- 52'182 Punkte, nun wird aufgefüllt

INSERT INTO 
	intervista.erste_auswahl
SELECT
	CONCAT('s_', gid::TEXT) AS id,
	st_snaptogrid(areal_2,0.01) AS areal_raw
FROM 
	intervista.strassenabschnitte
WHERE 
	anz_pois <= 3
ORDER BY 
	dtv_alle DESC
LIMIT 
	10000;



----------------------------------------------------------
----------------------------------------------------------
-- Bereinigung der ersten Auswahl
----------------------------------------------------------
----------------------------------------------------------

-- Um sicherzustellen, dass nicht gelöschte wieder reinrutschen, wird eine separate Liste mit den ID's geführt, die schon mal ausgewählt wurden

DROP TABLE IF EXISTS 
	intervista.id_list;

CREATE TABLE 
	intervista.id_list 
AS 
SELECT 
	id 
FROM 
	intervista.erste_auswahl;

--------------------------
-- Gleiches Areal
--------------------------

-----------
-- Autobereinigung, einfach laufen lassen
-----------

DELETE FROM 
	intervista.erste_auswahl 
WHERE 
	id IN 
(
	SELECT 
		sub2.id 
	FROM 
		(
		SELECT
			id,
			areal_raw,
			geom_group,
		    ROW_NUMBER() OVER (PARTITION BY geom_group ORDER BY RANDOM()) AS rn
		FROM
		(
			SELECT
				id,
				areal_raw,
				DENSE_RANK() OVER (ORDER BY ST_AsText(areal_raw)) AS geom_group
			FROM
				intervista.erste_auswahl
		) sub1
		) sub2
	WHERE 
		rn > 1
);


-- Erste Bereinigung ergab von 50'000 rund 5211 Löschungen wegen Doppelarealen

SELECT 
	COUNT(*) 
FROM 
	intervista.erste_auswahl;

WHERE 
	id LIKE 's_%'
;

-- 24K POIS
-- 24.4K Strassen

-----------------------------------------------------------------------------------------
-- Bereinigung nach Doppelten abgeschlossen
-----------------------------------------------------------------------------------------


--------------------------
-- Zu viele Überschneidungen
--------------------------

CREATE INDEX 
	areal_index 
ON 
	intervista.erste_auswahl 
USING GIST 
	(areal_raw);

ALTER TABLE 
	intervista.erste_auswahl
ADD COLUMN IF NOT EXISTS
	intersections INT;
	
----------------------------
-- Überschneidungen finden
----------------------------

DROP TABLE IF EXISTS 
	intervista.intra_intersections;

CREATE TABLE 
	intervista.intra_intersections
AS
SELECT 
	t0.id,
	t0.areal_raw,
	t1.id AS id2,
	t1.areal_raw AS areal_raw2
FROM 
	intervista.erste_auswahl t0
JOIN 
	intervista.erste_auswahl t1
ON 
	ST_INTERSECTS(t0.areal_raw, t1.areal_raw)
WHERE 
	t0.id <> t1.id;
	
--ALTER TABLE 
--	intervista.intra_intersections 
--ADD COLUMN IF NOT EXISTS
--	same_area BOOLEAN DEFAULT FALSE,
--ADD COLUMN IF NOT EXISTS 
--	area_1_in_2 FLOAT,
--ADD COLUMN IF NOT EXISTS 
--	area_2_in_1 FLOAT;
	
--UPDATE 
--	intervista.intra_intersections
--SET 
--	area_1_in_2 = (ST_AREA(ST_INTERSECTION(areal_raw,areal_raw2))::FLOAT)/ST_AREA(areal_raw)::FLOAT,
--	area_2_in_1 = (ST_AREA(ST_INTERSECTION(areal_raw,areal_raw2))::FLOAT)/ST_AREA(areal_raw2)::FLOAT
--;

--UPDATE 
--	intervista.intra_intersections 
--SET 
--	same_area = TRUE 
--WHERE 
--	area_1_in_2 > 0.999
--	AND 
--	area_2_in_1 > 0.999;

-- Doppelte Areas rausfischen

--SELECT 
--	COUNT(*) 
--FROM -
--	intervista.intra_intersections 
--WHERE 
--	same_area = TRUE;
	


-------------------------
-- Analyse Intra Intersections
-------------------------

UPDATE 
	intervista.erste_auswahl t0
SET 
	intersections = t1.anzahl 
FROM 
	( 
		SELECT 
			id,
			COUNT(*) AS anzahl
		FROM 
			intervista.intra_intersections 
		GROUP BY 
			id
	) t1
WHERE 
	t0.id = t1.id;
	
SELECT 
	COUNT(*)
FROM 
	intervista.erste_auswahl 
WHERE 
	intersections > 4
AND 
	id LIKE 's_%';
	
SELECT 
	* 
FROM 
	intervista.intra_intersections 
WHERE 
	id = 's_268257';
	
----------------------------------------------------
----------------------------------------------------
-- Abschnitte anpassen
----------------------------------------------------
----------------------------------------------------

-- Einfügen von Typen 

ALTER TABLE 
	intervista.erste_auswahl 
ADD COLUMN 
	typ_code VARCHAR(5);
	
-- Autobahnen markieren

UPDATE
	intervista.erste_auswahl 
SET 
	typ_code = 'A'
WHERE 
	id LIKE 's_%'
	AND 
	REPLACE(id,'s_','')::INT IN (SELECT gid FROM geo_afo_prod.mv_lay_str_freq_aktuell WHERE str_type IN (1,5));

-- Bahnhöfe markieren

UPDATE
	intervista.erste_auswahl 
SET 
	typ_code = 'B'
WHERE 
	id LIKE 'p_%'
	AND 
	REPLACE(id,'p_','')::INT IN (SELECT poi_id FROM geo_afo_prod.mv_lay_poi_aktuell WHERE poi_typ_id = 1301);




--------------------------------------
-- Abschneiden von POI's ausser Autobahnen
--------------------------------------

ALTER TABLE 
	intervista.erste_auswahl 
ADD COLUMN IF NOT EXISTS 
	autobahn_flag BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS 
	schienen_flag BOOLEAN DEFAULT FALSE;


UPDATE 
	intervista.erste_auswahl 
SET 
	autobahn_flag = TRUE 
WHERE 
	id IN 
	(
		SELECT 
			t0.id
		FROM 
			intervista.erste_auswahl t0
		JOIN 
			intervista.autobahnen t1
		ON 
			ST_INTERSECTS(t0.areal_raw,ST_BUFFER(t1.geo_line_lv95,15))
	);	


UPDATE 
	intervista.erste_auswahl 
SET 
	schienen_flag = TRUE 
WHERE 
	id IN 
	(
		SELECT 
			t0.id
		FROM 
			intervista.erste_auswahl t0
		JOIN 
			intervista.bahnschienen t1
		ON 
			ST_INTERSECTS(t0.areal_raw,t1.geolv95)
	);	

--------------------------------------------


ALTER TABLE 
	intervista.erste_auswahl 
ADD COLUMN IF NOT EXISTS 
	areal_trimmed GEOMETRY;

-- Autobahn Trim

UPDATE 
	intervista.erste_auswahl t0
SET 
	areal_trimmed = ST_DIFFERENCE(t0.areal_raw,(SELECT ST_UNION(ST_BUFFER(geo_line_lv95,15)) FROM intervista.autobahnen))
WHERE 
	(typ_code = 'B' OR typ_code IS NULL)
	AND autobahn_flag = TRUE;
	
-- Bahnschienen Trim

ALTER TABLE 
	intervista.bahnschienen 
ADD COLUMN IF NOT EXISTS
	geolv95 GEOMETRY;

UPDATE 
	intervista.bahnschienen 
SET 
	geolv95 = ST_BUFFER(geom,15);

UPDATE 
	intervista.erste_auswahl t0
SET 
	areal_trimmed = 
	CASE 
		WHEN areal_trimmed IS NULL THEN	ST_DIFFERENCE(t0.areal_raw,ST_SetSRID((SELECT ST_UNION(geolv95) FROM intervista.bahnschienen),2056))
		WHEN areal_trimmed IS NOT NULL THEN ST_DIFFERENCE(t0.areal_trimmed,ST_SetSRID((SELECT ST_UNION(geolv95) FROM intervista.bahnschienen),2056))
	END
WHERE 
	t0.typ_code IS NULL
	AND 
	t0.schienen_flag = TRUE;
	
------------------------------
-- Finales Areal einfügen
------------------------------

ALTER TABLE 
	intervista.erste_auswahl 
ADD COLUMN 
	areal_final GEOMETRY;




WITH polygons AS (
  -- Decompose the MULTIPOLYGON into individual POLYGON geometries
  SELECT 
    id,
    (ST_Dump(areal_trimmed)).geom AS single_geom
  FROM 
    intervista.erste_auswahl 
),
polygon_areas AS (
  -- Calculate the area of each POLYGON
  SELECT 
    id,
    single_geom,
    ST_Area(single_geom) AS area
  FROM 
    polygons
),
largest_polygon AS (
  -- Select the POLYGON with the largest area for each id
  SELECT 
    id,
    single_geom AS largest_geom
  FROM 
    polygon_areas
  WHERE 
    (id, area) IN (
      SELECT 
        id,
        MAX(area) 
      FROM 
        polygon_areas 
      GROUP BY 
        id
    )
)
-- Update the original table to retain only the largest sub-polygon
UPDATE 
  intervista.erste_auswahl 
SET 
  areal_final = largest_geom
FROM 
  largest_polygon
WHERE 
  erste_auswahl.id = largest_polygon.id
  AND 
  areal_trimmed IS NOT NULL;
  
 
 
UPDATE 
	intervista.erste_auswahl 
SET 
	areal_final = areal_raw 
WHERE 
	areal_trimmed IS NULL 
	AND 
	areal_final IS NULL;
	

UPDATE 
	intervista.erste_auswahl
SET 
	areal_final = areal_raw 
WHERE 
	areal_final IS NULL;
	


-------------------------
-- Finallist Erstellung
-------------------------

DROP TABLE IF EXISTS 
	intervista.schweiz_punkte;

CREATE TABLE 
	intervista.schweiz_punkte
AS
SELECT 
	id AS id,
	areal_final AS flaeche
FROM 
	intervista.erste_auswahl;




SELECT 
	* 
FROM 
	geo_afo_prod.mv_lay_str_freq_aktuell 
WHERE 
	gid = 14045;
	
SELECT 
	* 
FROM 
	intervista.erste_auswahl 
WHERE 
	REPLACE(id,'p_','')::INT IN (SELECT poi_id FROM geo_afo_prod.mv_lay_poi_aktuell WHERE plz4 IN (6403,6402,6405))
	AND 
	id LIKE 'p_%';
	

SELECT 
	COUNT(*) 
FROM 
	intervista.erste_auswahl;