
-- Create a temporary table to store the POIs
DROP TABLE IF EXISTS 
	intervista_bern_tamer.temp_pois;

CREATE TABLE 
	intervista_bern_tamer.temp_pois
AS
SELECT 
	*
FROM 
	geo_afo_prod.mv_lay_poi_aktuell 
WHERE 
	plz4 IN 
	( 
		SELECT 
			plz4 
		FROM
			geo_afo_prod.mv_lay_plz4_aktuell 
		WHERE 
			ort = 'Bern'
	)
AND 
	poi_typ_id IN 
	(
		1301, --Bhf
		1304, -- Tram
		504, --EKZ
		1206, -- Tertiäre Stufe Schulen
		1207, -- Zweitbildung nicht-tertiäre Stufe
		1205, -- Sekundarstufe 2
		406, -- Spitäler
		202, -- Convenience
		207, -- Food
		303, -- Eishockey
		310 -- Sportstadion
	);

DROP TABLE IF EXISTS 
	intervista_bern_tamer.verpflegung_pois;

CREATE TABLE 
	intervista_bern_tamer.verpflegung_pois 
AS 
SELECT 
	*
FROM 
	geo_afo_prod.mv_lay_poi_aktuell 
WHERE 
	plz4 IN 
	( 
		SELECT 
			plz4 
		FROM
			geo_afo_prod.mv_lay_plz4_aktuell 
		WHERE 
			ort = 'Bern'
	)
AND 
	poi_typ_id IN 
	(
		1004 -- alle Verpflegungs POIs
	);
	

-- Auf Formatierung achten
-- ort 'Bern' bei Falschschreibung nicht ideal, arbeite hier mit Postleitzahlen
-- poi_typ_id's sind Integers, brauchen keine Anführungs und Schlusszeichen
-- Kommentierung der einzelnen Nummern, ansonsten schwer nachzuvollziehen, welche wozu gebraucht wurden

--//////////////////////////////////////////////////////// bus stop /////////////////////////////////////////////////////
	
-- Create a temporary table to hold the buffered bus stop geometries
DROP TABLE IF EXISTS 
	intervista_bern_tamer.buffered_bus_stops;

CREATE TABLE 
	intervista_bern_tamer.buffered_bus_stops 
AS
SELECT 
	b.poi_id
	, ST_Buffer(b.geo_point_lv95, 100) AS buffered_geom
FROM 
	geo_afo_prod.mv_lay_poi_aktuell AS b
where
	b.plz4 IN 
	( 
		SELECT 
			plz4 
		FROM
			geo_afo_prod.mv_lay_plz4_aktuell 
		WHERE 
			ort = 'Bern'
	)
AND 
	b.poi_typ_id  = 1302 --Bus;



-- Create a temporary table to hold the intersection between buffered bus stops and poi
DROP TABLE IF EXISTS
	intervista_bern_tamer.intersected_pois_bus;

CREATE TABLE 
	intervista_bern_tamer.intersected_pois_bus 
AS
SELECT 
	b.poi_id AS bus_stop_id
	,b.buffered_geom
	,p.*
FROM 
	intervista_bern_tamer.buffered_bus_stops AS b 
JOIN 
	geo_afo_prod.mv_lay_poi_aktuell AS p 
ON 
	ST_Intersects(b.buffered_geom, p.geo_point_lv95)
where
	P.poi_typ_id <> 1302;

--tamer:

-- Kontrolle wieviel dieser Bus Stops überhaupt eine Überschneidung aufweisen

SELECT 
	DISTINCT bus_stop_id 
FROM 
	intervista_bern_tamer.intersected_pois_bus;

-- Hier hast du nur die Busse, die auch Überschneidungen haben, um das ganze vollständig zu haben, solltest du eine Tabelle
-- führen, die alle Busstops beinhaltet, so kannst du kontrollieren, welche Busse z.B, 0 Überschneidungen haben und
-- schauen, ob dies plausibel ist, oder nicht

ALTER TABLE 
	intervista_bern_tamer.buffered_bus_stops 
ADD COLUMN 
	anz_pois INT;

UPDATE 
	intervista_bern_tamer.buffered_bus_stops t0
SET 
	anz_pois = t1.poi_count
FROM
	( 
		SELECT 
			bus_stop_id,
			COUNT(*) AS poi_count
		FROM 
			intervista_bern_tamer.intersected_pois_bus 
		GROUP BY 
			bus_stop_id
	) t1
WHERE
	t0.poi_id = t1.bus_stop_id;

-- Nicht alle haben eine Anzahl POIs rundherum

UPDATE 
	intervista_bern_tamer.buffered_bus_stops
SET 
	anz_pois = 0 
WHERE 
	anz_pois IS NULL;

--tamer
-- Wir haben alle nötigen Infos in der Bus Tabelle, diese Tabellen sind unnötig

-- Calculate the count of pois for each bus stop
--CREATE TABLE intervista_bern_tamer.bus_stop_poi_count 
--AS
--SELECT 
--	 bus_stop_id
--	,buffered_geom
--	,COUNT(*) AS poi_count
--FROM 
--	intervista_bern_tamer.intersected_pois_bus
--GROUP BY 
--	 bus_stop_id
--	,buffered_geom;

-- Filter out bus stops based on the count of pois
--CREATE TABLE 
--	intervista_bern_tamer.good_bus_stops 
--AS
--SELECT 
--	bus_stop_id
--	,buffered_geom
--FROM 
--	intervista_bern_tamer.bus_stop_poi_count
--WHERE 
--	poi_count > 2; -- adjust as needed
	
	
--DROP TABLE intervista_bern_tamer.buffered_bus_stops;
--DROP TABLE intervista_bern_tamer.intersected_pois;   
--DRPO TABLE intervista_bern_tamer.bus_stop_poi_count;
--DROP TABLE intervista_bern_tamer.good_bus_stops; 
 

--/////////////////////////////////// Street sections //////////////////////////////////////////////////////////////

-- Create a temporary table to hold the buffered street section geometries
--CREATE TABLE 
--	intervista_bern_tamer.buffered_street_sections 
--AS
--SELECT 
--	  s.*
--	, ST_Buffer(s.geo_line_lv95, 30) AS buffered_geom
--FROM 
--	geo_afo_prod.mv_lay_str_freq_aktuell AS s;

--tamer
-- Hier sind alle Strassenfrequenzen drinnen, hier hätte man nur die in Bern nehmen können,
-- Dies kann man einfach so machen, dass nur Strassen berücksichtigt werden, die Kontakt zur Fläche der Stadt Bern haben.

--tamer SQL

CREATE TABLE 
	intervista_bern_tamer.buffered_street_sections 
AS 
SELECT 
	t0.*,
	ST_Buffer(t0.geo_line_lv95, 30) AS buffered_geom
FROM 
	geo_afo_prod.mv_lay_str_freq_aktuell t0
JOIN 
	geo_afo_prod.mv_lay_plz4_aktuell t1
ON 
	ST_INTERSECTS(t0.geo_line_lv95,t1.geo_poly_lv95)
WHERE 
	t1.plz4 IN 
	(
		SELECT 
			plz4 
		FROM
			geo_afo_prod.mv_lay_plz4_aktuell 
		WHERE 
			ort = 'Bern'
	);

-- Create a temporary table to hold the intersection between buffered street sections and points of interest
CREATE TABLE 
	intervista_bern_tamer.intersected_pois_street 
AS
SELECT 
	s.gid AS street_section_id
	, s.*
	, p.*
FROM 
	intervista_bern_tamer.buffered_street_sections AS s
JOIN 
	geo_afo_prod.mv_lay_poi_aktuell AS p 
ON 
	ST_Intersects(s.buffered_geom, p.geo_point_lv95)
;

SELECT 
	DISTINCT street_section_id 
FROM 
	intervista_bern_tamer.intersected_pois_street;



-- Bei Joins unbedingt beachten, dass 'ort' in beiden Tabellen vorkommen kann
-- Hier klar machen, auf welche Tablle du dich beziehst -> p.ort

ALTER TABLE 
	intervista_bern_tamer.buffered_street_sections 
ADD COLUMN IF NOT EXISTS
	anz_pois INT;

UPDATE 
	intervista_bern_tamer.buffered_street_sections t0
SET 
	anz_pois = t1.poi_count
FROM
	( 
		SELECT 
			street_section_id,
			COUNT(*) AS poi_count
		FROM 
			intervista_bern_tamer.intersected_pois_street 
		GROUP BY 
			street_section_id
	) t1
WHERE
	t0.gid = t1.street_section_id;

-- Nicht alle haben eine Anzahl POIs rundherum

UPDATE 
	intervista_bern_tamer.buffered_street_sections
SET 
	anz_pois = 0 
WHERE 
	anz_pois IS NULL;

-- Doppeleinträge löschen

CREATE TABLE 
	intervista_bern_tamer.buffered_street_sections_bereinigt 
AS
SELECT 
	*,
    ROW_NUMBER() OVER (PARTITION BY str_id ORDER BY RANDOM()) AS RowNumber
FROM 
	intervista_bern_tamer.buffered_street_sections;

DELETE FROM intervista_bern_tamer.buffered_street_sections_bereinigt  WHERE RowNumber > 1;

-- 2. Areal hinzufügen

ALTER TABLE 
	intervista_bern_tamer.buffered_street_sections 
ADD COLUMN IF NOT EXISTS 
	centroid_point GEOMETRY,
ADD COLUMN IF NOT EXISTS 
	areal_2 GEOMETRY;

UPDATE 
	intervista_bern_tamer.buffered_street_sections 
SET 
	centroid_point = ST_CENTROID(geo_line_lv95);

UPDATE 
	intervista_bern_tamer.buffered_street_sections 
SET 
	areal_2 = ST_BUFFER(centroid_point,25);



-----------------------------------------------------
--
-- AREALE HINZUFÜGEN
--
-----------------------------------------------------


-- Generell schauen, wo Areale zu finden bei den Temp POIs

SELECT 
	DISTINCT 
		poi_typ_id 
FROM 
	geo_afo_prod.lay_poi_geo_hist 
WHERE 
	poi_id IN
	( 
		SELECT 
			poi_id 
		FROM 
			intervista_bern_tamer.temp_pois 
	)
	AND 
	uuid_swisstopo IS NOT NULL;


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
			intervista_bern_tamer.buffered_bus_stops 
	)
	AND 
	uuid_swisstopo IS NOT NULL;


-- Hinzufügen der SWISSTOPO_UUID bei den POIs

ALTER TABLE 
	intervista_bern_tamer.temp_pois 
ADD COLUMN IF NOT EXISTS 
	uuid_swisstopo VARCHAR(200),
ADD COLUMN IF NOT EXISTS 
	areal GEOMETRY;


ALTER TABLE 
	intervista_bern_tamer.buffered_bus_stops 
ADD COLUMN IF NOT EXISTS 
	uuid_swisstopo VARCHAR(200),
ADD COLUMN IF NOT EXISTS 
	areal GEOMETRY;


UPDATE 
	intervista_bern_tamer.temp_pois t0
SET 
	uuid_swisstopo = t1.uuid_swisstopo 
FROM 
	geo_afo_prod.lay_poi_geo_hist t1
WHERE 
	t0.poi_id = t1.poi_id;

UPDATE 
	intervista_bern_tamer.buffered_bus_stops t0
SET 
	uuid_swisstopo = t1.uuid_swisstopo 
FROM 
	geo_afo_prod.lay_poi_geo_hist t1
WHERE 
	t0.poi_id = t1.poi_id;


-- Kontrolle der UUID Swisstopo

-- Areale hinzufügen

UPDATE 
	intervista_bern_tamer.temp_pois 
SET 
	from_areal = FALSE;

ALTER TABLE 
	intervista_bern_tamer.temp_pois 
ADD COLUMN 
	from_areal BOOLEAN DEFAULT FALSE;

-- Schulen (19)

UPDATE 
	intervista_bern_tamer.temp_pois t0
SET 
	areal = t1.geom,
	from_areal = TRUE
FROM 
	swisstopo.v_swisstlm3d_tlm_schulareale t1
WHERE 
	t0.uuid_swisstopo = t1.uuid;

-- Nutzungsareal (30)

UPDATE 
	intervista_bern_tamer.temp_pois t0
SET 
	areal = t1.geom,
	from_areal = TRUE
FROM 
	swisstopo.v_swisstlm3d_tlm_nutzungsareal t1
WHERE 
	t0.uuid_swisstopo = t1.uuid
	
-- Freizeitareal (0)

UPDATE 
	intervista_bern_tamer.temp_pois t0
SET 
	areal = t1.geom,
	from_areal = TRUE
FROM 
	swisstopo.v_swisstlm3d_tlm_freizeitareal t1
WHERE 
	t0.uuid_swisstopo = t1.uuid;

-- Geländename (0)

UPDATE 
	intervista_bern_tamer.temp_pois t0
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
	intervista_bern_tamer.temp_pois t0
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
	intervista_bern_tamer.temp_pois
SET 
	areal = ST_Buffer(geo_point_lv95, 50)
WHERE 
	areal IS NULL
	AND 
	poi_typ_id = 504;

-- Schulen wo kein Areal 

UPDATE 
	intervista_bern_tamer.temp_pois
SET 
	areal = ST_Buffer(geo_point_lv95, 50)
WHERE 
	from_areal = FALSE
	AND 
	poi_typ_id IN (1205,1206,1207);

-- Spitäler (75m)

UPDATE 
	intervista_bern_tamer.temp_pois
SET 
	areal = ST_Buffer(geo_point_lv95, 75)
WHERE 
	from_areal = FALSE
	AND 
	poi_typ_id IN (406);

-- Bahnhöfe (100m)

UPDATE 
	intervista_bern_tamer.temp_pois
SET 
	areal = ST_Buffer(geo_point_lv95, 100)
WHERE 
	areal IS NULL
	AND 
	poi_typ_id IN (1301);

-- Tramhaltestellen (25m)

UPDATE 
	intervista_bern_tamer.temp_pois
SET 
	areal = ST_Buffer(geo_point_lv95, 25)
WHERE 
	areal IS NULL
	AND 
	poi_typ_id IN (1304);

-- Food / Convenience (30m)

UPDATE 
	intervista_bern_tamer.temp_pois
SET 
	areal = ST_Buffer(geo_point_lv95, 30)
WHERE 
	areal IS NULL
	AND 
	poi_typ_id IN (202,207);

-- Stadien und Sport (200m)

UPDATE 
	intervista_bern_tamer.temp_pois
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
	intervista_bern_tamer.temp_pois 
WHERE 
	areal IS NULL;

-- Bushaltestellen

ALTER TABLE 
	intervista_bern_tamer.buffered_bus_stops
ADD COLUMN IF NOT EXISTS
	geo_point_lv95 GEOMETRY;

UPDATE 
	intervista_bern_tamer.buffered_bus_stops t0
SET 
	geo_point_lv95 = t1.geo_point_lv95
FROM 
	geo_afo_prod.mv_lay_poi_aktuell t1
WHERE 
	t0.poi_id = t1.poi_id;

UPDATE 
	intervista_bern_tamer.buffered_bus_stops
SET 
	areal = ST_Buffer(geo_point_lv95, 25)
;


-- Testung wieviele POIs (Aufbereitung)

SELECT 
	COUNT(*) 
FROM 
	intervista_bern_tamer.temp_pois;

DELETE FROM 
	intervista_bern_tamer.temp_pois 
WHERE 
	poi_id = 63449;

SELECT 
	COUNT(*) 
FROM 
	intervista_bern_tamer.buffered_street_sections_bereinigt 
WHERE 
	anz_pois > 5;


SELECT 
	COUNT(*) 
FROM 
	intervista_bern_tamer.buffered_bus_stops 
WHERE 
	anz_pois > 2;




-------------------------
-- Finallist Erstellung
-------------------------

DROP TABLE IF EXISTS 
	intervista_bern_tamer.final_list;

CREATE TABLE 
	intervista_bern_tamer.final_list
AS
SELECT 
	poi_id AS id,
	areal AS flaeche,
	TRUE AS is_poi_id 
FROM 
	intervista_bern_tamer.temp_pois 
;

INSERT INTO 
	intervista_bern_tamer.final_list
SELECT 
	poi_id,
	areal,
	TRUE
FROM 
	intervista_bern_tamer.buffered_bus_stops
WHERE 
	anz_pois > 2;

INSERT INTO 
	intervista_bern_tamer.final_list
SELECT 
	gid,
	areal_2,
	FALSE 
FROM 
	intervista_bern_tamer.buffered_street_sections_bereinigt
WHERE 
	anz_pois > 5;

-- Auffüllen mit High Frequenz Strassenabschnitten

INSERT INTO 
	intervista_bern_tamer.final_list
SELECT 
	gid,
	areal_2,
	FALSE 
FROM 
	intervista_bern_tamer.buffered_street_sections_bereinigt
WHERE 
	gid NOT IN 
	( 
		SELECT 
			id 
		FROM 
			intervista_bern_tamer.final_list
		WHERE 
			is_poi_id = FALSE
	)
ORDER BY 
	dtv_alle DESC
LIMIT 78;

SELECT 
	* 
FROM 
	intervista_bern_tamer.final_list;


-- Exportieren im GeoJSON

CREATE TABLE 
	intervista_bern_tamer.export_table 
AS
SELECT 
	*,
	ST_AsGeoJSON(flaeche) AS geojson 
FROM 
	intervista_bern_tamer.final_list;

ALTER TABLE 
	intervista_bern_tamer.export_table
DROP COLUMN 
	geojson;

-- Calculate the count of points of interest for each street section
--CREATE TABLE intervista_bern_tamer.street_section_poi_count 
--AS
--SELECT 
--	street_section_id
--	,buffered_geom
--	,geo_line_lv03
--	,geo_line_lv95
--	,geo_line_wgs84
--	,COUNT(*) AS poi_count
--FROM 
--	intervista_bern_tamer.intersected_pois_street
--GROUP BY 
--	street_section_id
--	,buffered_geom
--	,geo_line_lv03
--	,geo_line_lv95
--	,geo_line_wgs84;


-- Filter out street sections based on the count of points of interest
--CREATE TABLE intervista_bern_tamer.high_frequency_street_sections 
--AS
--SELECT 
--	street_section_id
--	--,geo_line_lv03
--	--,geo_line_lv95
--	--,geo_line_wgs84
--	,buffered_geom
---FROM 
--	intervista_bern_tamer.street_section_poi_count
--WHERE 
--	poi_count > 10; -- adjust as needed
	
	
--DROP TABLE intervista_bern_tamer.buffered_street_sections;
--DROP TABLE intervista_bern_tamer.intersected_pois_street;
--DROP TABLE intervista_bern_tamer.street_section_poi_count;
--DROP TABLE intervista_bern_tamer.high_frequency_street_sections;    
--//////////////////////////////////////////////////////////// final Poi's ////////////////////////////////////////////////////////

	
-- Create a table for all relevant POIs_bus_stops
--CREATE TABLE intervista_bern_tamer.final_pois_bus_stops 
--AS   
--SELECT 
--	*
--FROM 
--	intervista_bern_tamer.intersected_pois_bus  
--WHERE 
--	bus_stop_id IN (SELECT bus_stop_id  FROM intervista_bern_tamer.good_bus_stops)



-- Create a table for all relevant POIs_street_section
--CREATE TABLE intervista_bern_tamer.final_pois_street_sections
--AS
--SELECT 
--	*
--FROM 
--	intervista_bern_tamer.intersected_pois_street  
--WHERE 
--	street_section_id  IN (SELECT street_section_id FROM intervista_bern_tamer.high_frequency_street_sections);



--DROP TABLE intervista_bern_tamer.final_pois_bus_stops;
--DROP TABLE intervista_bern_tamer.final_pois_street_sections;




--//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	
------------------------------------------------------------------------------------------------------------

--tamer Playground 

------------------------------------------------------------------------------------------------------------

--------------
-- OLTEN
--------------

SELECT 
	*
FROM 
	geo_afo_prod.mv_lay_poi_aktuell 
WHERE 
	ort = 'Olten';

CREATE TABLE 
	geo_poi_olten.olten_pois_korrigiert 
AS
SELECT 
	* 
FROM 
	geo_afo_prod.mv_lay_poi_aktuell 
WHERE 
	ort = 'Olten';

INSERT INTO 
	geo_poi_olten.olten_pois_korrigiert
	
	
SELECT 
	* 
FROM
	geo_afo_prod.mv_lay_poi_aktuell 
WHERE 
	plz4 = 4600
	AND 
	ort <> 'Olten';

SELECT 
	* 
FROM
	geo_afo_prod.mv_lay_poi_aktuell 
WHERE 
	ort LIKE '%Olten%'
	AND 
	poi_id NOT IN (SELECT poi_id FROM geo_poi_olten.olten_pois_korrigiert);

------------
-- BERN
------------

SELECT 
	*
FROM
	geo_afo_prod.mv_lay_poi_aktuell 
WHERE 
	plz4 IN
	(
	SELECT 
		plz4 
	FROM
		geo_afo_prod.mv_lay_plz4_aktuell 
	WHERE 
		ort = 'Bern'
	)
	AND 
	poi_typ_id = 1302;
	

CREATE TABLE 
	intervista_bern_tamer.export_tabelle_bern
AS 
SELECT 
	* 
FROM 
	intervista_bern_tamer.export_table;
	
SELECT
	* 
FROM 
	