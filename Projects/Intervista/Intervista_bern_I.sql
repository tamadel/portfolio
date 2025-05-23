
-- Genereller Ablauf, Queries sollten strukturiert sein, zu viele Tabellen nicht nötig.
-- Da danach zu viele Joins vorhanden, man kann 1 Bus Tabelle und 1 Strassen Tabellen machen 
-- und dann einzeln anhängen, geht darum, dass die einzelnen Schritte / Queries einzeln passieren,
-- aber nicht jede einzelne Query eine separate Tabelle enthält -> Wird sehr schnell unübersichtlich


-- Create a temporary table to store the POIs
CREATE TABLE 
	intervista_bern.temp_pois
AS
SELECT 
	*
FROM 
	geo_afo_prod.mv_lay_poi_aktuell 
WHERE 
	ort ='Bern'
AND 
	poi_typ_id IN (1301, '1302', '1303', '1304', '303', '504', '406', '207', '202', '310', '317', '1206');

-- tamer
-- Auf Formatierung achten
-- ort 'Bern' bei Falschschreibung nicht ideal, arbeite hier mit Postleitzahlen
-- poi_typ_id's sind Integers, brauchen keine Anführungs und Schlusszeichen
-- Kommentierung der einzelnen Nummern, ansonsten schwer nachzuvollziehen, welche wozu gebraucht wurden

--//////////////////////////////////////////////////////// bus stop /////////////////////////////////////////////////////
	
-- Create a temporary table to hold the buffered bus stop geometries
CREATE TABLE intervista_bern.buffered_bus_stops 
AS
SELECT 
	b.poi_id
	, ST_Buffer(b.geo_point_lv95, 50) AS buffered_geom
FROM 
	geo_afo_prod.mv_lay_poi_aktuell AS b
where
	ort = 'Bern'
AND 
	b.poi_typ_id  = '1302';

-- tamer
-- Hier ebenfalls mit Postleitzahlen arbeiten.

-- Create a temporary table to hold the intersection between buffered bus stops and poi
CREATE TABLE intervista_bern.intersected_pois_bus 
AS
SELECT 
	b.poi_id AS bus_stop_id
	,b.buffered_geom
	,p.*
FROM 
	intervista_bern.buffered_bus_stops  AS b 
JOIN 
	geo_afo_prod.mv_lay_poi_aktuell  AS p 
ON 
	ST_Intersects(b.buffered_geom, p.geo_point_lv95)
where
	P.poi_typ_id <> '1302';

-- tamer:

SELECT 
	DISTINCT bus_stop_id 
FROM 
	intervista_bern_tamer.intersected_pois_bus;

-- Hier hast du nur die Busse, die auch Überschneidungen haben, um das ganze vollständig zu haben, solltest du eine Tabelle
-- führen, die alle Busstops beinhaltet, so kannst du kontrollieren, welche Busse z.B, 0 Überschneidungen haben und
-- schauen, ob dies plausibel ist, oder nicht

-- Calculate the count of pois for each bus stop
CREATE TABLE intervista_bern.bus_stop_poi_count 
AS
SELECT 
	 bus_stop_id
	,buffered_geom
	,COUNT(*) AS poi_count
FROM 
	intervista_bern.intersected_pois_bus
GROUP BY 
	 bus_stop_id
	,buffered_geom;

-- Filter out bus stops based on the count of pois
CREATE TABLE intervista_bern.good_bus_stops 
AS
SELECT 
	bus_stop_id
	,buffered_geom
FROM 
	intervista_bern.bus_stop_poi_count
WHERE 
	poi_count > 2; -- adjust as needed
	
	
--DROP TABLE intervista_bern.buffered_bus_stops;
--DROP TABLE intervista_bern.intersected_pois;   
--DRPO TABLE intervista_bern.bus_stop_poi_count;
--DROP TABLE intervista_bern.good_bus_stops; 
 

--/////////////////////////////////// Street sections //////////////////////////////////////////////////////////////

-- Create a temporary table to hold the buffered street section geometries
CREATE TABLE intervista_bern.buffered_street_sections 
AS
SELECT 
	  s.*
	, ST_Buffer(s.geo_line_lv95, 30) AS buffered_geom
FROM 
	geo_afo_prod.mv_lay_str_freq_aktuell AS s;

-- tamer
-- Hier sind alle Strassenfrequenzen drinnen, hier hätte man nur die in Bern nehmen können,
-- Dies kann man einfach so machen, dass nur Strassen berücksichtigt werden, die Kontakt zur Fläche der Stadt Bern haben.

-- tamer SQL

CREATE TABLE 
	intervista_bern_tamer.strassen_bern 
AS 
SELECT 
	t0.* 
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
	)

-- Create a temporary table to hold the intersection between buffered street sections and points of interest
CREATE TABLE intervista_bern.intersected_pois_street 
AS
SELECT 
	s.gid AS street_section_id
	, s.*
	, p.*
FROM 
	intervista_bern.buffered_street_sections AS s
JOIN 
	geo_afo_prod.mv_lay_poi_aktuell AS p 
ON 
	ST_Intersects(s.buffered_geom, p.geo_point_lv95)
WHERE
	ort ='Bern';

-- tamer
-- Bei Joins unbedingt beachten, dass 'ort' in beiden Tabellen vorkommen kann
-- Hier klar machen, auf welche Tablle du dich beziehst -> p.ort

-- Calculate the count of points of interest for each street section
CREATE TABLE intervista_bern.street_section_poi_count 
AS
SELECT 
	street_section_id
	,buffered_geom
	,geo_line_lv03
	,geo_line_lv95
	,geo_line_wgs84
	,COUNT(*) AS poi_count
FROM 
	intervista_bern.intersected_pois_street
GROUP BY 
	street_section_id
	,buffered_geom
	,geo_line_lv03
	,geo_line_lv95
	,geo_line_wgs84;




-- Filter out street sections based on the count of points of interest
CREATE TABLE intervista_bern.high_frequency_street_sections 
AS
SELECT 
	street_section_id
	--,geo_line_lv03
	--,geo_line_lv95
	--,geo_line_wgs84
	,buffered_geom
FROM 
	intervista_bern.street_section_poi_count
WHERE 
	poi_count > 10; -- adjust as needed
	
	
--DROP TABLE intervista_bern.buffered_street_sections;
--DROP TABLE intervista_bern.intersected_pois_street;
--DROP TABLE intervista_bern.street_section_poi_count;
--DROP TABLE intervista_bern.high_frequency_street_sections;    
--//////////////////////////////////////////////////////////// final Poi's ////////////////////////////////////////////////////////

	
-- Create a table for all relevant POIs_bus_stops
CREATE TABLE intervista_bern.final_pois_bus_stops 
AS   
SELECT 
	*
FROM 
	intervista_bern.intersected_pois_bus  
WHERE 
	bus_stop_id  IN (SELECT bus_stop_id  FROM intervista_bern.good_bus_stops)



-- Create a table for all relevant POIs_street_section
CREATE TABLE intervista_bern.final_pois_street_sections
AS
SELECT 
	*
FROM 
	intervista_bern.intersected_pois_street  
WHERE 
	street_section_id  IN (SELECT street_section_id FROM intervista_bern.high_frequency_street_sections);


--DROP TABLE intervista_bern.final_pois_bus_stops;
--DROP TABLE intervista_bern.final_pois_street_sections;




--//////////////////////////////////////////////////////////////////////////////////////////////////////////////
SELECT 
	*
FROM 
	geo_afo_prod.mv_lay_str_freq_aktuell;


	
