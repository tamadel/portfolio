--==================================================================================
-- DFSEO: Hauptkategorie >> "Food Geschäft"
--        kategory >> 'Lebensmittel', 'Lebensmittelgeschäft'
--        poi_typ >> alle 4 poi_typ
--Datum: 07.10.2024
--==================================================================================

-------------------------------------------------------------
-- Schritt 1: Auswahl der Point of Interest (POI)-Kategorie
-------------------------------------------------------------
SELECT 
	* 
FROM 
	geo_afo_prod.meta_poi_google_maps_category
WHERE 
	hauptkategorie_neu in ('Food-Geschäft', 'Food Geschäft')
	 AND 
	kategorie_neu in (
					'Lebensmittelgeschäft'
					,'Lebensmittelhändler'
					,'Getränkegeschäft'
					,'Convenience-Geschäft'
					,'Supermarkt'
					,'Bäckerei & Konditorei'
	)
	and 
	poi_typ_neu <> 'Lebensmittelgeschäft'
;

select distinct
	 kategorie_neu
	 ,hauptkategorie_neu 
from
	geo_afo_prod.meta_poi_google_maps_category
WHERE 
	hauptkategorie_neu in ('Food-Geschäft', 'Food Geschäft')
;
-----------------------------------------------------------------
-- Schritt 2: Erstellung der Metadatentabelle (T1) - POST-Anfrage
------------------------------------------------------------------
select 
	*
from 
	google_maps_dev.google_map_metadata_food_geschäft
where  
	cost = 0
	and 
	keyword not in(
				SELECT  
				   keyword
				FROM 
				    google_maps_dev.google_map_metadata_food_geschäft
				WHERE
				    id is not null
				    and 
				    datetime is null
	)
;	
	
select 
	*
from 
	google_maps_dev.google_map_metadata_food_geschäft
where 
	datetime is null
	and
	id is not null
;
 
SELECT distinct 
    plz4
    ,keyword
    ,id
FROM 
    google_maps_dev.google_map_metadata_food_geschäft
WHERE
    id is not null
    and 
    datetime is null
;


-----------------------------------------------------------------
-- Schritt 3: Abrufen der Rohdatentabelle (T2) - GET-Anfrage
-----------------------------------------------------------------
select 
	*
from 
	google_maps_dev.google_map_results_food_geschäft
;

select 
	*
from 
	geo_afo_prod.meta_poi_google_maps_category 
where 
	hauptkategorie_neu = 'Food-Geschäft'
;
-----------------------------------------------------------------
-- Schritt 4: Erstellung der Tabelle für eindeutige Einträge (T3)
-----------------------------------------------------------------
select 
	*
from 
	google_maps_dev.google_map_items_food_geschäft
;

DROP TABLE IF EXISTS 
          google_maps_dev.google_map_items_<category_name>;

CREATE TABLE 
          google_maps_dev.google_map_items_food_geschäft_v1 
AS
SELECT
    m.cid,
    m.rank_absolute,
    m.keyword,
    m.exact_match,
    m.address_info->>'zip' AS plz4,
    m.address_info->>'city' AS ort,
    m.address_info->>'address' AS strasse,
    m.address_info->>'country_code' AS country_code,
    m.address,
    m.title,
    m.phone,
    m.domain,
    m.url,
    m.rating,
    m.total_photos,
    m.hotel_rating,
    m.category,
    m.additional_categories,
    m.category_ids,
    m.work_hours,
    ST_Transform(ST_SetSRID(ST_MakePoint(m.longitude, m.latitude), 4326), 2056) AS geo_point_lv95,
    m.longitude,
    m.latitude
FROM
    (
        SELECT
            m.cid,
            m.rank_absolute,
            m.address_info,
            m.keyword,
            m.exact_match,
            m.address,
            m.title,
            m.phone,
            m.domain,
            m.url,
            m.rating,
            m.hotel_rating,
            m.total_photos,
            m.category,
            m.additional_categories,
            m.category_ids,
            m.work_hours,
            m.longitude,
            m.latitude,
            ROW_NUMBER() OVER (PARTITION BY m.cid ORDER BY m.rank_absolute, 
            RANDOM()) AS row_num
        FROM
            google_maps_dev.google_map_results_food_geschäft  m
        WHERE
            m.address_info->>'country_code' = 'CH'
    ) m
WHERE
    m.row_num = 1
;












--Test der Eindeutigkeit von cid:
SELECT 
    cid
    ,COUNT(*)
FROM 
    google_maps_dev.google_map_items_food_geschäft
GROUP BY 
    cid
HAVING  
    COUNT(*) > 1
;

---------------------------------------------------
--Schritt 5: Filtern und Anreichern der Daten (T4)
--------------------------------------------------
-- add category_ids_de 
ALTER TABLE 
       google_maps_dev.google_map_items_food_geschäft
ADD COLUMN 
       category_ids_de JSONB
;

UPDATE 
    google_maps_dev.google_map_items_food_geschäft
SET 
    category_ids_de = CASE
                        WHEN 
                        	additional_categories IS NULL 
                             OR 
                             additional_categories::text = 'null'
                        THEN 
                        	to_jsonb(array[category])
                        WHEN 
                        	jsonb_typeof(additional_categories::jsonb) = 'array'
                        THEN 
                        	jsonb_insert(additional_categories::jsonb, '{0}', to_jsonb(category))
                        ELSE 
                        	to_jsonb(array[category] || additional_categories::text)
                      END
;


select 
	*
from 
	google_maps_dev.google_map_items_food_geschäft
;

-- Tabelle zur Entfaltung der Kategorien erstellen:

DROP TABLE IF EXISTS 
         google_maps_dev.google_map_kateg_food_geschäft;

CREATE TABLE 
        google_maps_dev.google_map_kateg_food_geschäft 
AS
SELECT 
	*
FROM (
	    SELECT 
	        cid,
	        split_part(keyword, ' ', 1) AS keyword,
	        exact_match,
	        category,
	        additional_categories,
	        category_ids_de,
	        category_ids,
	        jsonb_array_elements_text(category_ids_de::jsonb) AS category_de,
	        jsonb_array_elements_text(category_ids::jsonb) AS category_en,
	        plz4,
	        ort,
	        strasse,
	        address,
	        title,
	        phone,
	        work_hours,
	        work_hours->>'current_status' AS current_status,
	        domain,
	        url,
	        rating->>'value' AS google_bewertung,
	        rating->>'votes_count' AS anz_bewertungen,
	        total_photos AS anz_fotos,
	        geo_point_lv95,
	        longitude,
	        latitude 
	    FROM 
	        google_maps_dev.google_map_items_food_geschäft
	    WHERE 
	        jsonb_typeof(category_ids_de::jsonb) = 'array'
	        AND 
	       	jsonb_typeof(category_ids::jsonb) = 'array'
) t
WHERE 
    category_en IN (
		               SELECT 
		                  category_en
		               FROM 
		                  google_maps_dev.google_map_category_hierarchy
		               WHERE 
		                  hauptkategorie_neu = 'Food Geschäft'
		                  AND 
		                  kategorie_neu in ('Lebensmittel', 'Lebensmittelgeschäft')
   )
;


---------------------------------------------------------------------------------------
-- Schritt 6: Hinzufügen neuer Attribute und Vervollständigung der fehlenden Daten (T5)
---------------------------------------------------------------------------------------
drop table if exists  
    google_maps_dev.google_map_food_geschäft;

create table  
    google_maps_dev.google_map_food_geschäft
as 
select  
    cid,
    keyword,
    category_ids_de,
    category_ids,
    STRING_AGG(distinct category_de, '/ ') as categories_de,
    STRING_AGG(distinct category_en, '/ ') as categories_en,
    title,
    address,
    strasse as str_hausnummer,
    REGEXP_REPLACE(strasse, '[0-9]+[A-Za-z]*$', '') as strasse,
    (REGEXP_MATCH(strasse, '[0-9]+[A-Za-z]*$'))[1] as hausnummer,
    plz4,
    ort,
    domain,
    url,
    google_bewertung,
    anz_bewertungen,
    anz_fotos,
    geo_point_lv95,
    longitude,
    latitude
from 
    google_maps_dev.google_map_kateg_food_geschäft
group by 
    cid,
    title,
    plz4,
    ort,
    strasse,
    address,
    domain,
    url,
    google_bewertung,
    anz_bewertungen,
    anz_fotos,
    geo_point_lv95,
    longitude,
    latitude,
    category_ids_de,
    category_ids,
    keyword
;


SELECT
    *
FROM
    google_map_food_geschäft
;














