--==================================================================================
-- DFSEO: Hauptkategorie >> "Mobilität_p4"
--        kategory >> 'Haltestelle'
--        poi_typ >> alle 6 poi_typ
--Datum: 07.10.2024
--==================================================================================

--First: select the poi category to use them as keyword in DataForSEO 
----------------------------------------------
--Step(1): Selected Category for Mobilität_P2
----------------------------------------------
select 
	*
from 
	geo_afo_prod.meta_poi_google_maps_category
where 
	hauptkategorie_neu = 'Mobilität'
	and 
	kategorie_neu = 'Haltestelle' 
;


--second: create the first table whiches the metadata table 
-------------------------------------------------
--Step(2): T1 google_map_metadata (Metadata) "POST"
-------------------------------------------------
select 
	*
from 
	google_maps_dev.google_map_metadata_mobilität
where 
	hauptkategorie_neu = 'Mobilität'
	and 
	kategorie_neu = 'Haltestelle'
;


--third: create the second table which is the retrieaved data from DataForSEO it's a row data 
-------------------------------------------------
--Step(3): T2 google_map_results (Results) "GET"
-------------------------------------------------
select 
	*
from 
	google_maps_dev.google_map_results_mobilität 
where
	poi_typ in (
				'Bushaltestelle'
				,'Fährterminal'
				,'Haltestelle'
				,'Bergbahn'
				,'Tramhaltestelle'
				,'Bahnhof'	
    )
;


select 
	*
from 
	google_maps_dev.google_map_results_mobilität
where 
	poi_typ is null
;


-- adding a new column called "poi_typ" where we split the poi type from the keyword to filter the table with poi_type in case the table contains lots of different poi types 
alter table google_maps_dev.google_map_results_mobilität
add column poi_typ text
;

update google_maps_dev.google_map_results_mobilität_piv
set 
	poi_typ = TRIM(SUBSTRING(keyword FROM '^[^0-9]+'))
where 
	poi_typ is null 
;
	

--fourth: create the third table wich is called items in this table we filter "rank_absoult" attribute and select the one has the min(rank_absolute) and like this we get rid of duplicated cid and we have at the end a table with unique "Cid"
-------------------------------------------------------------------------------------
--Step(4): T3 google_map_items (Items) unique cid and filitered by "rank_abslout"
-- created by Py script
-------------------------------------------------------------------------------------

select 
	*
from 
	google_maps_dev.google_map_items_mobilität_piv
;

select 
	cid
	,count(*)
from
	google_maps_dev.google_map_items_mobilität_piv
group by
	cid
having 
	count(*) > 1
;

-- continue from the Doku 
---------------------------------------------------
--Step(5): T4  Filtern und Anreichern der Daten 
--------------------------------------------------
alter table  
       google_maps_dev.google_map_items_mobilität_piv
add column  
       category_ids_de JSONB
;

--category_ids_de aktualisieren:
update  
    google_maps_dev.google_map_items_mobilität_piv
set  
    category_ids_de = case 
                        when additional_categories is null  
                             or  
                             additional_categories::text = 'null'
                        then
                        	to_jsonb(array[category])
                        when
                        	jsonb_typeof(additional_categories::jsonb) = 'array'
                        then
                        	jsonb_insert(additional_categories::jsonb, '{0}', to_jsonb(category))
                        else      
                  			to_jsonb(array[category] || additional_categories::text)
                      end 
;

select 
	cid
	,category
	,additional_categories
	,category_ids_de
	,category_ids
from
	google_maps_dev.google_map_items_mobilität_piv
;	

--Tabelle zur Entfaltung der Kategorien erstellen:
drop table if exists  
         google_maps_dev.google_map_kateg_mobilität_piv;

create table  
        google_maps_dev.google_map_kateg_mobilität_piv
as 
select  
	*
from (
    select  
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
    from  
        google_maps_dev.google_map_items_mobilität_piv
    where  
        jsonb_typeof(category_ids_de::jsonb) = 'array'
        and  
       	jsonb_typeof(category_ids::jsonb) = 'array'
) t
where  
    category_en in (
               select  
                  category_en
               from  
                  google_maps_dev.google_map_category_hierarchy
               where  
                  hauptkategorie_neu = 'Mobilität'
                  and  
                  kategorie_neu = 'Haltestelle'
   )
;

select 
	*
from 
	google_maps_dev.google_map_kateg_mobilität_piv
;

---------------------------------------------------------------------------------------
-- Schritt 6: T5 Hinzufügen neuer Attribute und Vervollständigung der fehlenden Daten
---------------------------------------------------------------------------------------
drop table if exists  
    google_maps_dev.google_map_mobilität_piv;

create table  
    google_maps_dev.google_map_mobilität_piv 
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
    google_maps_dev.google_map_kateg_mobilität_piv
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


select 
	*
from 
	google_maps_dev.google_map_mobilität_piv 
;



--------------------------------------------------------------------------
-- Schritt 7: Normalisierung
--------------------------------------------------------------------------
--PLZ4, falls vorhanden:
-- Get plz4 from address if exists
select 
	*
from
	google_maps_dev.google_map_mobilität_piv
where 
	plz4 NOT SIMILAR TO '[0-9]{4}'
;

update google_maps_dev.google_map_mobilität_piv
set 
	plz4 = regexp_replace(address, '([^0-9]*)([0-9]{4,6})([^0-9]*)', '\2')
where 
	plz4 is null
;

--Fehlende PLZ4
select 
	t0.*
	,t1.plz
from
	google_maps_dev.google_map_mobilität_piv t0
left join
	geo_afo_prod.imp_plz6_geo_neu t1
on
	ST_Contains(t1.geo_poly_lv95 , t0.geo_point_lv95)
where 
	t0.plz4 NOT SIMILAR TO '[0-9]{4}'
	or 
	t0.plz4 is null
	or 
	t0.plz4 <> t1.plz::text
;


update google_maps_dev.google_map_mobilität_piv t0
set
	plz4 = t1.plz
from
	geo_afo_prod.imp_plz6_geo_neu t1
where  
    t0.plz4 not similar to '[0-9]{4}'
    and
    ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;


-- Hausnummer
-- Get hausenummer from address if exists
select   
    address,
    strasse,
    TRIM(REGEXP_REPLACE(address, '^([^,0-9]+).*', '\1')) AS strasse_neu,
    case  
        when
        TRIM(REGEXP_REPLACE(address, '^.*? ([0-9]+)(,| ).*$', '\1')) = plz4 
        then	
        	null	
        when
        TRIM(REGEXP_REPLACE(address, '^.*? ([0-9]+)(,| ).*$', '\1')) ~ '^[0-9]+$' 
        then
         TRIM(REGEXP_REPLACE(address, '^.*? ([0-9]+)(,| ).*$', '\1'))
        else 
           null 
    end as hausnummer_neu,
    plz4
from 
    google_maps_dev.google_map_mobilität_piv
where  
    hausnummer is null  
    and
   	not address ~ '^[0-9]{4} [A-Za-zäöüÄÖÜß\s-]+$'
;

   
-- update hotel und gastronomie Table 
update google_maps_dev.google_map_mobilität_piv
set 
hausnummer = 
        case  
	       when
	           TRIM(REGEXP_REPLACE(address, '^.*? ([0-9]+)(,| ).*$', '\1')) = plz4 
	       then	
	           null	
	       when
	          TRIM(REGEXP_REPLACE(address, '^.*? ([0-9]+)(,| ).*$', '\1')) ~ '^[0-9]+$' 
	       then
	           TRIM(REGEXP_REPLACE(address, '^.*? ([0-9]+)(,| ).*$', '\1'))
	       else 
	           null 
	end,
	strasse = TRIM(REGEXP_REPLACE(address, '^([^,0-9]+).*', '\1'))
where  
    hausnummer is null  
    and
    not address ~ '^[0-9]{4} [A-Za-zäöüÄÖÜß\s-]+$'
;


-- Hinzufügen von PLZ6
alter table
	google_maps_dev.google_map_mobilität_piv
add column 
	plz6 text
;


update
	google_maps_dev.google_map_mobilität_piv t0
set
	plz6 = t1.plz6
from
	geo_afo_prod.imp_plz6_geo_neu t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;


--Hinzufügen von Gemeinde
alter table
	google_maps_dev.google_map_mobilität_piv
add column 
	gemeinde text,
add column
	gmd_nr numeric
;

update
	google_maps_dev.google_map_mobilität_piv t0
set
	gemeinde = t1.gemeinde
	,gmd_nr = t1.gmd_nr
from
	geo_afo_prod.imp_gmd_geo_neu t1
where
	ST_Contains(t1.geo_poly_lv95, t0.geo_point_lv95)
;


--==========================================================
-- Schritt 8: Erstellung der Vergleichstabelle (T6)
--==========================================================

DROP TABLE IF EXISTS 
    google_maps_dev.google_map_abgleich_mobilität_piv;

CREATE TABLE 
    google_maps_dev.google_map_abgleich_mobilität_piv 
AS
SELECT 
    cid,
    categories_de,
    categories_en,
    title,
    address,
    strasse,
    hausnummer,
    plz4,
    plz6,
    gemeinde,
    gmd_nr,
    ort,
    domain,
    url,
    google_bewertung,
    anz_bewertungen,
    anz_fotos,
    geo_point_lv95,
    longitude,
    latitude
FROM 
    google_maps_dev.google_map_mobilität_piv
   ;



select 
	*
from 
	google_maps_dev.google_map_abgleich_mobilität_piv 
;







select 
	*
from
	geo_afo_prod.meta_poi_google_maps_category
where 
	hauptkategorie_neu = 'Mobilität'
	and 
	kategorie_neu = 'Haltestelle'
;






/*
---------------------------------------------------
-- Adressdaten-Normalisierung
---------------------------------------------------
-- Get plz4 from address if exists
select 
	*
from
	google_maps_dev.google_map_<category_name>
where 
	plz4 NOT SIMILAR TO '[0-9]{4}'
	or
	plz4 isnull
;

update google_maps_dev.google_map_<category_name>
set 
	plz4 = regexp_replace(adresse, '([^0-9]*)([0-9]{4,6})([^0-9]*)', '\2')
where 
	plz4 is null
;
*/



