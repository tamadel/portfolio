--===================================
-- Hotel and gastronomie: Adjustments 
--===================================
-- again for new version v2 I need to add a new address block (korr_ ) 
alter table google_maps_dev.google_map_hotel_gastronomie_v2
add column korr_strasse text,
add column korr_hausnum text,
add column korr_plz4 text,
add column korr_ort text
;


--------------------------------------------------------
-- Create column: Korr_plz4
--------------------------------------------------------
--Korr_plz4
update google_maps_dev.google_map_hotel_gastronomie_v2
set 
	korr_plz4 = plz
where  
	google_plz4 = plz 
;

update google_maps_dev.google_map_hotel_gastronomie_v2
set 
	korr_plz4 = google_plz4
where  
	google_plz4 <> plz 
;

select
	bezeichnung 
	,adresse 
	,korr_plz4
	,google_plz4 
	,gwr_plz4 
	,plz
	,gwr_strasse 
from
	google_maps_dev.google_map_hotel_gastronomie_v2
where 
	google_plz4 <> plz
;

select 
	*
from 
	geo_afo_prod.imp_plz6_geo_neu 
where 
	plz = '0354'
;

--------------------------------------------------------
-- Create column: Korr_Ort
--------------------------------------------------------
-- Korr_Ort
update google_maps_dev.google_map_hotel_gastronomie_v2
set 
	korr_ort = ort
where 
	google_ort = ort
	and 
	korr_ort is null 
;

update google_maps_dev.google_map_hotel_gastronomie_v2
set 
	korr_ort = google_ort
where 
	google_ort <> ort
	and 
	korr_ort is null 
;


select
	korr_ort
	,google_ort 
	,gwr_ort 
	,ort
from
	google_maps_dev.google_map_hotel_gastronomie_v2
where 
	google_plz4 <> plz
;


--------------------------------------------------------
-- Korr_Strasse: cleaning street name from google data
--------------------------------------------------------

select 
	google_strasse 
	,regexp_replace(regexp_replace(google_strasse, '[/-]', '', 'g'), '\d+', '', 'g') as google_strasse_neu
from 
	google_maps_dev.google_map_hotel_gastronomie_v2
where 
	google_strasse is not null
;

update google_maps_dev.google_map_hotel_gastronomie_v2
set 
	korr_strasse = default 
;

update google_maps_dev.google_map_hotel_gastronomie_v2
set 
	google_strasse = regexp_replace(regexp_replace(google_strasse, '[/-]', '', 'g'), '\d+', '', 'g') 
;
 
--update google_maps_dev.google_map_hotel_gastronomie_v2
--set 
	--google_strasse = trim(google_strasse) 
--;
--======================================================================
-- Strasse - Part(1)
select 
	adresse
	,google_strasse
	,gwr_strasse
	,gwr_strasse_std
from
	google_maps_dev.google_map_hotel_gastronomie_v2
where 
	trim(google_strasse) = gwr_strasse
;

update google_maps_dev.google_map_hotel_gastronomie_v2
set 
	korr_strasse = gwr_strasse 
where 
	trim(google_strasse) = gwr_strasse 
; 

--============================================================================
-- Strasse - part(2)
select 
	korr_strasse 
	,adresse
	,google_strasse
	,gwr_strasse
	,gwr_strasse_std
from
	google_maps_dev.google_map_hotel_gastronomie_v2
where 
	lower(trim(google_strasse)) = gwr_strasse_std
	and 
	korr_strasse is null
;

update google_maps_dev.google_map_hotel_gastronomie_v2
set 
	korr_strasse = gwr_strasse 
where 
	lower(trim(google_strasse)) = gwr_strasse_std
	and 
	korr_strasse is null
; 

--==============================================================================
-- Strasse - part(3)
select 
	count(*)
from
	google_maps_dev.google_map_hotel_gastronomie_v2
where
	korr_strasse is null --15727
	google_strasse is null  -- 738
;

alter table google_maps_dev.google_map_hotel_gastronomie_v2
add column street_name text;

update google_maps_dev.google_map_hotel_gastronomie_v2
set street_name = trim(case   
					    when length(adresse) - length(replace(adresse, ',', '')) = 3 then regexp_replace(split_part(adresse, ',', 3), '\d+', '')
					    when length(adresse) - length(replace(adresse, ',', '')) = 2 then regexp_replace(split_part(adresse, ',', 2), '\d+', '')
					    when length(adresse) - length(replace(adresse, ',', '')) = 1 then regexp_replace(split_part(adresse, ',', 1), '\d+', '')
					    else regexp_replace(adresse, '\d+', '') 
					 end)
;


select   
    google_strasse,
    gwr_strasse
from   
    google_maps_dev.google_map_hotel_gastronomie_v2
where  
	korr_strasse is null
	and
    lower(trim(street_name)) <> lower(trim(google_strasse))
    and (
        lower(trim(street_name)) = lower(trim(gwr_strasse_std))
        or lower(trim(street_name)) = lower(trim(gwr_strasse))
    );
   

update google_maps_dev.google_map_hotel_gastronomie_v2
set 
	korr_strasse = gwr_strasse 
where 
	korr_strasse is null
	and
    lower(trim(street_name)) <> lower(trim(google_strasse))
    and (
        lower(trim(street_name)) = lower(trim(gwr_strasse_std))
        or 
        lower(trim(street_name)) = lower(trim(gwr_strasse))
        )
;


select 
	korr_strasse 
	,adresse 
	,street_name
	,google_strasse 
	,gwr_strasse 
	,gwr_strasse_std 
from 
	google_maps_dev.google_map_hotel_gastronomie_v2
where 
	korr_strasse is null
	and(
	lower(trim(street_name)) = lower(trim(gwr_strasse))
	or 
	lower(trim(street_name)) = lower(trim(gwr_strasse_std))
	or 
	lower(trim(google_strasse)) = lower(trim(gwr_strasse))
	or
	lower(trim(google_strasse)) = lower(trim(gwr_strasse_std))
	)
;

	
update google_maps_dev.google_map_hotel_gastronomie_v2
set 
	korr_strasse = gwr_strasse 
where 
	korr_strasse is null
	and(
	lower(trim(street_name)) = lower(trim(gwr_strasse))
	or 
	lower(trim(street_name)) = lower(trim(gwr_strasse_std))
	or 
	lower(trim(google_strasse)) = lower(trim(gwr_strasse))
	or
	lower(trim(google_strasse)) = lower(trim(gwr_strasse_std))
	)
;



--=======================================================================================
-- Strasse - part(4) take google_strasee into korr_strasse

select 
	korr_strasse 
	,adresse 
	,trim(case   
	    when length(adresse) - length(replace(adresse, ',', '')) = 3 then regexp_replace(split_part(adresse, ',', 3))--, '\d+', '')
	    when length(adresse) - length(replace(adresse, ',', '')) = 2 then regexp_replace(split_part(adresse, ',', 2))--, '\d+', '')
	    when length(adresse) - length(replace(adresse, ',', '')) = 1 then regexp_replace(split_part(adresse, ',', 1))--, '\d+', '')
	    else adresse 
	 end) as street_name
	,google_strasse 
	,gwr_strasse 
	,gwr_strasse_std 
from
	google_maps_dev.google_map_hotel_gastronomie_v2
where 
	korr_strasse is null
	and 
	google_strasse = street_name
;


update google_maps_dev.google_map_hotel_gastronomie_v2
set 
	korr_strasse = google_strasse 
where 
	korr_strasse is null
	and 
	google_strasse = street_name
;


--========================================================================================
-- Strasse - part(5) google_strasse is not clean
select 
	korr_strasse 
	,adresse 
	,street_name
	,google_strasse 
	,gwr_strasse 
	,gwr_strasse_std 
from
	google_maps_dev.google_map_hotel_gastronomie_v2
where 
	korr_strasse is null
	and 
	google_strasse <> street_name
	and 
	street_name ~ gwr_strasse 
;

update google_maps_dev.google_map_hotel_gastronomie_v2
set 
	korr_strasse = gwr_strasse 
where 
	korr_strasse is null
	and 
	google_strasse <> street_name
	and 
	street_name ~ gwr_strasse
; 



-- cleaning google_strasse

select 
	korr_strasse 
	,adresse 
	,regexp_replace(street_name, '\d+', '', 'g') AS street_name
	,google_strasse
	,gwr_strasse 
	,gwr_strasse_std 
from
	google_maps_dev.google_map_hotel_gastronomie_v2
where 
	korr_strasse is null 
	and
	(
	google_strasse ILIKE '%strasse%'
    OR google_strasse ILIKE '%Rue%'
    OR google_strasse ILIKE '%Av. %'
    OR google_strasse ILIKE '%Avenue%'
    OR google_strasse ILIKE '%Chem. des%'
    OR google_strasse ILIKE '%Chemin des%'
    OR google_strasse ILIKE '%weg%'
    )
    and(
    street_name ILIKE '%strasse%'
    OR street_name ILIKE '%Rue%'
    OR street_name ILIKE '%Av. %'
    OR street_name ILIKE '%Avenue%'
    OR street_name ILIKE '%Chem. des%'
    OR street_name ILIKE '%Chemin des%'
    OR street_name ILIKE '%weg%'
    ) 
;


update google_maps_dev.google_map_hotel_gastronomie_v2 t0
set 
	korr_strasse = google_strasse 
where 
	korr_strasse is null 
	and
	(
	google_strasse ILIKE '%strasse%'
    OR google_strasse ILIKE '%Rue%'
    OR google_strasse ILIKE '%Av. %'
    OR google_strasse ILIKE '%Avenue%'
    OR google_strasse ILIKE '%Chem. des%'
    OR google_strasse ILIKE '%Chemin des%'
    OR google_strasse ILIKE '%weg%'
    )
    and(
    street_name ILIKE '%strasse%'
    OR street_name ILIKE '%Rue%'
    OR street_name ILIKE '%Av. %'
    OR street_name ILIKE '%Avenue%'
    OR street_name ILIKE '%Chem. des%'
    OR street_name ILIKE '%Chemin des%'
    OR street_name ILIKE '%weg%'
    ) 
;


update google_maps_dev.google_map_hotel_gastronomie_v2 t0
set 
	korr_strasse = google_strasse
where 
	korr_strasse is null
	and 
	google_strasse is not null 
	and 
	google_strasse like '%Chem.%'
;	



update google_maps_dev.google_map_hotel_gastronomie_v2 t0
set 
	korr_strasse = google_strasse
where 
	korr_strasse is null
	and 
	google_strasse is not null 
	and 
	google_strasse like '%Rte de%'
;



update google_maps_dev.google_map_hotel_gastronomie_v2 t0
set 
	korr_strasse = google_strasse
where 
	korr_strasse is null
	and 
	google_strasse is not null 
	and 
	google_strasse like '%Pl.%'
;


update google_maps_dev.google_map_hotel_gastronomie_v2 t0
set 
	korr_strasse = google_strasse
where 
	korr_strasse is null
	and 
	google_strasse is not null 
	and 
	google_strasse like '%Chemin%'
;


update google_maps_dev.google_map_hotel_gastronomie_v2 t0
set 
	korr_strasse = google_strasse
where 
	korr_strasse is null
	and 
	google_strasse is not null 
	and 
	google_strasse like '%Rte%'
;

update google_maps_dev.google_map_hotel_gastronomie_v2 t0
set 
	korr_strasse = google_strasse
where 
	korr_strasse is null
	and 
	google_strasse is not null 
	and(
	google_strasse like '%Les %'
	or 
	google_strasse like '%Le %'
	or 
	google_strasse like '%La %'
	)
;

update google_maps_dev.google_map_hotel_gastronomie_v2 t0
set 
	korr_strasse = google_strasse
where 
	korr_strasse is null
	and 
	google_strasse is not null 
	and 
	google_strasse like '%Prom.%'
;


update google_maps_dev.google_map_hotel_gastronomie_v2 t0
set 
	korr_strasse = google_strasse
where 
	korr_strasse is null
	and 
	google_strasse is not null 
	and 
	google_strasse like '%Via%'
;


update google_maps_dev.google_map_hotel_gastronomie_v2 t0
set 
	korr_strasse = google_strasse
where 
	korr_strasse is null
	and 
	google_strasse is not null 
	and 
	google_strasse like '%Esp%'
;

update google_maps_dev.google_map_hotel_gastronomie_v2 t0
set 
	korr_strasse = google_strasse
where 
	korr_strasse is null
	and 
	google_strasse is not null 
	and 
	google_strasse like '%Rue%'
;

update google_maps_dev.google_map_hotel_gastronomie_v2 t0
set 
	korr_strasse = google_strasse
where 
	korr_strasse is null
	and 
	google_strasse is not null 
	and 
	google_strasse like '%Quai%'
;


update google_maps_dev.google_map_hotel_gastronomie_v2 t0
set 
	korr_strasse = google_strasse
where 
	korr_strasse is null
	and 
	google_strasse is not null 
	and 
	google_strasse like '%Quai%'
;


--test

select 
	korr_strasse
	,adresse
	,street_name
	,google_strasse
	,gwr_strasse
	,gwr_strasse_std 
from
	google_maps_dev.google_map_hotel_gastronomie_v2
where 
	korr_strasse is null
	and 
	google_strasse is not null 
	and(
	google_strasse like '%Route%'
	or 
	street_name like '%Route%'
	)
;


select 
	korr_strasse 
	,adresse 
	,regexp_replace(street_name, '\d+', '', 'g') AS street_name
	,google_strasse
	,gwr_strasse 
	,gwr_strasse_std 
from
	google_maps_dev.google_map_hotel_gastronomie_v2
where 
	google_strasse ~ gwr_strasse 
	and 
	korr_strasse is null
;


select 
	*
from 
	google_maps_dev.google_map_hotel_gastronomie_v2
;

--------------------------------------------------------
-- Create column: Korr_hausnum
--------------------------------------------------------

--Korr_hausnum we are going to take gwr_haunum in casse google_hausnum is null or worng(instead of hausnum is plz4) 
select
	korr_hausnum
	,adresse 
	,google_strasse
	,gwr_strasse
	,gwr_strasse_std
	,google_hausnum 
	,gwr_hausnum 
from
	google_maps_dev.google_map_hotel_gastronomie_v2
where 
	(google_strasse = gwr_strasse or lower(google_strasse) ~ gwr_strasse_std)
	and 
	(google_hausnum is null or google_hausnum = google_plz4)
;


update google_maps_dev.google_map_hotel_gastronomie_v2
set 
	korr_hausnum = gwr_hausnum
where  
	(google_strasse = gwr_strasse or lower(google_strasse) ~ gwr_strasse_std)
	and 
	(google_hausnum is null or google_hausnum = google_plz4)
;


select
	korr_hausnum
	,adresse 
	,split_part(adresse, ',', 2) AS street_name
	,google_strasse
	,gwr_strasse
	,gwr_strasse_std
	,google_hausnum 
	,gwr_hausnum 
from
	google_maps_dev.google_map_hotel_gastronomie_v2
where 
	(korr_hausnum is null)
	and 
	(gwr_strasse is not null) 
	and 
	(gwr_hausnum is not null)
	and 
	(lower(split_part(adresse, ',', 2)) ~ gwr_strasse_std) 
;


update google_maps_dev.google_map_hotel_gastronomie_v2
set 
	korr_hausnum = google_hausnum
where  
	korr_hausnum is null
	and 
	google_hausnum <> google_plz4 
;


update google_maps_dev.google_map_hotel_gastronomie_v2
set 
	korr_hausnum = gwr_hausnum
where  
	(korr_hausnum is null)
	and 
	(gwr_strasse is not null) 
	and 
	(gwr_hausnum is not null)
	and 
	(lower(split_part(adresse, ',', 2)) ~ gwr_strasse_std) 
;

select 
	*
from 
	--"temp".tmp_hotel_gastro_v2 
	google_maps_dev.google_map_hotel_gastronomie_v2
where 
	korr_strasse = gwr_strasse 
	and 
	korr_hausnum is null
	and 
	gwr_hausnum is not null
;


update google_maps_dev.google_map_hotel_gastronomie_v2
set korr_hausnum = gwr_hausnum 
where 
	korr_strasse = gwr_strasse 
	and 
	korr_hausnum is null
	and 
	gwr_hausnum is not null
;


--//////////////////////////////////////////////////////////////////////////////////////////////
drop table if exists temp.tmp_hotel_gastro_v2;

create table temp.tmp_hotel_gastro_v2 
as
select 
	'''' || cid as cid -- Add a single quote (') to the beginning of the string to ensure that the format in the CSV file is not changed. This is a temporary solution.  
	,bezeichnung 
	,category_ids_en 
	,category_ids_de 
	,poi_typ 
	,kategorie 
	,hauptkategorie 
	,korr_strasse
	,korr_hausnum
	,korr_plz4
	,korr_ort
	,'''' || telefon as telefon 
	,adresse 
	,google_strasse 
	,google_hausnum 
	,google_plz4 
	,google_ort 
	,gwr_strasse
	,gwr_strasse_std
	,gwr_hausnum 
	,gwr_plz4 
	,gwr_ort 
	,distance
	,plz6
	,plz
	,ort
	,gemeinde 
	,gmd_nr
	,url 
	,"domain" 
	,anz_fotos 
	,google_bewertung 
	,anz_bewertungen 
	,relevant 
	,status 
	,opening_times 
	,geo_point_google
	,geo_point_gwr
from 
	google_maps_dev.google_map_hotel_gastronomie_v2
;

--=====================================
-- Abgleich Tabelle 
--=====================================

-- create table for Comparison with our AFO POI 
drop table if exists google_maps_dev_abgleich.google_hotel_gastro;
create table google_maps_dev_abgleich.google_hotel_gastro
as
select
	 cid as cid 
	,trim(coalesce(korr_strasse, ' ')||' '|| coalesce(korr_hausnum, '')) as strasse
	--,korr_hausnum
	,korr_plz4::numeric as plz4
	,korr_ort as ort
	,adresse as address
	,bezeichnung as title
	,google_strasse 
	,google_hausnum
	,google_plz4
	,google_ort
	,gwr_strasse 
	,gwr_hausnum 
	,gwr_plz4 
	,gwr_ort 
	,plz6
	,gmd_nr
	,gemeinde
	,"domain" 
	,url 
	,trim(REPLACE(REPLACE(poi_typ, '[', ''), ']', '')) as google_poi_typ
	,status
	,geo_point_google as geo_point_lv95 
from 
	google_maps_dev.google_map_hotel_gastronomie_v2
;


select 
	*
from 
	google_maps_dev_abgleich.google_hotel_gastro
;
	
	
	--5983899395991930672
	
	
	
select
	*
	--count(*)
from 
	"temp".tmp_hotel_gastro_v2
	--google_maps_dev.google_map_hotel_gastronomie_v2
where 
	--korr_strasse  is null -- = 925
	google_strasse is null -- = 738
	-- korr_hausnum is null -- = 2'599
	--korr_plz4 is null -- = 3
	--and
	--korr_ort  is null -- = 3
;



select 
	*
from 
	temp.tmp_hotel_gastro_v2
where 
	--adresse = 'Madrisa-Zügenhüttli, Klosters-Madrisa Bergbahnen AG, Madrisastrasse 7, 7252 Klosters Dorf'
	korr_hausnum is null
;


select 
	*
from 
	geo_afo_prod.imp_plz6_geo_neu 
where 
	plz in (8238, 6911)
;


select 
	*
from 
	geo_afo_prod.mv_lay_plz4_aktuell 
where 
	plz4 in (8238, 6911)
;

--============================
-- Simon

SELECT address, public.search_leerschlag(address,1,',')
FROM google_maps_dev_abgleich.google_hotel_gastro
where REGEXP_COUNT(address,',',1)=2
LIMIT 100;























