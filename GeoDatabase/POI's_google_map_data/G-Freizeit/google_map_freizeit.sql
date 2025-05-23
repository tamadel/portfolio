--=============================
-- Hauptkategorie = 'Freizeit'
-- 02.11.2024
--============================

select
	*
from  
	geo_afo_prod.meta_poi_google_maps_category
where 
	hauptkategorie_neu = 'Freizeit'
	and 
	kategorie_neu in ('Sportanlage', 'Eisanlagen', 'Fitnessanlagen', 'Campingplatz','Golfanlage')
;
	
	
	
--Part(2) >> ('Freizeitanlage', 'Gartenanlage', 'Kinderanlagen', 'Tieranlagen', 'Stadion/Arena') 
--Part(3) >> ('Kino & Theater', 'Museum', 'Kultureinrichtungen', 'Event' )
	


update 
	geo_afo_prod.meta_poi_google_maps_category
set next_run_date = current_date
where hauptkategorie_neu = 'Freizeit'
; 



-- Metadata
select 
	*
from 
	google_maps_dev.google_map_metadata_freizeit
;



ALTER TABLE 
	google_maps_dev.google_map_metadata_freizeit
ADD COLUMN IF NOT EXISTS datetime DATE,
ADD COLUMN IF NOT EXISTS item_type TEXT,
ADD COLUMN IF NOT EXISTS n_result NUMERIC,
ADD COLUMN IF NOT EXISTS status_message TEXT,
ADD COLUMN IF NOT EXISTS status_code TEXT
;



--=============================================================================
-- Part(1) >> 
--('Sportanlage', 'Eisanlagen', 'Fitnessanlagen', 'Campingplatz','Golfanlage')
--=============================================================================

-- Metadata
select
	*
from 
	google_maps_dev.google_map_metadata_freizeit
where 
	hauptkategorie_neu = 'Freizeit'
	and 
	kategorie_neu in ('Sportanlage', 'Eisanlagen', 'Fitnessanlagen', 'Campingplatz','Golfanlage')
	and 
	n_result is null
; 



-- Results
select 
	*
from 
	google_maps_dev.google_map_results_freizeit_pi
;



-- items
select 
	*
from 
	google_maps_dev.google_map_items_freizeit_pi
;	




--=============================================================================
-- Part(2) >> 
--('Freizeitanlage', 'Gartenanlage', 'Kinderanlagen', 'Tieranlagen', 'Stadion/Arena') 
--=============================================================================

-- Metadata
select
	*
from 
	google_maps_dev.google_map_metadata_freizeit
where 
	hauptkategorie_neu = 'Freizeit'
	and 
	kategorie_neu in ('Freizeitanlage', 'Gartenanlage', 'Kinderanlagen', 'Tieranlagen', 'Stadion/Arena') 
	and 
	n_result is null
; 



-- Results
select 
	*
from 
	google_maps_dev.google_map_results_freizeit_pii
;



-- items
select 
	*
from 
	google_maps_dev.google_map_items_freizeit_pii
;





--=============================================================================
-- Part(3) >> 
--('Kino & Theater', 'Museum', 'Kultureinrichtungen', 'Event' ) 
--=============================================================================

-- Metadata
select
	*
from 
	google_maps_dev.google_map_metadata_freizeit
where 
	hauptkategorie_neu = 'Freizeit'
	and 
	kategorie_neu in ('Kino & Theater', 'Museum', 'Kultureinrichtungen', 'Event' )
	and 
	n_result is null
;



-- Results
select 
	*
from 
	google_maps_dev.google_map_results_freizeit_piii
;



-- items
select 
	*
from 
	google_maps_dev.google_map_items_freizeit_piii
;





-----------------------------------






