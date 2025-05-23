--===============================
-- Tenz-Standorte in der Schweiz
--===============================

drop table if exists
	tenz.lay_tenz_pois_ch;

create table
	tenz.lay_tenz_pois_ch
as
select  
	*
from  
	dblink(
		'geo_database',
		$POSTGRES$
			select  	  
				cid
				,poi_typ
				,bezeichnung as company
				,adresse
				,geo_point_google as geo_point_lv95
			from  
				google_maps_dev.google_map_hotel_gastronomie_v5 
			where
				bezeichnung like '%Tenz%'
		$POSTGRES$
	) AS google_map_hotel_gastronomie_v5 (
		cid text
		,poi_typ text
		,company text
		,adresse text
		,geo_point_lv95 public.geometry --(multipolygon, 2056)
	)
;

select * from tenz.lay_tenz_pois_ch;
