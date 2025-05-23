--=======================================================
-- Project: import GMD aktuell 2025   
-- Date: 12.02.2025
-- Tamer Adel 
--=======================================================
-- original dataset from Swisstopo 
select 
	*
from 
	public.swissboundaries3d_1_5_tlm_hoheitsgebiet;
   

-- clean up the original table and creat a new table 
drop table if exists 
		geo_afo_prod.imp_gmd_geo_01_2025;
create table 
		geo_afo_prod.imp_gmd_geo_01_2025
as
select
    gid 
	,datum_aend 
	,datum_erst 
	,herkunft_j 
	,objektart 															as objekt_art
	,bfs_nummer 														as gmd_nr
	,bezirksnum 														as bzr_nr
	,kantonsnum 														as kanton_nr
	,"name" 															as gemeinde
	,gem_flaech 
	,see_flaech 
	,icc 
	,einwohnerz 
	,hist_nr
    ,ST_Force2D(ST_SetSRID(ST_Multi(geom), 2056)) 						as geo_poly_lv95
    ,ST_Transform(ST_SetSRID(ST_Force2D(ST_Multi(geom)), 2056), 21781) 	as geo_poly_lv03  
    ,ST_Transform(ST_SetSRID(ST_Force2D(ST_Multi(geom)), 2056), 4326) 	as geo_poly_wgs84
from 
   public.swissboundaries3d_1_5_tlm_hoheitsgebiet
where 
    icc in ('CH', 'LI')
    and
    objektart in ('Gemeindegebiet')
;
   
   

select 
	gmd_nr
	,gemeinde 
from 
	geo_afo_prod.imp_gmd_geo_01_2025
where 
	gmd_nr not in(
					select 
						gmd_nr 
					from
						geo_afo_prod.imp_gmd_geo_neu 
	)
;


-- GMD-Mutation von 01.01.2024 bis 01.01.2025
--6513	Laténa
--2239	Grolley-Ponthaux




