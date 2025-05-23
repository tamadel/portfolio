
drop table if exists
	tenz.lay_hkt100_lausanne;

create table
	tenz.lay_hkt100_lausanne
as
SELECT 
	*
FROM 
	dblink(
		'geo_database',
		$POSTGRES$
			SELECT 	  
				hkt.gid
				,hkt.reli
				,hkt.arbeitsstaetten_tot
				,hkt.besch_tot
				,hkt.geo_poly_lv95
			FROM 
				geo_afo_prod.mv_lay_hkt100_aktuell hkt
			join (
				select
					*
				FROM 
					geo_afo_prod.mv_lay_gmd_aktuell 
				where
					-- Agglo Lausanne
					agglo_nr_2012 = 5586
			) gmd
			on
				st_within( hkt.geo_poly_lv95, gmd.geo_poly_lv95 )
		$POSTGRES$
	) AS mv_lay_poi_aktuell (
		gid int
		,reli int
		,arbeitsstaetten_tot int
		,besch_tot int
		,geo_poly_lv95 public.geometry(multipolygon, 2056)
	)
;	

select 
	*
from
	tenz.lay_hkt100_lausanne
;
