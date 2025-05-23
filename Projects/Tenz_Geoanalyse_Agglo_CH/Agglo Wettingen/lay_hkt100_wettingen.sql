
drop table if exists
	tenz.lay_hkt100_wettingen;

create table
	tenz.lay_hkt100_wettingen
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
				,hkt.pers_tot
				,ntile(10) over (
					order by
						hkt.pers_tot
				) as pers_tot_dec
				,hkt.arbeitsstaetten_tot
				,ntile(10) over (
					order by
						hkt.arbeitsstaetten_tot
				) as arbeitsstaetten_tot_dec
				,hkt.besch_tot
				,ntile(10) over (
					order by
						hkt.besch_tot
				) as besch_tot_dec
				,(hkt.besch_f_s3 + hkt.besch_m_s3) besch_s3
				,ntile(10) over (
					order by
						(hkt.besch_f_s3 + hkt.besch_m_s3)
				) as besch_s3_dec
				,hkt.geo_poly_lv95
			FROM 
				geo_afo_prod.mv_lay_hkt100_aktuell hkt
			join (
				select
					*
				FROM 
					geo_afo_prod.mv_lay_gmd_aktuell 
				WHERE
					-- Agglo Wettingen
					agglo_nr_2012 = 4021
			) gmd
			on
				st_within( hkt.geo_poly_lv95, gmd.geo_poly_lv95 )
		$POSTGRES$
	) AS mv_lay_poi_aktuell (
		gid int
		,reli int
		,pers_tot float
		,pers_tot_dec int
		,arbeitsstaetten_tot float
		,arbeitsstaetten_tot_dec int
		,besch_tot float
		,besch_tot_dec int
		,besch_s3 float
		,besch_s3_dec int
		,geo_poly_lv95 public.geometry(multipolygon, 2056)
	)
;	

select 
	*
from
	tenz.lay_hkt100_wettingen
;







