
DROP table if exists
	tenz.lay_agglo_ch;

create table
	tenz.lay_agglo_ch
as
SELECT 
	*
FROM 
	dblink(
		'geo_database',
		$POSTGRES$
			select 
				t0.agglo_nr_2012 
				,t0.gmd_nr 
				,'Agglo ' || t0.gemeinde as agglo_desc
				,t2.anz_prs_tot
				,case
					-- falls Agglo Zürich und Tessin: Wert auf 0 setzen
					when t0.agglo_nr_2012 in (261, 5113, 5002, 5192, 5250) then 0
					else
						ntile(10) over (
							order by
								t2.anz_prs_tot
						)
				end as anz_prs_tot_dec
				,t2.anz_betriebe_tot
				,case
					-- falls Agglo Zürich und Tessin: Wert auf 0 setzen
					when t0.agglo_nr_2012 in (261, 5113, 5002, 5192, 5250) then 0
					else
						ntile(10) over (
							order by
								t2.anz_betriebe_tot
						)
				end as anz_betriebe_tot_dec
				,t2.anz_beschaeftigte_tot
				,case
					-- falls Agglo Zürich und Tessin: Wert auf 0 setzen
					when t0.agglo_nr_2012 in (261, 5113, 5002, 5192, 5250) then 0
					else
						ntile(10) over (
							order by
								t2.anz_beschaeftigte_tot
						)
				end as anz_beschaeftigte_tot_dec
				,t2.geo_poly_lv95
			from
				geo_afo_prod.mv_lay_gmd_aktuell t0
			join (
				select
					agglo_nr_2012 
					,max(anz_prs) max_anz_prs
				from
					geo_afo_prod.mv_lay_gmd_aktuell
				where
					coalesce(agglo_nr_2012,0) <> 0
				group by
					agglo_nr_2012
			) t1
			on
				t0.agglo_nr_2012 = t1.agglo_nr_2012
				and
				t0.anz_prs = t1.max_anz_prs
			join (
				select
					agglo_nr_2012
					,sum(anz_prs) as anz_prs_tot
					,sum(anz_betriebe_tot) as anz_betriebe_tot
					,sum(anz_beschaeftigte_tot) as anz_beschaeftigte_tot
					,st_union(geo_poly_lv95) as geo_poly_lv95
				from
					geo_afo_prod.mv_lay_gmd_aktuell
				where
					coalesce(agglo_nr_2012,0) <> 0
				group by
					agglo_nr_2012
			) t2
			on
				t1.agglo_nr_2012 = t2.agglo_nr_2012
		$POSTGRES$
	) AS mv_lay_gmd_aktuell (
		agglo_nr_2012 int
		,gmd_nr int
		,agglo_desc text
		,anz_prs_tot int
		,anz_prs_tot_dec int
		,anz_betriebe_tot int
		,anz_betriebe_tot_dec int
		,anz_beschaeftigte_tot int
		,anz_beschaeftigte_tot_dec int
		,geo_poly_lv95 public.geometry(multipolygon, 2056)
	)
;

select 
	*
from
	tenz.lay_agglo_ch
;
