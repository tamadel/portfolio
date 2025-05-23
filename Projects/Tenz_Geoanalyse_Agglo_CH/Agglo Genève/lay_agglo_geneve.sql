--=========================================================
--Projektname: Standortanalyse für Tenz (Gastronomie)
--Projektnummer: 512801
--script: lay_agglo_geneve
--Datum: 10.01.2025
--Tamer Adel
--=========================================================

---------------------
-- Agglo geneve
---------------------
drop table if exists
	tenz.lay_agglo_geneve;

create table
	tenz.lay_agglo_geneve
as
SELECT 
	*
FROM 
	dblink(
		'geo_database',
		$POSTGRES$
			SELECT 	  
				gid
				,gmd_nr
				,gemeinde
				,geo_poly_lv95
			FROM 
				geo_afo_prod.mv_lay_gmd_aktuell 
			where
				agglo_nr_2012 = 6621 --geneve
		$POSTGRES$
	) AS mv_lay_gmd_aktuell (
		gid int
		,gmd_nr numeric
		,gemeinde text
		,geo_poly_lv95 public.geometry(multipolygon, 2056)
	)
;

select 
	*
from
	tenz.lay_agglo_geneve
;

---------------------
-- Poi's geneve
---------------------
select * from tenz.lay_poi_attraktor;

drop table
	tenz.lay_poi_attraktor_geneve;

create table
	tenz.lay_poi_attraktor_geneve
as
select
	a.*
from
	tenz.lay_poi_attraktor a
join
	tenz.lay_agglo_geneve b
on
	st_within( a.geo_point_lv95, b.geo_poly_lv95 )
;

-- Data ansicht filiter in QGIS 
select 
	*
from
	tenz.lay_poi_attraktor_geneve
where 
	poi_typ_id in (
				207 						-- Food j und 
				,506 						-- Non-Food j
				,504						-- Einkaufszentrum j
				,505						-- Multimedia/Haushalt/Software j
				,508						-- Non Food/Warenhaus j
				,1204						-- Sekundarstufe 1 j
				,1205						-- Sekundarstufe 2 j
				,1206						-- Tertiäre Stufe j
				,1207	
	)
;



-------------------------
-- Strassenfrequenzen
-------------------------
select * from tenz.lay_str_freq; 

drop table
	tenz.lay_str_freq_geneve;

create table
	tenz.lay_str_freq_geneve
as
select
	a.*
from
	tenz.lay_str_freq a
join
	tenz.lay_agglo_geneve b
on
	st_within( a.geo_line_lv95, b.geo_poly_lv95 )
;


select * from tenz.lay_str_freq_geneve;


-------------------------
-- Bahnhöfe
-------------------------
select * from tenz.lay_bahnhof;

drop table
	tenz.lay_bahnhof_geneve;

create table
	tenz.lay_bahnhof_geneve
as
select
	a.*
from
	tenz.lay_bahnhof a
join
	tenz.lay_agglo_geneve b
on
	st_within( a.geo_point_lv95, b.geo_poly_lv95 )
;

select * from tenz.lay_bahnhof_geneve;


---------------------
-- HKT100 geneve
---------------------
drop table if exists
	tenz.lay_hkt100_geneve;

create table
	tenz.lay_hkt100_geneve
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
					-- Agglo geneve
					agglo_nr_2012 = 6621
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
	tenz.lay_hkt100_geneve
;



























