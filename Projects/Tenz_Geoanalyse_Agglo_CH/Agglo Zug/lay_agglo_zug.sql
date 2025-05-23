--=========================================================
--Projektname: Standortanalyse für Tenz (Gastronomie)
--Projektnummer: 512801
--script: lay_agglo_Zug
--Datum: 10.01.2025
--Tamer Adel
--=========================================================

---------------------
-- Agglo Zug
---------------------
drop table if exists
	tenz.lay_agglo_zug;

create table
	tenz.lay_agglo_zug
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
				agglo_nr_2012 = 1711 --zug
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
	tenz.lay_agglo_zug
;

---------------------
-- Poi's zug
---------------------
select * from tenz.lay_poi_attraktor;

drop table
	tenz.lay_poi_attraktor_zug;

create table
	tenz.lay_poi_attraktor_zug
as
select
	a.*
from
	tenz.lay_poi_attraktor a
join
	tenz.lay_agglo_zug b
on
	st_within( a.geo_point_lv95, b.geo_poly_lv95 )
;

-- Data ansicht filiter in QGIS 
select 
	*
from
	tenz.lay_poi_attraktor_zug
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


-----------------------
-- Konkurrenz zug
-----------------------
/*select 
	* 
from 
	geo_afo_prod.mv_lay_poi_aktuell 
where 
	poi_typ_id in (
					201   -- Bäckerei
					,1003 --Café
					,202  --Convenience
					,1004 --Fastfood
					,207  --Food
	)
; 


drop table
	tenz.lay_poi_attrkonkur_zug;

create table
	tenz.lay_poi_attrkonkur_zug
as
select
	a.*
from
	tenz.lay_poi_attrkonkur a
join
	tenz.lay_agglo_zug b
on
	st_within( a.geo_point_lv95, b.geo_poly_lv95 )
;
*/

-------------------------
-- Strassenfrequenzen
-------------------------
select * from tenz.lay_str_freq; 

drop table
	tenz.lay_str_freq_zug;

create table
	tenz.lay_str_freq_zug
as
select
	a.*
from
	tenz.lay_str_freq a
join
	tenz.lay_agglo_zug b
on
	st_within( a.geo_line_lv95, b.geo_poly_lv95 )
;


select * from tenz.lay_str_freq_zug;


-------------------------
-- Bahnhöfe
-------------------------
select * from tenz.lay_bahnhof;

drop table
	tenz.lay_bahnhof_zug;

create table
	tenz.lay_bahnhof_zug
as
select
	a.*
from
	tenz.lay_bahnhof a
join
	tenz.lay_agglo_zug b
on
	st_within( a.geo_point_lv95, b.geo_poly_lv95 )
;

select * from tenz.lay_bahnhof_zug;


---------------------
-- HKT100 zug
---------------------
drop table if exists
	tenz.lay_hkt100_zug;

create table
	tenz.lay_hkt100_zug
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
					-- Agglo zug
					agglo_nr_2012 = 1711
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
	tenz.lay_hkt100_zug
;



/*
---------------------
-- Zielgruppenanalyse
---------------------
drop table if exists 
	tenz.lay_anz_ziel_prs_zug;
create table 
	tenz.lay_anz_ziel_prs_zug
as
select
	gid
	,anz_prs 
	,altkls_15_19
	,altkls_20_24
	,altkls_25_29
	,altkls_30_34
	,altkls_35_39
	,altkls_1  --Anzahl Personen - Alterklasse 1 - 0-19 Jahre	
	,altkls_2  --Anzahl Personen - Alterklasse 2 - 20-39 Jahre
	,geo_poly_lv95 
from  
	geo_afo_prod.mv_lay_gmd_aktuell 
where 
	-- Agglo zug
	agglo_nr_2012 = 1711
;

select * from tenz.lay_anz_ziel_prs_zug;



drop table if exists 
	tenz.lay_hkt100_anz_ziel_prs_zug;
create table 
	tenz.lay_hkt100_anz_ziel_prs_zug
as	
select 
	hkt.gid
	,hkt.reli
	,hkt.pers_tot
	,round(hkt.m_15_19 + hkt.w_15_19) 								as wohnbev_15_19
	,round(hkt.m_20_24 + hkt.w_20_24) 								as wohnbev_20_24
	,round(hkt.m_25_29 + hkt.w_25_29) 								as wohnbev_25_29
	,round(hkt.m_30_34 + hkt.w_30_34) 								as wohnbev_30_34
	,hkt.geo_poly_lv95 
from
	geo_afo_prod.mv_lay_hkt100_aktuell hkt
join (
	select
		*
	from  
		geo_afo_prod.mv_lay_gmd_aktuell 
	where 
		-- Agglo zug
		agglo_nr_2012 = 1711
) gmd
on
	st_within( hkt.geo_poly_lv95, gmd.geo_poly_lv95 )
;

*/























