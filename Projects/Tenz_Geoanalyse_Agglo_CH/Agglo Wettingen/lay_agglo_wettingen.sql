--=========================================================
--Projektname: Standortanalyse für Tenz (Gastronomie)
--Projektnummer: 512801
--script: lay_agglo_wettingen 
--Datum: 06.01.2025
--Tamer Adel
--=========================================================

---------------------
-- Agglo Wettingen
---------------------
drop table if exists
	tenz.lay_agglo_wettingen;

create table
	tenz.lay_agglo_wettingen
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
				agglo_nr_2012 = 4021
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
	tenz.lay_agglo_wettingen
;

---------------------
-- Poi's Wettingen
---------------------
select * from tenz.lay_poi_attraktor;

drop table
	tenz.lay_poi_attraktor_wettingen;

create table
	tenz.lay_poi_attraktor_wettingen
as
select
	a.*
from
	tenz.lay_poi_attraktor a
join
	tenz.lay_agglo_wettingen b
on
	st_within( a.geo_point_lv95, b.geo_poly_lv95 )
;

-- Data ansicht filiter in QGIS 
select 
	*
from
	tenz.lay_poi_attraktor_wettingen
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
-- Konkurrenz Wettingen
-----------------------
select 
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
	tenz.lay_poi_attrkonkur_wettingen;

create table
	tenz.lay_poi_attrkonkur_wettingen
as
select
	a.*
from
	tenz.lay_poi_attrkonkur a
join
	tenz.lay_agglo_wettingen b
on
	st_within( a.geo_point_lv95, b.geo_poly_lv95 )
;


-------------------------
-- Strassenfrequenzen
-------------------------
select * from tenz.lay_str_freq; 

drop table
	tenz.lay_str_freq_wettingen;

create table
	tenz.lay_str_freq_wettingen
as
select
	a.*
from
	tenz.lay_str_freq a
join
	tenz.lay_agglo_wettingen b
on
	st_within( a.geo_line_lv95, b.geo_poly_lv95 )
;


select * from tenz.lay_str_freq_wettingen;


-------------------------
-- Bahnhöfe
-------------------------
select * from tenz.lay_bahnhof;

drop table
	tenz.lay_bahnhof_wettingen;

create table
	tenz.lay_bahnhof_wettingen
as
select
	a.*
from
	tenz.lay_bahnhof a
join
	tenz.lay_agglo_wettingen b
on
	st_within( a.geo_point_lv95, b.geo_poly_lv95 )
;

select * from tenz.lay_bahnhof_wettingen;




---------------------
-- Zielgruppenanalyse
---------------------
drop table if exists 
	tenz.lay_anz_ziel_prs_wettingen;
create table 
	tenz.lay_anz_ziel_prs_wettingen
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
	-- Agglo Wettingen
	agglo_nr_2012 = 4021
;

select * from tenz.lay_anz_ziel_prs_wettingen;



drop table if exists 
	tenz.lay_hkt100_anz_ziel_prs_wettingen;
create table 
	tenz.lay_hkt100_anz_ziel_prs_wettingen
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
		-- Agglo Wettingen
		agglo_nr_2012 = 4021
) gmd
on
	st_within( hkt.geo_poly_lv95, gmd.geo_poly_lv95 )
;
















--/////////////////////////////////////////////
--==========================================
-- TEST ideas
--==========================================
select * from geo_afo_prod.mv_lay_hkt100_aktuell;
select * from geo_afo_prod.mv_lay_gmd_aktuell;

drop table if exists tenz.lay_hkt100_wettingen_anz_pois;
create table tenz.lay_hkt100_wettingen_anz_pois
as
select 
	hkt.gid
	,gmd.gid 													as gmd_gid
	,hkt.reli
	--,hkt.pers_tot
	--,ntile(4) over(order by hkt.pers_tot) 						as pers_tot_dec
	,gmd.altkls_15_19
	,gmd.altkls_20_24
	,gmd.altkls_25_29
	,gmd.altkls_30_34
	,(hkt.m_15_19 + hkt.w_15_19) 								as wohnbev_15_19
	,(hkt.m_20_24 + hkt.w_20_24) 								as wohnbev_20_24
	,(hkt.m_25_29 + hkt.w_25_29) 								as wohnbev_25_29
	,(hkt.m_30_34 + hkt.w_30_34) 								as wohnbev_30_34
	--,hkt.arbeitsstaetten_tot 
	--,ntile(4) over(order by hkt.arbeitsstaetten_tot) 				as arbeitsstaetten_tot_dec 
	--,hkt.besch_tot
	--,ntile(4) over(order by hkt.besch_tot) 						as besch_tot_dec
	--,(hkt.besch_f_s3 + hkt.besch_m_s3) 							as besch_s3
	--,ntile(4) over(order by (hkt.besch_f_s3 + hkt.besch_m_s3)) 	as besch_s3_dec
	,hkt.anz_universitaet
	,hkt.anz_fachhochschule
	,hkt.anz_berufsschule 
	,hkt.anz_schule
	,hkt.anz_schwimmbad
	,hkt.anz_sportstadion
	,hkt.anz_fitnessstudio 
	,hkt.anz_icehockey_stadion 
	,hkt.anz_spielwaren 
	,hkt.anz_tierpark_zoo 
	,hkt.anz_kino 
	,hkt.anz_club_discothek
	,hkt.anz_event
	,hkt.anz_einkaufszentrum 
	,hkt.anz_non_food_warenhaus
	,hkt.anz_multimedia_haushalt_software 
	,hkt.anz_convenience 
	,hkt.anz_getraenke
	,hkt.anz_fastfood 
	--,hkt.anz_food 
	,hkt.geo_poly_lv95
from
	geo_afo_prod.mv_lay_hkt100_aktuell hkt
join (
	select
		*
	from  
		geo_afo_prod.mv_lay_gmd_aktuell 
	where 
		-- Agglo Wettingen
		agglo_nr_2012 = 4021
) gmd
on
	st_within( hkt.geo_poly_lv95, gmd.geo_poly_lv95 )
;


select * from tenz.lay_hkt100_wettingen_anz_pois where pers_tot_dec = 1;




















