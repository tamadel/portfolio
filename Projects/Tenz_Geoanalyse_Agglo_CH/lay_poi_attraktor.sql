--=================================
-- Relevante POI's für Tenz ganze CH
--=================================

drop table if exists
	tenz.lay_poi_attraktor;

create table
	tenz.lay_poi_attraktor
as
SELECT 
	*
FROM 
	dblink(
		'geo_database',
		$POSTGRES$
			SELECT 	  
				poi_id 
				,poi_typ_id 
				,poi_typ
				,company 
				,adresse 
				,plz4::int 
				,ort 
				,geo_point_lv95 
			FROM 
				geo_afo_prod.mv_lay_poi_aktuell 
			where
				poi_typ_id in (					
					207 						-- Food j und 
					,1001						-- Bar
					,1002 						-- Biergarten
					,1003 						-- cafe
					,1004 						-- Fastfood 
					,1005 						-- pub
					,1006 						-- Restaurant
					,506 						-- Non-Food j
					,504						-- Einkaufszentrum j
					,505						-- Multimedia/Haushalt/Software j
					,508						-- Non Food/Warenhaus j
					,1204						-- Sekundarstufe 1 j
					,1205						-- Sekundarstufe 2 j
					,1206						-- Tertiäre Stufe j
					,1207						-- Zweitausbildung nicht-tertiäre Stufe j
				)
		$POSTGRES$
	) AS mv_lay_poi_aktuell (
		poi_id int
		,poi_typ_id int
		,poi_typ text
		,company text
		,adresse text
		,plz4 int
		,ort text
		,geo_point_lv95 public.geometry(point, 2056)
	)
;	

	--302							-- Club Discothek n
	--,303						-- Eishalle / Icehockey Stadion n
	--,304						-- Fitnessstudio n
	--,313						-- Freizeitpark n
	--,306						-- Kino n
	--,307						-- Öffentlicher Park n
	--,1005						-- Pub n
	--,1006						-- Restaurant n
	--,309						-- Schwimmbad n
	--,317						-- Sportplatz n
	--,310						-- Sportstadion n
	--,311						-- Theater / event n

--===================
-- Poi's Lausanne
--===================
drop table
	tenz.lay_poi_attraktor_lausanne;

create table
	tenz.lay_poi_attraktor_lausanne
as
select
	a.*
from
	tenz.lay_poi_attraktor a
join
	tenz.lay_agglo_lausanne b
on
	st_within( a.geo_point_lv95, b.geo_poly_lv95 )
;

select 
	*
from
	tenz.lay_poi_attraktor_lausanne
;


--============================
-- Tenz Konkurrenzen ganze CH
--============================
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


drop table if exists
	tenz.lay_poi_attrkonkur;

create table
	tenz.lay_poi_attrkonkur
as
SELECT 
	*
FROM 
	dblink(
		'geo_database',
		$POSTGRES$
			SELECT 	  
				poi_id 
				,poi_typ_id 
				,poi_typ
				,company 
				,adresse 
				,plz4::int 
				,ort 
				,geo_point_lv95 
			FROM 
				geo_afo_prod.mv_lay_poi_aktuell 
			where
				poi_typ_id in (					
					201   				--Bäckerei
					,1003 				--Café
					,202  				--Convenience
					,1004 				--Fastfood
					,207  				--Food
				)
		$POSTGRES$
	) AS mv_lay_poi_aktuell (
		poi_id int
		,poi_typ_id int
		,poi_typ text
		,company text
		,adresse text
		,plz4 int
		,ort text
		,geo_point_lv95 public.geometry(point, 2056)
	)
;	








