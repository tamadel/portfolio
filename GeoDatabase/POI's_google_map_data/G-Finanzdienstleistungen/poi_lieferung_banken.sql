
--=======================================================================
-- Bereinigung poi_lieferung_banken
--=======================================================================

--------------------------------------------------------------
-- geo_point_lv95 in Geometry umwandeln
--------------------------------------------------------------
alter table 
	google_maps_dev_abgleich.poi_lieferung_banken 
rename column
	geo_point_lv95 to geo_point_lv95_str
;

alter table 
	google_maps_dev_abgleich.poi_lieferung_banken
add column
	geo_point_lv95 geometry(POINT, 2056)
;

update
	google_maps_dev_abgleich.poi_lieferung_banken
set
	geo_point_lv95 = st_geomfromtext(geo_point_lv95_str)
where
	1 = 1
;

alter table
	google_maps_dev_abgleich.poi_lieferung_banken
drop column
	geo_point_lv95_str
;

select
	*
from
	google_maps_dev_abgleich.poi_lieferung_banken
;

--=======================================================================
-- Bereinigung poi_lieferung_banken_zkb
--=======================================================================

--------------------------------------------------------------
-- geo_point_lv95 in Geometry umwandeln
--------------------------------------------------------------
alter table 
	google_maps_dev_abgleich.poi_lieferung_banken_zkb 
rename column
	geo_point_lv95 to geo_point_lv95_str
;

alter table 
	google_maps_dev_abgleich.poi_lieferung_banken_zkb
add column
	geo_point_lv95 geometry(POINT, 2056)
;

update
	google_maps_dev_abgleich.poi_lieferung_banken_zkb
set
	geo_point_lv95 = st_geomfromtext(geo_point_lv95_str)
where
	1 = 1
;

alter table
	google_maps_dev_abgleich.poi_lieferung_banken_zkb
drop column
	geo_point_lv95_str
;

select
	*
from
	google_maps_dev_abgleich.poi_lieferung_banken_zkb
;

--=======================================================================
-- Tabellen zusammenfügen
--=======================================================================
drop table if exists
	google_maps_dev_abgleich.poi_lieferung_banken_tot;

create table 
	google_maps_dev_abgleich.poi_lieferung_banken_tot
as
select
	poi_id
	,hauskey
	,poi_typ_id
	,poi_typ
	,company_group_id
	,company_group
	,company_id
	,company
	,company_unit
	,company_brand
	,bezeichnung_lang
	,bezeichnung_kurz
	,adresse
	,plz4
	,ort
	,url
	,geo_point_lv95
from
	google_maps_dev_abgleich.poi_lieferung_banken
;

insert into
	google_maps_dev_abgleich.poi_lieferung_banken_tot 
(
	poi_id
	,hauskey
	,poi_typ_id
	,poi_typ
	,company_group_id
	,company_group
	,company_id
	,company
	,company_unit
	,company_brand
	,bezeichnung_lang
	,bezeichnung_kurz
	,adresse
	,plz4
	,ort
	,url
	,geo_point_lv95
)
select
	poi_id
	,hauskey
	,poi_typ_id
	,poi_typ
	,company_group_id
	,company_group
	,company_id
	,company
	,company_unit
	,company_brand
	,bezeichnung_lang
	,bezeichnung_kurz
	,adresse
	,plz4
	,ort
	,url
	,geo_point_lv95
from
	google_maps_dev_abgleich.poi_lieferung_banken_zkb
;

--=======================================================================
-- Haukey bereinigen (scheint z.T. beim Import/Export verändert worden zu sein)
--=======================================================================
update
	google_maps_dev_abgleich.poi_lieferung_banken_tot r
set 
	hauskey = u.hauskey::bigint
from 
	geo_afo_prod.mv_lay_poi_aktuell u
where 
	r.poi_id::text = u.poi_id::text
;
update
	google_maps_dev_abgleich.poi_lieferung_banken_tot
set 
	hauskey = null 
where
	trim(hauskey) = ''
;

--=======================================================================
-- company_group_id / company_group aktualisieren
--=======================================================================
update
	google_maps_dev_abgleich.poi_lieferung_banken_tot r
set 
	company_group_id = (r.company_id/1000000)::int
	,company_group = u.company_group
from 
	geo_afo_prod.v_meta_company_group_aktuell u
where 
	(r.company_id/1000000)::int = u.company_group_id
;

--=======================================================================
-- company_id aktualisieren
--=======================================================================
update
	google_maps_dev_abgleich.poi_lieferung_banken_tot r
set 
	company_id = u.company_id
from 
	geo_afo_prod.v_meta_company_aktuell u
where 
	r.company_group_id = u.company_group_id
	and
	lower(r.company) = lower(u.company)
;

--=======================================================================
-- Checks
--=======================================================================
select 
	*
from
	google_maps_dev_abgleich.poi_lieferung_banken_tot
order by
	random()
limit
	100
;

select
	count(*)
from
	google_maps_dev_abgleich.poi_lieferung_banken_tot
;

select
	count(*)
from
	google_maps_dev_abgleich.poi_lieferung_banken
;

select
	count(*)
from
	google_maps_dev_abgleich.poi_lieferung_banken_zkb
;

select
	*
from
	geo_afo_prod.meta_poi_typ_hist 
where 
	extract(year from gueltig_bis) = 9999
order by 	
	poi_typ
;

select
	count(*)
from
	geo_afo_prod.mv_lay_poi_aktuell 
where 
	poi_typ_id = 102
;
