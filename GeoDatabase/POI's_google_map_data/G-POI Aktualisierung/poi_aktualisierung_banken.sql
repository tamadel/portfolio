
--===============================================================================
-- Suchen -> Ersetzen
-- <HAUPTKATEGORIE> 		-> Hauptkategorie für Tabellen-Bezeichnungen
-- <HAUPTKATEGORIE_TEXT>	-> Hauptkategorie als Text
--===============================================================================


--===============================================================================
-- BANKEN WURDEN MANUELL ÜBERARBEITET, DESHALB ÜBERNAHME DIREKT AUS 
-- IMPORT-TABELLE google_maps_dev_abgleich.poi_lieferung_banken_tot
--===============================================================================
-- select * from google_maps_dev_abgleich.poi_lieferung_banken_tot;

-------------------------------------------------------------------------------------
-- tmp-Tabelle mit Matching zw. POIs und Google-Maps
-------------------------------------------------------------------------------------
/*
drop table if exists
	tmp_poi_gm_match;

create temp table
	tmp_poi_gm_match
as
select
	dubletten_nr	
	,poi_id::text							as cid
	,null::numeric							as poi_id
from
	google_maps_dev_abgleich.poi_abgleich_google_<HAUPTKATEGORIE>_tot
where
	quelle = 'GOOGLE'
	and
	dubletten_nr is not null
;

update
	tmp_poi_gm_match r
set
	poi_id = u.poi_id::numeric
from
	google_maps_dev_abgleich.poi_abgleich_google_<HAUPTKATEGORIE>_tot u
where
	u.quelle = 'AFO'
	and
	r.dubletten_nr = u.dubletten_nr 
;
-- select * from tmp_poi_gm_match;
-- select * from google_maps_dev_abgleich.poi_abgleich_google_<HAUPTKATEGORIE>_tot order by dubletten_nr, quelle;
*/

-------------------------------------------------------------------------------------
-- tmp-Tabelle mit poi_main_kat_id / poi_kat_id / poi_typ_id erstellen
-------------------------------------------------------------------------------------
/*
drop table if exists
	tmp_poi_map;

create temp table
	tmp_poi_map
as
with
	category_id as (
		select 
			a.cid
			,a.poi_id
			,a.category_ids
			,replace(replace(category_id,'[',''),']','') as category_id 									--category_id as text
			--,trim(replace(replace(replace(category_id, '[', ''), ']', ''), '"', '')) as category_id 		--category_id as jsonb
		from (
			select
				t0.cid::text
				,t1.poi_id::numeric
				,string_to_array(t0.category_en_ids, ' | ') as category_ids   							--category_ids as text
		    	--,string_to_array(t0.category_ids::text, ',') as category_ids    						--category_id as jsonb
			from
				google_maps_dev.google_map_<HAUPTKATEGORIE>_v1 t0
			left join
				tmp_poi_gm_match t1
			on
				t0.cid::text = t1.cid::text
		) a,
		unnest(a.category_ids) as category_id
	)
	,poi_main_kat as (
		select 
			*
		from
			geo_afo_prod.meta_poi_main_kat_2024_hist 
		where
			extract(year from gueltig_bis) = 9999
	)
	,poi_kat as (
		select 
			*
		from
			geo_afo_prod.meta_poi_kat_2024_hist 
		where
			extract(year from gueltig_bis) = 9999
	)
	,poi_typ as (
		select 
			*
		from
			geo_afo_prod.meta_poi_typ_2024_hist 
		where
			extract(year from gueltig_bis) = 9999
	)
select distinct
	a.cid
	,a.poi_id
	,c.poi_main_kat_id
	,b.hauptkategorie_neu 
	,d.poi_kat_id
	,b.kategorie_neu
	,e.poi_typ_id::bigint
	,b.poi_typ_neu
from
	category_id a
join 
	geo_afo_prod.meta_poi_categories_business_data_aktuell b
on
	a.category_id = b.category_id
join
	poi_main_kat c
on
	lower(b.hauptkategorie_neu) = lower(c.poi_main_kat)
join
	poi_kat d
on
	lower(b.kategorie_neu) = lower(d.poi_kat)
join 
	poi_typ e
on
	lower(b.poi_typ_neu) = lower(e.poi_typ)
;
-- select * from tmp_poi_map order by cid;
*/

-------------------------------------------------------------------------------------
-- bei gematchten POIs Geometrien in lay_poi_geo_hist von Google übernehmen
-------------------------------------------------------------------------------------
update
	geo_afo_prod.lay_poi_geo_2024_hist r
set
	geo_point_lv03 = st_transform(st_setsrid(u.geo_point_lv95,2056), 21781) 
	,geo_point_lv95 = st_transform(st_setsrid(u.geo_point_lv95,2056), 2056)
	,geo_point_wgs84 = st_transform(st_setsrid(u.geo_point_lv95,2056), 4326)
	,updated_ts = current_timestamp
from 
	google_maps_dev_abgleich.poi_lieferung_banken_tot u
where
	extract(year from r.gueltig_bis) = 9999
	and
	r.poi_id::text = u.poi_id::text
;


-------------------------------------------------------------------------------------
-- bei gematchten POIs cid und category_ids in lay_poi_geo_hist setzen, falls leer
-------------------------------------------------------------------------------------
/*
update
	geo_afo_prod.lay_poi_geo_2024_hist r
set
	cid = u.cid
	,category_ids = u.category_ids
	,updated_ts = current_timestamp
from (
	select
		t0.cid::text
		,t0.poi_id::numeric
		,string_to_array(t1.category_en_ids, ' | ') 		as category_ids -- ctegory_ids as TEXT
	--	,string_to_array(t1.category_ids::text, ' | ') 		as category_ids -- category_ids as JSON
	from
		tmp_poi_gm_match t0
	join
		google_maps_dev.google_map_<HAUPTKATEGORIE>_v1 t1
	on
		t0.cid::text = t1.cid::text
) u
where
	extract(year from r.gueltig_bis) = 9999
	and
	r.poi_id = u.poi_id
	and
	r.cid is null
;
-- select * from geo_afo_prod.lay_poi_geo_2024_hist where updated_ts::date = current_date order by updated_ts desc;
*/

-------------------------------------------------------------------------------------
-- bei gematchten POIs poi_main_kat_id_list in lay_poi_geo_hist setzen, falls leer
-------------------------------------------------------------------------------------
/*
update
	geo_afo_prod.lay_poi_geo_2024_hist r
set
	poi_main_kat_id_list = ARRAY_APPEND(r.poi_main_kat_id_list, u.poi_main_kat_id)
	,updated_ts = current_timestamp
from (
	select
		*
	from
		geo_afo_prod.meta_poi_main_kat_2024_hist
	where
		extract(year from gueltig_bis) = 9999
		and
		poi_main_kat = '<HAUPTKATEGORIE_TEXT>'
) u
where
	extract(year from r.gueltig_bis) = 9999
	and
	r.poi_main_kat_id_list is null
	and
	r.poi_id in (
		select
			poi_id
		from
			tmp_poi_gm_match
	)
;
-- select * from geo_afo_prod.lay_poi_geo_2024_hist where updated_ts::date = current_date order by updated_ts desc;
*/

-------------------------------------------------------------------------------------
-- bei gematchten POIs poi_kat_id_list in lay_poi_geo_hist setzen, falls leer
-------------------------------------------------------------------------------------
/*
update
	geo_afo_prod.lay_poi_geo_2024_hist r
set
	poi_kat_id_list = u.poi_kat_id_list
	,updated_ts = current_timestamp
from (
	select
		poi_id
		,array_agg(poi_kat_id::bigint) as poi_kat_id_list
	from (
		select distinct
			poi_id
			,poi_kat_id
		from
			tmp_poi_map
	) a
	group by
		a.poi_id 
) u
where
	extract(year from r.gueltig_bis) = 9999
	and
	r.poi_id = u.poi_id
	and
	r.poi_kat_id_list is null
;
-- select * from geo_afo_prod.lay_poi_geo_2024_hist where updated_ts::date = current_date order by updated_ts desc;
*/

-------------------------------------------------------------------------------------
-- bei gematchten POIs poi_typ_id_list in lay_poi_geo_hist setzen, falls leer
-------------------------------------------------------------------------------------
/*
update
	geo_afo_prod.lay_poi_geo_2024_hist r
set
	poi_typ_id_list = u.poi_typ_id_list
	,updated_ts = current_timestamp
from (
	select
		poi_id
		,array_agg(poi_typ_id::bigint) as poi_typ_id_list
	from (
		select distinct
			poi_id
			,poi_typ_id
		from
			tmp_poi_map
	) a
	group by
		a.poi_id 
) u
where
	extract(year from r.gueltig_bis) = 9999
	and
	r.poi_id = u.poi_id
	and
	r.poi_typ_id_list is null
;
-- select * from geo_afo_prod.lay_poi_geo_2024_hist where updated_ts::date = current_date order by updated_ts desc;
*/

-------------------------------------------------------------------------------------
-- bei gematchten POIs bezeichnung_lang in lay_poi_geo_hist von Google übernehmen
-------------------------------------------------------------------------------------
/*
update
	geo_afo_prod.lay_poi_geo_2024_hist r
set
	bezeichnung_lang = u.company
	,updated_ts = current_timestamp
from (
	select 
		b.poi_id
		,a.company
	from
		google_maps_dev_abgleich.poi_abgleich_google_<HAUPTKATEGORIE>_tot a
	join 
		tmp_poi_gm_match b
	on
		a.poi_id = b.cid
) u
where
	extract(year from r.gueltig_bis) = 9999
	and
	r.poi_id = u.poi_id
;
-- select * from geo_afo_prod.lay_poi_geo_2024_hist where updated_ts::date = current_date order by updated_ts desc;
*/


/*
 * **********************************************************************************************
 * 	FALLS BISHERIGE POIS ÜBERNOMMEN WERDEN SOLLEN
 * **********************************************************************************************
*/
/*
-------------------------------------------------------------------------------------
-- tmp-Tabelle mit aktuellen POIs
-------------------------------------------------------------------------------------
drop table if exists
	tmp_poi_current;

create temp table
	tmp_poi_current
as
select
	poi_id
	,poi_typ_id_old
from
	geo_afo_prod.lay_poi_geo_2024_hist
where
	extract(year from gueltig_bis) = 9999
	and
	poi_typ_id_old in (
		select distinct
			poi_typ_id_alt
		from
			google_maps_dev_abgleich.map_poi_typ_alt_neu 
		where 
			poi_main_kat_neu = '<HAUPTKATEGORIE_TEXT>'
	)
;
-- select * from tmp_poi_current;
*/

-------------------------------------------------------------------------------------
-- poi_main_kat_id_list in lay_poi_geo_hist für Banken setzen, falls leer
-------------------------------------------------------------------------------------
update
	geo_afo_prod.lay_poi_geo_2024_hist r
set
	poi_main_kat_id_list = ARRAY_APPEND(r.poi_main_kat_id_list, u.poi_main_kat_id_neu)
	,updated_ts = current_timestamp
from 
	google_maps_dev_abgleich.map_poi_typ_alt_neu u	
where
	extract(year from r.gueltig_bis) = 9999
	and
	r.poi_typ_id_old = 102 -- Bank
	and
	r.poi_typ_id_old = u.poi_typ_id_alt
	and
	r.poi_main_kat_id_list is null
;

-- select * from geo_afo_prod.lay_poi_geo_2024_hist where updated_ts::date = current_date order by updated_ts desc;

-------------------------------------------------------------------------------------
-- bei nicht gematchten POIs poi_kat_id_list in lay_poi_geo_hist setzen, falls leer
-------------------------------------------------------------------------------------
update
	geo_afo_prod.lay_poi_geo_2024_hist r
set
	poi_kat_id_list = ARRAY_APPEND(r.poi_kat_id_list, u.poi_kat_id_neu)
	,updated_ts = current_timestamp
from 
	google_maps_dev_abgleich.map_poi_typ_alt_neu u	
where
	extract(year from r.gueltig_bis) = 9999
	and
	r.poi_typ_id_old = 102 -- Bank
	and
	r.poi_typ_id_old = u.poi_typ_id_alt
	and
	r.poi_kat_id_list is null
;
-- select * from geo_afo_prod.lay_poi_geo_2024_hist where updated_ts::date = current_date order by updated_ts desc;

-------------------------------------------------------------------------------------
-- bei nicht gematchten POIs poi_typ_id_list in lay_poi_geo_hist setzen, falls leer
-------------------------------------------------------------------------------------
update
	geo_afo_prod.lay_poi_geo_2024_hist r
set
	poi_typ_id_list = ARRAY_APPEND(r.poi_typ_id_list, u.poi_typ_id_neu)
	,updated_ts = current_timestamp
from 
	google_maps_dev_abgleich.map_poi_typ_alt_neu u	
where
	extract(year from r.gueltig_bis) = 9999
	and
	r.poi_typ_id_old = 102 -- Bank
	and
	r.poi_typ_id_old = u.poi_typ_id_alt
	and
	r.poi_typ_id_list is null
;
-- select * from geo_afo_prod.lay_poi_geo_2024_hist where updated_ts::date = current_date order by updated_ts desc;


/*
 * **********************************************************************************************
 * 	FALLS GOOGLE POIS ÜBERNOMMEN WERDEN SOLLEN
 * **********************************************************************************************
*/

-------------------------------------------------------------------------------------
-- tmp-Tabelle mit aktuellen POIs
-------------------------------------------------------------------------------------
drop table if exists
	tmp_poi_current;

create temp table
	tmp_poi_current
as
select
	poi_id
	,cid
	,poi_main_kat_id_list
	,poi_kat_id_list
	,poi_typ_id_list
	,category_ids
	,hauskey
	,company_id
	,''								as company
	,company_group_id
	,''								as company_group
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
	geo_afo_prod.lay_poi_geo_2024_hist
where
	extract(year from gueltig_bis) = 9999
	and
	poi_typ_id_old = 102 -- Bank
;

update
	tmp_poi_current r
set 
	company = u.company
from (
	select
		*
	from
		geo_afo_prod.meta_company_hist
	where
		extract(year from gueltig_bis) = 9999
) u
where
	r.company_id = u.company_id
;

update
	tmp_poi_current r
set 
	company_group = u.company_group
from (
	select
		*
	from
		geo_afo_prod.meta_company_group_hist
	where
		extract(year from gueltig_bis) = 9999
) u
where
	r.company_group_id = u.company_group_id
;
-- select * from tmp_poi_current;

-------------------------------------------------------------------------------------
-- tmp-Tabelle mit neuen POIs von Google-Maps
-------------------------------------------------------------------------------------
/*
drop table if exists
	tmp_gm_new;

create temp table
	tmp_gm_new
as
select 
	b.poi_id::numeric
	,hauskey::bigint
	,company_id::int
	,company	 
	,company_group_id::int
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
	google_maps_dev_abgleich.poi_lieferung_banken_tot
left join (
	select
		cid
		,array_agg(poi_kat_id::bigint) as poi_kat_id_list
	from (
		select distinct
			cid
			,poi_kat_id
		from
			tmp_poi_map
	) a
	group by
		cid
) c
on
	a.cid = c.cid
left join (
	select
		cid
		,array_agg(poi_typ_id::bigint) as poi_typ_id_list
	from (
		select distinct
			cid
			,poi_typ_id
		from
			tmp_poi_map
	) a
	group by
		cid
) d
on
	a.cid = d.cid
;
-- select * from tmp_gm_new;
*/

-------------------------------------------------------------------------------------
-- hauskey aus lay_gbd_geo_hist setzen         
-- -> nächstes Gebäude innerhalb 20m Radius
-------------------------------------------------------------------------------------
drop table if exists
	tmp_gbd_match;

create temp table
	tmp_gbd_match
as
select
	poi.poi_id
	,poi.adresse			as poi_adresse
	,poi.plz4				as poi_plz4
	,poi.ort				as poi_ort
	,gbd.hauskey			as gbd_hauskey
	,gbd.strbezl			as gbd_strbezl
	,gbd.hnr				as gbd_hrn
	,gbd.hnra				as gbd_hnra
	,gbd.plz4				as gbd_plz4
	,gbd.ort				as gbd_ort
	,st_distance( poi.geo_point_lv95, gbd.geo_point_eg_lv95 ) as dist
	,rank() over (
		partition by
			poi.poi_id
		order by
			st_distance( poi.geo_point_lv95, gbd.geo_point_eg_lv95 )
			,gbd.hauskey
	) 						as prio
from
	google_maps_dev_abgleich.poi_lieferung_banken_tot poi
join (
	select
		hauskey
		,strbezl
		,hnr
		,hnra
		,plz4
		,ort
		,geo_point_eg_lv95
		,st_x(geo_point_eg_lv95)			as x_koord
		,st_y(geo_point_eg_lv95)			as y_koord
	from
		geo_afo_prod.lay_gbd_geo_hist
	where
		extract(year from gueltig_bis) = 9999
		and
		plz4 in (
			select distinct
				plz4
			from
				google_maps_dev_abgleich.poi_lieferung_banken_tot
		)
) gbd
on
	poi.plz4 = gbd.plz4
	and
	st_x(poi.geo_point_lv95) between gbd.x_koord-10 and gbd.x_koord+10
	and
	st_y(poi.geo_point_lv95) between gbd.y_koord-10 and gbd.y_koord+10
;
-- select * from tmp_gbd_match;

update
	google_maps_dev_abgleich.poi_lieferung_banken_tot r
set
	hauskey = u.gbd_hauskey
from 
	tmp_gbd_match u
where
	r.poi_id = u.poi_id
	and
	u.prio = 1
	and
	trim(r.hauskey) = ''
;
-- select * from google_maps_dev_abgleich.poi_lieferung_banken_tot;


-------------------------------------------------------------------------------------
-- company_group und company in Google-Maps übernehmen, falls Match mit bestehenden POIs
-------------------------------------------------------------------------------------
/*
drop table if exists
	tmp_company_update;

create temp table
	tmp_company_update
as
select distinct
	poi.poi_id
	,co.company_group_id
	,co.company_group
	,co.company_id
	,co.company
from (
	select
		*
	from
		geo_afo_prod.lay_poi_geo_2024_hist 
	where
		extract(year from gueltig_bis) = 9999
		and
		poi_id in (
			select 
				poi_id 
			from
				tmp_gm_new
			where
				poi_id is not null
		)
) poi
join (
	select
		cg.company_group_id
		,cg.company_group
		,co.company_id
		,co.company
	from (
		select
			*
		from
			geo_afo_prod.meta_company_hist
		where
			extract(year from gueltig_bis) = 9999
	) co
	join (
		select
			*
		from
			geo_afo_prod.meta_company_group_hist
		where
			extract(year from gueltig_bis) = 9999
	) cg
	on
		co.company_group_id = cg.company_group_id
) co
on
	poi.company_id = co.company_id 
;
-- select * from tmp_company_update;

update	
	tmp_gm_new r
set
	company_group_id = u.company_group_id
	,company_group = u.company_group
	,company_id = u.company_id
	,company = u.company
from 
	tmp_company_update u
where
	r.poi_id is not null
	and
	r.poi_id = u.poi_id
;
-- select * from tmp_gm_new where poi_id is not null;
*/

-------------------------------------------------------------------------------------
-- tmp-Tabelle mit veralteten Einträgen erstellen
-------------------------------------------------------------------------------------
drop table if exists
	tmp_poi_old;

create temp table
	tmp_poi_old
as
select
	poi_id::text
	--,cid
	--,poi_main_kat_id_list
	--,poi_kat_id_list
	--,poi_typ_id_list
	--,category_ids
	,hauskey
	,company_id
	,company
	,company_group_id
	,company_group
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
	tmp_poi_current
except
select
	poi_id::text
	--,cid
	--,poi_main_kat_id_list
	--,poi_kat_id_list
	--,poi_typ_id_list
	--,category_ids
	,hauskey::int
	,company_id
	,company
	,company_group_id
	,company_group
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
	google_maps_dev_abgleich.poi_lieferung_banken_tot
;
-- select * from tmp_poi_old;


-------------------------------------------------------------------------------------
-- tmp-Tabelle mit neuen Einträgen erstellen
-------------------------------------------------------------------------------------
drop table if exists
	tmp_poi_new;

create temp table
	tmp_poi_new
as
select
	poi_id::text
	--,cid
	--,poi_main_kat_id_list
	--,poi_kat_id_list
	--,poi_typ_id_list
	--,category_ids
	,hauskey::int
	,company_id
	,company
	,company_group_id
	,company_group
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
	google_maps_dev_abgleich.poi_lieferung_banken_tot
except
select
	poi_id::text
	--,cid
	--,poi_main_kat_id_list
	--,poi_kat_id_list
	--,poi_typ_id_list
	--,category_ids
	,hauskey
	,company_id
	,company
	,company_group_id
	,company_group
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
	tmp_poi_current
;
-- select * from tmp_poi_new;

-------------------------------------------------------------------------------------
-- poi_main_kat_id_list, poi_kat_id_list und poi_typ_id_list in lay_poi_geo_hist für Banken setzen
-------------------------------------------------------------------------------------
alter table
	tmp_poi_new
add column if not exists
	poi_main_kat_id_list int array
;
alter table
	tmp_poi_new
add column if not exists
	poi_kat_id_list bigint array
;
alter table
	tmp_poi_new
add column if not exists
	poi_typ_id_list bigint array
;
	
update
	tmp_poi_new r
set
	poi_main_kat_id_list = ARRAY_APPEND(r.poi_main_kat_id_list, u.poi_main_kat_id_neu)
	,poi_kat_id_list = ARRAY_APPEND(r.poi_kat_id_list, u.poi_kat_id_neu)
	,poi_typ_id_list = ARRAY_APPEND(r.poi_typ_id_list, u.poi_typ_id_neu)
from 
	google_maps_dev_abgleich.map_poi_typ_alt_neu u	
where
	u.poi_typ_id_alt = 102 -- Bank
	and
	poi_typ_id_list is null
;
-- select * from tmp_poi_new;

-------------------------------------------------------------------------------------
-- company_group auf 'Freie Gastronomie' setzen, falls leer
-------------------------------------------------------------------------------------
update 	
	tmp_poi_new
set
	company_group = 'Freie Privatbank'
where
	trim(coalesce(company_group, '')) = ''
;
-- select * from tmp_poi_new;

-------------------------------------------------------------------------------------
-- Tabelle für Aktualisierung von company und company_group erstellen
-------------------------------------------------------------------------------------
drop table if exists
	tmp_companies;

create temp table
	tmp_companies
as
select distinct
	t1.company_group_id
	,coalesce(t1.company_group, 'Freie Privatbank') as company_group
	,t0.company_id
	,t0.company
from
	tmp_poi_new t0
left join (
	select
		*
	from
		geo_afo_prod.meta_company_group_hist
	where
		extract(year from gueltig_bis) = 9999
) t1
on
	(t0.company_id/1000000)::int = t1.company_group_id
;
-- select * from tmp_companies;

-------------------------------------------------------------------------------------
-- company_group_id setzen, falls noch nicht gesetzt, aber in meta_company_group_hist vorhanden
-------------------------------------------------------------------------------------
update
	tmp_companies r
set
	company_group_id = u.company_group_id
from (
	select
		*
	from
		geo_afo_prod.meta_company_group_hist
	where
		extract(year from gueltig_bis) = 9999
) u
where
	lower(r.company_group) = lower(u.company_group)
	and
	coalesce(r.company_group_id, 0) = 0
;
-- select * from tmp_companies;

-------------------------------------------------------------------------------------
-- tmp-Tabelle mit neuen company_group erstellen
-------------------------------------------------------------------------------------
drop table if exists
	tmp_company_group_new;
	
create temp table
	tmp_company_group_new
as
select distinct
	row_number() over ( 
		partition by
			company_group
		order by
			random()
	) as rec_num
	,company_group 
	,(
		select 
			max(company_group_id) 
		from
			geo_afo_prod.meta_company_group_hist
	) as last_company_group_id
from (
	select distinct
		company_group
	from
		tmp_companies
	where
		lower(company_group) not in (
			select
				lower(company_group)
			from
				geo_afo_prod.meta_company_group_hist
			where
				extract(year from gueltig_bis) = 9999
		)
) a
;
-- select * from tmp_company_group_new;

-------------------------------------------------------------------------------------
-- Eintrag der neuen company_group in meta_company_group_hist
-------------------------------------------------------------------------------------
insert into
	geo_afo_prod.meta_company_group_hist
(
	company_group_id
	,company_group 
	,gueltig_von 
	,gueltig_bis 
	,created_ts 
	,updated_ts 
)
select 
	(last_company_group_id + rec_num)					as company_group_id
	,company_group 
	,current_date
	,'9999-12-31'
	,current_timestamp
	,current_timestamp
from
	tmp_company_group_new
;
-- select * from geo_afo_prod.meta_company_group_hist where created_ts::date = current_date;

-------------------------------------------------------------------------------------
-- company_group_id in tmp_companies setzen, falls leer
-------------------------------------------------------------------------------------
update
	tmp_companies r
set
	company_group_id = u.company_group_id
from (
	select
		*
	from
		geo_afo_prod.meta_company_group_hist
	where
		extract(year from gueltig_bis) = 9999
) u
where
	lower(r.company_group) = lower(u.company_group)
	and
	coalesce(r.company_group_id, 0) = 0
;
-- select * from tmp_companies;

-------------------------------------------------------------------------------------
-- company_group_id in tmp_poi_new setzen, falls leer
-------------------------------------------------------------------------------------
update
	tmp_poi_new r
set
	company_group_id = u.company_group_id
from (
	select distinct
		company_group_id
		,company_group
	from
		tmp_companies
) u
where
	lower(r.company_group) = lower(u.company_group)
	and
	coalesce(r.company_group_id, 0) = 0
;
-- select * from tmp_poi_new;

-------------------------------------------------------------------------------------
-- company_id setzen, falls noch nicht gesetzt, aber in meta_company_hist vorhanden
-------------------------------------------------------------------------------------
update
	tmp_companies r
set
	company_id = u.company_id
from (
	select
		*
	from
		geo_afo_prod.meta_company_hist
	where
		extract(year from gueltig_bis) = 9999
) u
where
	r.company_group_id = u.company_group_id
	and
	lower(r.company) = lower(u.company)
	and
	coalesce(r.company_id, 0) = 0
;
-- select * from tmp_companies;

-------------------------------------------------------------------------------------
-- tmp-Tabelle mit neuen company erstellen
-------------------------------------------------------------------------------------
drop table if exists
	tmp_company_new;
	
create temp table
	tmp_company_new
as
select distinct
	row_number() over ( 
		partition by
			company_group_id
		order by
			lower(company)
	) as rec_num
	,company_group_id 
	,company_id 
	,company 
	,last_company_id
from (
	select distinct
		t0.company_group_id
		,t0.company_id
		,t0.company
		,coalesce(t2.last_company_id,(t0.company_group_id*1000000)) as last_company_id
	from
		tmp_companies t0
	left join (
		select
			company_group_id
			,company
		from
			geo_afo_prod.meta_company_hist
		where
			extract(year from gueltig_bis) = 9999
	) t1
	on
		t0.company_group_id = t1.company_group_id
		and
		lower(t0.company) = lower(t1.company)
	left join (
		select
			company_group_id
			,max(company_id)		as last_company_id
		from
			geo_afo_prod.meta_company_hist
		group by
			company_group_id
	) t2
	on
		t0.company_group_id = t2.company_group_id
	where
		t1.company is null
) a
where
	coalesce(company,'') <> ''
;
-- select * from tmp_company_new;

-------------------------------------------------------------------------------------
-- Eintrag der neuen company in meta_company_hist
-------------------------------------------------------------------------------------
insert into
	geo_afo_prod.meta_company_hist
(
	company_group_id
	,company_id
	,company
	,gueltig_von 
	,gueltig_bis 
	,created_ts 
	,updated_ts 
)
select 
	company_group_id 									as company_group_id
	,(last_company_id+rec_num)							as company_id
	,company											as company 
	,current_date										as gueltig_von
	,'9999-12-31'										as gueltig_bis
	,current_timestamp									as created_ts
	,current_timestamp									as updated_ts
from
	tmp_company_new
;
-- select * from geo_afo_prod.meta_company_hist where created_ts::date = current_date;

-------------------------------------------------------------------------------------
-- company_id in tmp_poi_new setzen
-------------------------------------------------------------------------------------
update
	tmp_poi_new r
set
	company_id = u.company_id
from (
	select
		*
	from
		geo_afo_prod.meta_company_hist
	where
		extract(year from gueltig_bis) = 9999
) u
where
	r.company_group_id = u.company_group_id
	and
	lower(r.company) = lower(u.company)
;
-- select * from tmp_poi_new where company_id is null;

-- POIs ohne company_id löschen
delete from 
	tmp_poi_new
where
	company_id is null
;



-------------------------------------------------------------------------------------
-- veraltete Einträge in lay_poi_geo_his deaktivieren
-------------------------------------------------------------------------------------
update
	geo_afo_prod.lay_poi_geo_2024_hist
set
	gueltig_bis = current_date - 1
	,updated_ts = current_timestamp
where
	extract(year from gueltig_bis) = 9999
	and
	poi_id::text in (
		select 
			poi_id::text 
		from
			tmp_poi_old
	)
;	


-------------------------------------------------------------------------------------
-- aktualisierte Einträge der bestehenden POIs in lay_poi_geo_his einfügen
-------------------------------------------------------------------------------------
drop table if exists
	tmp_poi_update;

create table
	tmp_poi_update
as
select
	*
from
	tmp_poi_new
where
	poi_id::text in (
		select distinct
			poi_id::text
		from
			geo_afo_prod.lay_poi_geo_2024_hist
	)
;
-- select * from tmp_poi_new;

insert into
	geo_afo_prod.lay_poi_geo_2024_hist
(
	poi_id
	,hauskey
	,poi_main_kat_id_list
	,poi_kat_id_list
	,poi_typ_id_list
	,company_id
	,company_group_id
	,company_unit
	,company_brand
	,bezeichnung_lang
	,adresse
	,plz4
	,ort
	,url
	,geo_point_lv03
	,geo_point_lv95
	,geo_point_wgs84
	,gueltig_von
	,gueltig_bis
	,created_ts
	,updated_ts
	,last_check_ts 
)
select
	poi_id::bigint
	,hauskey
	,poi_main_kat_id_list
	,poi_kat_id_list
	,poi_typ_id_list
	,company_id
	,company_group_id
	,company_unit
	,company_brand
	,bezeichnung_lang
	,adresse
	,plz4
	,ort
	,url
	,st_transform(st_setsrid(geo_point_lv95,2056), 21781)			as geo_point_lv03
	,st_transform(st_setsrid(geo_point_lv95,2056), 2056)			as geo_point_lv95
	,st_transform(st_setsrid(geo_point_lv95,2056), 4326)			as geo_point_wgs84
	,current_date													as gueltig_von
	,'9999-12-31'													as gueltig_bis
	,current_timestamp												as created_ts
	,current_timestamp												as updated_ts
	,current_timestamp												as last_check_ts 
from
	tmp_poi_update
;
-- select * from geo_afo_prod.lay_poi_geo_2024_hist where created_ts::date = current_date;


-------------------------------------------------------------------------------------
-- Einträge der neuen POIs in lay_poi_geo_his einfügen
-------------------------------------------------------------------------------------
drop table if exists
	tmp_poi_new_insert;

create table
	tmp_poi_new_insert
as
select
	row_number() over ( order by company ) as rec_num
	,(
		select 
			max(poi_id)
		from
			geo_afo_prod.lay_poi_geo_2024_hist
	) as last_poi_id
	,*
from
	tmp_poi_new
where
	poi_id::text not in (
		select distinct
			poi_id::text
		from
			geo_afo_prod.lay_poi_geo_2024_hist
	)
;
-- select * from tmp_poi_new_insert;

insert into
	geo_afo_prod.lay_poi_geo_2024_hist
(
	poi_id
	,hauskey
	,poi_main_kat_id_list
	,poi_kat_id_list
	,poi_typ_id_list
	,company_id
	,company_group_id
	,company_unit
	,company_brand
	,bezeichnung_lang
	,adresse
	,plz4
	,ort
	,url
	,geo_point_lv03
	,geo_point_lv95
	,geo_point_wgs84
	,gueltig_von
	,gueltig_bis
	,created_ts
	,updated_ts
	,last_check_ts 
)
select
	(last_poi_id + rec_num)											as poi_id 
	,hauskey
	,poi_main_kat_id_list
	,poi_kat_id_list
	,poi_typ_id_list
	,company_id
	,company_group_id
	,company_unit
	,company_brand
	,bezeichnung_lang
	,adresse
	,plz4
	,ort
	,url
	,st_transform(st_setsrid(geo_point_lv95,2056), 21781)			as geo_point_lv03
	,st_transform(st_setsrid(geo_point_lv95,2056), 2056)			as geo_point_lv95
	,st_transform(st_setsrid(geo_point_lv95,2056), 4326)			as geo_point_wgs84
	,current_date													as gueltig_von
	,'9999-12-31'													as gueltig_bis
	,current_timestamp												as created_ts
	,current_timestamp												as updated_ts
	,current_timestamp												as last_check_ts 
from
	tmp_poi_new_insert
;
-- select * from geo_afo_prod.lay_poi_geo_2024_hist where extract(year from gueltig_bis) = 9999 and 6001001 = any(poi_typ_id_list);



