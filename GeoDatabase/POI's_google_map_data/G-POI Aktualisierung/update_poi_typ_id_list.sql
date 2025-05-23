

------------------------------------------------------
-- tmp-Tabelle mit allen POI-Typen pro POI
------------------------------------------------------
drop table if exists
	tmp_unnest;

create temp table
	tmp_unnest
as
select 
	t0.poi_id
	,t0.poi_typ_id
	,pt.poi_kat_id 
	,pk.poi_main_kat_id
from (
	select
		poi_id
		,unnest(poi_typ_id_list) as poi_typ_id
	from
		geo_afo_prod.lay_poi_geo_2024_hist
) t0
left join
	geo_afo_prod.meta_poi_typ_2024_hist pt
on 
	t0.poi_typ_id = pt.poi_typ_id
left join
	geo_afo_prod.meta_poi_kat_2024_hist pk
on
	pt.poi_kat_id = pk.poi_kat_id
;

------------------------------------------------------
-- poi_typ_id_list aktualisieren
------------------------------------------------------
update
	geo_afo_prod.lay_poi_geo_2024_hist r
set 
	poi_typ_id_list = u.poi_typ_id_list
from (
	select
		poi_id
		,array_agg(poi_typ_id) as poi_typ_id_list
	from (
		select distinct 
			poi_id
			,poi_typ_id
		from
			tmp_unnest
	) t0
	group by
		poi_id
) u
where
	extract(year from gueltig_bis) = 9999
	and
	r.poi_id = u.poi_id
;

------------------------------------------------------
-- poi_kat_id_list aktualisieren
------------------------------------------------------
update
	geo_afo_prod.lay_poi_geo_2024_hist r
set 
	poi_kat_id_list = u.poi_kat_id_list
from (
	select
		poi_id
		,array_agg(poi_kat_id) as poi_kat_id_list
	from (
		select distinct 
			poi_id
			,poi_kat_id
		from
			tmp_unnest
	) t0
	group by
		poi_id
) u
where
	extract(year from gueltig_bis) = 9999
	and
	r.poi_id = u.poi_id
;

------------------------------------------------------
-- poi_main_kat_id_list aktualisieren
------------------------------------------------------
update
	geo_afo_prod.lay_poi_geo_2024_hist r
set 
	poi_main_kat_id_list = u.poi_main_kat_id_list
from (
	select
		poi_id
		,array_agg(poi_main_kat_id) as poi_main_kat_id_list
	from (
		select distinct 
			poi_id
			,poi_main_kat_id
		from
			tmp_unnest
	) t0
	group by
		poi_id
) u
where
	extract(year from gueltig_bis) = 9999
	and
	r.poi_id = u.poi_id
;
