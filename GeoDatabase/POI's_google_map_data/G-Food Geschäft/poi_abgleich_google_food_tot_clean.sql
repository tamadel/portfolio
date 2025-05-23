
drop table if exists
	google_maps_dev_abgleich.poi_abgleich_google_food_tot;

create table
	google_maps_dev_abgleich.poi_abgleich_google_food_tot
as
SELECT 
	*
FROM 
	dblink(
		'redshift',
		$REDSHIFT$
			select
				poi_id
				,hauskey
				,poi_typ_id
				,poi_typ
				,google_poi_typ
				,category_ids
				,company_group_id
				,company_group
				,company_id
				,company
				,company_unit
				,company_brand
				,bezeichnung_lang
				,bezeichnung_kurz
				,adresse
				,adress_lang
				,plz4
				,plz4_orig
				,ort
				,google_strasse
				,google_strasse_std
				,google_hausnum
				,google_plz4
				,google_ort
				,gwr_strasse
				,gwr_hausnum
				,gwr_plz4
				,gwr_ort
				,plz6
				,gemeinde
				,gmd_nr
				,url
				,"domain"
				,geo_point_lv95
				,quelle
				,dubletten_nr
			FROM 
				geo_afo_prod.poi_abgleich_google_food_tot
		$REDSHIFT$
	) AS poi_abgleich_google_food_tot (
		poi_id text
		,hauskey numeric
		,poi_typ_id numeric
		,poi_typ text
		,google_poi_typ text
		,category_ids text
		,company_group_id numeric
		,company_group text
		,company_id numeric
		,company text
		,company_unit text
		,company_brand text
		,bezeichnung_lang text
		,bezeichnung_kurz text
		,adresse text
		,adress_lang text
		,plz4 numeric
		,plz4_orig text
		,ort text
		,google_strasse text
		,google_strasse_std text
		,google_hausnum text
		,google_plz4 text
		,google_ort text
		,gwr_strasse text
		,gwr_hausnum text
		,gwr_plz4 int
		,gwr_ort text
		,plz6 text
		,gemeinde text
		,gmd_nr text
		,url text
		,"domain" text
		,geo_point_lv95 public.geometry(point, 2056)
		,quelle text
		,dubletten_nr text
	)
;


drop table if exists
	google_maps_dev_abgleich.poi_abgleich_google_food_tot_clean;

create table
	google_maps_dev_abgleich.poi_abgleich_google_food_tot_clean
as
select 
	case
		when position('.' in poi_id::text) > 0 then substring(poi_id::text, 1, position('.' in poi_id::text)-1)
		else poi_id::text
	end as poi_id
	,hauskey
	,poi_typ_id
	,poi_typ
	,google_poi_typ
	,category_ids
	,company_group_id
	,company_group
	,company_id
	,company
	,company_unit
	,company_brand
	,bezeichnung_lang
	,bezeichnung_kurz
	,adresse
	,adress_lang
	,plz4
	,plz4_orig::float::int
	,ort
	,google_strasse
	,google_strasse_std
	,google_hausnum
	,google_plz4::text
	,google_ort
	,gwr_strasse
	,gwr_hausnum
	,gwr_plz4
	,gwr_ort
	,plz6
	,gemeinde
	,case
		when position('.' in gmd_nr::text) > 0 then substring(gmd_nr::text, 1, position('.' in gmd_nr::text)-1)::int
		else null
	end as gmd_nr
	,url
	,"domain"
	,geo_point_lv95
	,quelle
	,dubletten_nr
FROM 
	google_maps_dev_abgleich.poi_abgleich_google_food_tot
;

select 
	*
from
	google_maps_dev_abgleich.poi_abgleich_google_food_tot_clean
;

select 
	*
from
	google_maps_dev_abgleich.poi_abgleich_google_food_tot_clean
where 	
	poi_id like '140823621820%'
;

