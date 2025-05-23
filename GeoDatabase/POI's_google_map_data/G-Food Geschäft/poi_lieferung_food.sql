

--------------------------------------------------------------
-- geo_point_lv95 in Geometry umwandeln
--------------------------------------------------------------
alter table 
	google_maps_dev_abgleich.poi_lieferung_food
rename column
	geo_point_lv95 to geo_point_lv95_str
;

alter table 
	google_maps_dev_abgleich.poi_lieferung_food
add column
	geo_point_lv95 geometry(POINT, 2056)
;

update
	google_maps_dev_abgleich.poi_lieferung_food
set
	geo_point_lv95 = st_geomfromtext(geo_point_lv95_str)
where 	
	1=1
;

alter table
	google_maps_dev_abgleich.poi_lieferung_food
drop column
	geo_point_lv95_str
;

--=======================================================================
-- company_group_id / company_group aktualisieren
--=======================================================================
update
	google_maps_dev_abgleich.poi_lieferung_food r
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
	google_maps_dev_abgleich.poi_lieferung_food r
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
-- Auszählungen
--=======================================================================
select
	*
from
	google_maps_dev_abgleich.poi_lieferung_food
;

select 
	poi_typ
	,count(*)
from
	google_maps_dev_abgleich.poi_lieferung_food
group by
	poi_typ
order by
	poi_typ
;

select 
	company
	,count(*)
from
	google_maps_dev_abgleich.poi_lieferung_food
group by
	company
order by
	company
;

select distinct
	company_group
from
	geo_afo_prod.mv_lay_poi_aktuell 
where
	poi_typ = 'Food'
order by 
	company_group
;


select
	company
	,count(*)
from
	geo_afo_prod.mv_lay_poi_aktuell 
where
	poi_typ = 'Food'
	and 
	company in (
		'Aldi Suisse'
		,'Aligro'
		,'Coop'
		,'Coop City'
		,'Denner'
		,'Denner Discount'
		,'Globus-Warenhaus'
		,'Landi Laden'
		,'Lidl Schweiz'
		,'Manor'
		,'Migros'
		,'Migros Partner'
		,'Prodega'
		,'Spar'
		,'TopCC'
		,'Treffpunkt'
		,'Volg Konsumwaren'
	)
group by
	company
order by
	company
;

select
	*
from
	geo_afo_prod.mv_lay_poi_aktuell
where
	company = 'Spar'
	and
	plz4 = 8105
;

select
	*
from
	geo_afo_prod.mv_lay_poi_aktuell
where
	poi_typ = 'Food'
	and
	company = 'Spar'
	and
	poi_id::text not in (
		select
			poi_id::text
		from
			google_maps_dev_abgleich.poi_lieferung_food
		where
			company = 'Spar'
	)
;

select distinct
	company_brand
from
	google_maps_dev_abgleich.poi_lieferung_food
order by
	company_brand 
;


select distinct
    company_group,
    company,
    company_unit,
    company_brand
from
	google_maps_dev_abgleich.poi_abgleich_google_food_tot_clean
where
	poi_typ_id = 202
order by
	company_brand 
;

select
	*
from
	geo_afo_prod.meta_poi_typ_2024_hist 
;

SELECT 
    DISTINCT
        company_group,
        company,
        company_unit,
        company_brand
FROM 
    geo_afo_prod.mv_lay_poi_aktuell 
WHERE
    poi_typ_id = 202
AND 
    company NOT IN ('Brezelkönig','Caffè Spettacolo','k kiosk','Press & Books')
ORDER BY 
    company_group ASC, company ASC;