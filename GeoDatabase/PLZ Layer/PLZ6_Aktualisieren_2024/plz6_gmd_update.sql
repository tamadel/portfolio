--=============================== 
-- Clean source table 
--=============================== 
drop table if exists 
    geo_afo_prod.imp_plz6_geo_neu
        ; 
 
create table 
    geo_afo_prod.imp_plz6_geo_neu
as
select
    "ZIP_ID" as plz_id,
    "ZIP4"::numeric as plz,
    "ADDITIONAL" as plzzus,
    concat("ZIP4", "ADDITIONAL")::numeric as plz6,
    "MODIFIED" as modified,
    "SHAPE_AREA" as shape_area,
    ST_Area(ST_SetSRID(geometry, 2056)) as qm,
    ST_Area(ST_SetSRID(geometry, 2056)) as qkm,
    "SHAPE_LEN" as shape_len,
    ST_Multi(geometry) as geo_poly_lv95,
    ST_Transform(ST_SetSRID(ST_Multi(geometry), 2056), 21781) as geo_poly_lv03,  
    ST_Transform(ST_SetSRID(ST_Multi(geometry), 2056), 4326) as geo_poly_wgs84
from 
    geo_afo_prod.imp_plz6_geo_neu_python
;


select 
	*
from 
	geo_afo_prod.imp_plz6_geo_neu_python;

select * from geo_afo_prod.imp_plz6_geo_neu;
--==========================================
-- Create test table "test_lay_gmd_geo_hist"
--==========================================
drop table if exists 
   geo_afo_prod.test_lay_plz6_geo_hist
;
create table 
    geo_afo_prod.test_lay_plz6_geo_hist
as
select
    *
from 
    geo_afo_prod.lay_plz6_geo_hist
;

-----------------------------
-- Delete/deactivate old plz6
----------------------------- 
update 
	geo_afo_prod.test_lay_plz6_geo_hist
set 
    gueltig_bis = current_date 
where 
    plz6 in (
        select 
            plz6
        from 
            geo_afo_prod.test_lay_plz6_geo_hist
        where 
            plz6 not in (
                select 
                    plz6 
                from 
                    geo_afo_prod.imp_plz6_geo_neu
            )
            and 
            extract(year from gueltig_bis) = 9999
    )
    and  
    extract(year from gueltig_bis) = 9999
;

-----------------------------
-- insert new records
-----------------------------
insert into  
	geo_afo_prod.test_lay_plz6_geo_hist (plz6, gueltig_bis)
select   
    n.plz6, 
    '9999-12-31' AS gueltig_bis
from   
    geo_afo_prod.imp_plz6_geo_neu n
where   
    not exists (
        select 		
            n.plz6 
        from 
            geo_afo_prod.test_lay_plz6_geo_hist h
        where 
            h.plz6 = n.plz6
        and 
        extract(year from gueltig_bis) = 9999
    )
;

-----------------------------------------------------------------
-- replace old poly with new poly if perecentage_overlap < 0.995
-----------------------------------------------------------------
update 
	geo_afo_prod.test_lay_plz6_geo_hist h
set  
    geo_poly_lv95 = u.new_poly,
    geo_poly_lv03 = ST_Transform(u.new_poly, 21781),
    geo_poly_wgs84 = ST_Transform(u.new_poly, 4326),
    geo_point_lv95 = ST_Centroid(u.new_poly),
    geo_point_lv03 = ST_Transform(ST_Centroid(u.new_poly), 21781),
    geo_point_wgs84 = ST_Transform(ST_Centroid(u.new_poly), 4326), 
    gueltig_von = CURRENT_DATE
from (
    select  
        t0.plz6,
        ROUND(ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056))) as area_old,
        ROUND(ST_Area(ST_SetSRID(t1.geo_poly_lv95, 2056))) as area_new,
        ROUND(ST_Area(ST_Intersection(
            ST_SetSRID(t0.geo_poly_lv95, 2056),
            ST_SetSRID(t1.geo_poly_lv95, 2056)
        ))) as area_overlap,
        ST_SetSRID(t0.geo_poly_lv95, 2056) as old_poly,
        ST_SetSRID(t1.geo_poly_lv95, 2056) as new_poly
    from 
        {settings['GEO_PROD_SCHEMA']}.{settings['TEST_LAY_PLZ6_GEO_HIST']} t0
    join 
        {settings['GEO_PROD_SCHEMA']}.{settings['IMP_PLZ6_GEO_NEU']} t1
    on 
        t0.plz6 = t1.plz6
    where 
        extract(year from t0.gueltig_bis) = 9999
        and  
        (ST_Area(ST_Intersection(ST_SetSRID(t0.geo_poly_lv95, 2056), ST_SetSRID(t1.geo_poly_lv95, 2056))) / ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056))) < 0.995
) u
where  
    h.plz6 = u.plz6
    and  
    extract(year from h.gueltig_bis) = 9999 
;

















