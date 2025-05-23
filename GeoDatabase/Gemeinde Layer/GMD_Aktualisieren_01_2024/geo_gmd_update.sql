--=============================== 
-- Clean source table 
--=============================== 
--Create a clean table using PostGIS functions, such as ST_Force2D(), to convert "Multipolygon Z" into "Multipolygon".
--Use ST_Multi() in case the result is a single Polygon, since our "hist_tabelle" contains "Multipolygon".

drop table if exists 
    geo_afo_prod.imp_gmd_geo_neu
; 
                    
create table 
    geo_afo_prod.imp_gmd_geo_neu
as
select
    "ICC" as icc,
    "BFS_NUMMER" as gmd_nr,
    "NAME" as gemeinde,
    "KANTONSNUM" as kanton_nr,
    NULL as kanton,
    "BEZIRKSNUM" as bzr_nr,
    "EINWOHNERZ" as einwohnerz,
    "HIST_NR" as hist_nr,
    "HERKUNFT_J" as herkunft_j,
    "OBJEKTART" as objekt_art,
    "GEM_FLAECH" as gem_flaech,
    ST_Force2D(ST_SetSRID(ST_Multi(geometry), 2056)) as geo_poly_lv95,
    ST_Transform(ST_SetSRID(ST_Force2D(ST_Multi(geometry)), 2056), 21781) as geo_poly_lv03,  
    ST_Transform(ST_SetSRID(ST_Force2D(ST_Multi(geometry)), 2056), 4326) as geo_poly_wgs84
from 
   geo_afo_prod.imp_gmd_geo_neu_python
where 
    "ICC" in ('CH', 'LI')
    and
    "OBJEKTART" in ('Gemeindegebiet')
;

--==========================================
-- Create test table "test_lay_gmd_geo_hist"
--==========================================
drop table if exists 
   geo_afo_prod.test_lay_gmd_geo_hist
;
create table 
   geo_afo_prod.test_lay_gmd_geo_hist
as
select
    t0.*,
    t1.kantons_nr as kanton_nr
from 
    geo_afo_prod.lay_gmd_geo_hist  t0
left join
	geo_afo_prod.map_kanton_nr  t1
on
	t0.kanton = t1.kanton
;

select 
	*
from 
	geo_afo_prod.map_kanton_nr;
-----------------------------
-- Delete/deactivate old gmd
----------------------------- 

update
    geo_afo_prod.test_lay_gmd_geo_hist
set
    gueltig_bis = current_date 
where
    gmd_nr in(
                select
                    gmd_nr
                from
                    geo_afo_prod.test_lay_gmd_geo_hist
                where
                    gmd_nr not in(
                                select
                                    gmd_nr 
                                from
                                    geo_afo_prod.imp_gmd_geo_neu
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
    geo_afo_prod.test_lay_gmd_geo_hist( 
    	gmd_nr,
        gemeinde,
        kanton,
        --kanton_nr,
        geo_poly_lv03,
        geo_poly_lv95,
        geo_poly_wgs84,
        gueltig_von,
        gueltig_bis,
        created_ts,
        updated_ts
    )
select  
    n.gmd_nr,
    n.gemeinde,
    n.kanton,
    n.kanton_nr,
    n.geo_poly_lv03,   
    n.geo_poly_lv95,     
    n.geo_poly_wgs84, 
    current_date as gueltig_von,
    '9999-12-31' as gueltig_bis,
    current_timestamp as created_ts,
    current_timestamp as updated_ts
from  
    geo_afo_prod.imp_gmd_geo_neu n
where  
    not exists (
                select		
                    n.gmd_nr 
                from
                    geo_afo_prod.test_lay_gmd_geo_hist h
                where
                    h.gmd_nr = n.gmd_nr
                    and
                    extract(year from gueltig_bis) = 9999
    );


--===================================================================
-- replace old poly with new poly
--===================================================================
   
-----------------------------------------------------------------
-- Step 1: Create temp table with proper SRID transformation
-----------------------------------------------------------------

drop table if exists tmp_perce_overlap;
create temp table tmp_perce_overlap
as
select
    t0.gid,
    t0.gmd_nr,
    t0.gemeinde,
    t0.kanton,
    t0.kanton_nr,
    t1.objekt_art,
    -- Ensure all geometries are in SRID 2056 (or another common SRID)
    ST_SetSRID(t0.geo_poly_lv95, 2056) as old_poly_lv95,
    ST_SetSRID(t1.geo_poly_lv95, 2056) as new_poly_lv95,
    t0.geo_poly_lv03 as old_poly_lv03,
    ST_Transform(ST_SetSRID(t1.geo_poly_lv95, 2056), 21781) as new_poly_lv03,
    t0.geo_poly_wgs84 as old_poly_wgs84,
    ST_Transform(ST_SetSRID(t1.geo_poly_lv95, 2056), 4326) as new_poly_wgs84,
    t0.gueltig_von,
    t0.gueltig_bis,
    t0.created_ts,
    t0.updated_ts,
    round(ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056))) as area_old,
    round(ST_Area(ST_SetSRID(t1.geo_poly_lv95, 2056))) as area_new,
    round(ST_Area(ST_Intersection(
                    ST_SetSRID(t0.geo_poly_lv95, 2056),
                    ST_SetSRID(t1.geo_poly_lv95, 2056)
                )
            )
         ) as area_overlap,
    ST_Area(ST_Intersection(ST_SetSRID(t0.geo_poly_lv95, 2056), ST_SetSRID(t1.geo_poly_lv95, 2056))) / ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056)) as percentage_overlap
from
    geo_afo_prod.test_lay_gmd_geo_hist t0 
join
    geo_afo_prod.imp_gmd_geo_neu t1
on
    t0.gmd_nr = t1.gmd_nr
where
    extract(year from t0.gueltig_bis) = 9999;

-----------------------------------------------------------------
-- Step 2: Update polygons where overlap is ≥ 99.5%
-----------------------------------------------------------------

update
    geo_afo_prod.test_lay_gmd_geo_hist h
set 
    geo_poly_lv95 = u.new_poly_lv95,
    geo_poly_lv03 = u.new_poly_lv03,
    geo_poly_wgs84 = u.new_poly_wgs84,
    gueltig_von = current_date,
    updated_ts = current_timestamp 
from 
    tmp_perce_overlap u
where 
    u.percentage_overlap >= 0.995
    and
    h.gmd_nr = u.gmd_nr
    and
    extract(year from h.gueltig_bis) = 9999;
   
   
-----------------------------------------------------------------
-- Step 3: Deactivate polygons where overlap is < 99.5%
-----------------------------------------------------------------

update
    geo_afo_prod.test_lay_gmd_geo_hist h
set 
    gueltig_bis = current_date,
    updated_ts = current_timestamp 
from 
    tmp_perce_overlap u
where 
    u.percentage_overlap < 0.995
    and
    h.gmd_nr = u.gmd_nr
    and 
    extract(year from h.gueltig_bis) = 9999;
   
-----------------------------------------------------------------
-- Step 4: Insert new polygons where overlap < 99.5%
-----------------------------------------------------------------

insert into 
    geo_afo_prod.test_lay_gmd_geo_hist(
        gmd_nr,
        gemeinde,
        kanton,
        kanton_nr,
        geo_poly_lv03,
        geo_poly_lv95,
        geo_poly_wgs84,
        gueltig_von,
        gueltig_bis,
        created_ts,
        updated_ts
    )
select  
    n.gmd_nr,
    n.gemeinde,
    n.kanton,
    n.kanton_nr,
    n.new_poly_lv03,
    n.new_poly_lv95,
    n.new_poly_wgs84,
    current_date as gueltig_von,
    '9999-12-31' as gueltig_bis,
    current_timestamp as created_ts,
    current_timestamp as updated_ts
from  
    tmp_perce_overlap n
where
    n.percentage_overlap < 0.995;
   
   
select 
    h.gmd_nr,
    ST_Area(ST_Intersection(ST_SetSRID(h.geo_poly_lv95, 2056), ST_SetSRID(n.geo_poly_lv95, 2056))) / 
    ST_Area(ST_SetSRID(n.geo_poly_lv95, 2056)) as percentage_overlap
from
    geo_afo_prod.test_lay_gmd_geo_hist h
join 
    geo_afo_prod.imp_gmd_geo_neu n
on
    h.gmd_nr = n.gmd_nr
where
    extract(year from h.gueltig_bis) = 9999;

   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
  
--=======================================
-- Kanton map falls notig ist 
--======================================
/*
drop table if exists geo_afo_prod.map_kanton_nr;
create table geo_afo_prod.map_kanton_nr
as
select distinct
	kantons_nr,
	kanton
from 
	sanitas.ms_regionen 
order by
	kantons_nr
;

select 
	*
from 
	geo_afo_prod.map_kanton_nr
;
*/ 
   
   
   
   
   
   
   
   








