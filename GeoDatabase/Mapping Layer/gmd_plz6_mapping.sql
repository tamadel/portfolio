--===============================
-- Project: Update HKT100 data
-- Date: 23.01.2025
-- Tamer
--===============================
-- Data layer in serverless 
-------------------
--(I) Gemeinde Neu 
-------------------
--geo_afo_prod.imp_gmd_geo_neu
--geo_afo_prod.imp_gmd_geo_neu_all
--geo_afo_prod.imp_gmd_geo_neu_manual

drop table if exists
	geo_afo_prod.imp_gmd_geo_neu;
create table
	geo_afo_prod.imp_gmd_geo_neu
as
select  
	*
from  
	dblink(
		'geo_database',
		$POSTGRES$
			select
				* 
			from
				geo_afo_prod.imp_gmd_geo_neu
		$POSTGRES$
	) as imp_gmd_geo_neu (
			icc text 
			,gmd_nr int8 
			,gemeinde text 
			,kanton_nr float8 
			,kanton text 
			,bzr_nr float8 
			,einwohnerz int8 
			,hist_nr float8 
			,herkunft_j int8 
			,objekt_art text 
			,gem_flaech float8 
			,geo_poly_lv95 public.geometry 
			,geo_poly_lv03 public.geometry 
			,geo_poly_wgs84 public.geometry
	)
;


select * from geo_afo_prod.imp_gmd_geo_neu;

------------------------------------------
-- GMD layer nach 01.01.2025 aktulisieren 
------------------------------------------
-- geo_afo_prod.imp_gmd_geo_01_2025 nach Serverless migrieren.

drop table if exists
	geo_afo_prod.imp_gmd_geo_01_2025;
create table
	geo_afo_prod.imp_gmd_geo_01_2025
as
select  
	*
from  
	dblink(
		'geo_database',
		$POSTGRES$
			select
				* 
			from
				geo_afo_prod.imp_gmd_geo_01_2025
		$POSTGRES$
	) as imp_gmd_geo_01_2025 (
				gid int4 
				,datum_aend date 
				,datum_erst date 
				,herkunft_j float8 
				,objekt_art varchar(20) 
				,gmd_nr float8 
				,bzr_nr float8 
				,kanton_nr float8 
				,gemeinde varchar(100) 
				,gem_flaech numeric 
				,see_flaech numeric 
				,icc varchar(20) 
				,einwohnerz float8 
				,hist_nr float8 
				,geo_poly_lv95 public.geometry 
				,geo_poly_lv03 public.geometry 
				,geo_poly_wgs84 public.geometry
	)
;

-- test
select 
	* 
from 
	geo_afo_prod.imp_gmd_geo_01_2025
where 
	gmd_nr not in(
					select 
						gmd_nr 
					from
						geo_afo_prod.imp_hkt100_aktuell_gmd_2025 --imp_gmd_geo_neu 
	)
;	
-- GMD-Mutation von 01.01.2024 bis 01.01.2025
-- 6513	Laténa
--alt  neu
--2217 2239 	Grolley-Ponthaux
--2200 2239 	Grolley-Ponthaux
--6453 6513 	Laténa
--6454 6513		Laténa
--6459 6513		Laténa
--6461 6513		Laténa

select 
	*
from
	geo_afo.gmd_bfs_mutationen
where 
	gmd_neu in (6513, 2239)
;

--test
select 
	*
from 
	geo_afo_prod.imp_hkt100_aktuell_neu_gmd
where 
	gmd_nr not in(
					select 
						gmd_nr 
					from
						geo_afo_prod.imp_gmd_geo_01_2025
	)
;



----------------
--(II) plz6 Neu 
----------------
drop table if exists
	geo_afo_prod.imp_plz6_geo_neu

create table
	geo_afo_prod.imp_plz6_geo_neu
as
select  
	*
from  
	dblink(
		'geo_database',
		$POSTGRES$
			select
				* 
			from
				geo_afo_prod.imp_plz6_geo_neu
		$POSTGRES$
	) as imp_plz6_geo_neu (
			gid text 
			,plz_id text 
			,plz numeric 
			,plzzus text 
			,plz6 numeric 
			,modified text 
			,shape_area text 
			,qm float8 
			,qkm float8 
			,shape_len float8 
			,geo_poly_lv95 public.geometry 
			,geo_poly_lv03 public.geometry 
			,geo_poly_wgs84 public.geometry 
			,geo_point_lv95 public.geometry 
			,geo_point_lv03 public.geometry 
			,geo_point_wgs84 public.geometry 
			,plz_ort text
	)
;

select * from geo_afo_prod.imp_plz6_geo_neu;


--=============================
-- Intersection and Areas 
--=============================
--indexing
drop index if exists 
	geo_afo_prod.idx_plz6_geo_poly_lv95;
create index 
	idx_plz6_geo_poly_lv95 on geo_afo_prod.imp_plz6_geo_neu using GIST(geo_poly_lv95);


drop index if exists 
	geo_afo_prod.idx_gemeinde_geo_poly_lv95;
create index 
	idx_gemeinde_geo_poly_lv95 on geo_afo_prod.imp_gmd_geo_01_2025 using GIST(geo_poly_lv95);

select * from geo_afo_prod.imp_plz6_geo_neu; --4072

select * from geo_afo_prod.imp_gmd_geo_01_2025; --2132


-----------------------
-- (I) First approach 
-----------------------
-- Straightforward, without filtering the significant intersection area.

drop table if exists 
	geo_afo_tmp.tmp_plz_gmd_mapping;
create table 
	geo_afo_tmp.tmp_plz_gmd_mapping 
as  
select   
    pl.plz6,
    pl.plz,
    pl.plz_ort 																					as ort,
    pl.geo_poly_lv95 																			as geo_poly_plz_lv95,
    pl.geo_poly_wgs84 																			as geo_poly_plz_wgs84,
    pl.geo_poly_lv03 																			as geo_poly_plz_lv03,
    gm.gemeinde,
    gm.gmd_nr,
    gm.bzr_nr,
    gm.kanton_nr,
    gm.geo_poly_lv95 																			as geo_poly_gmd_lv95,
    gm.geo_poly_lv03 																			as geo_poly_gmd_lv03,
    gm.geo_poly_wgs84 																			as geo_poly_gmd_wgs84,
    ST_Intersection(pl.geo_poly_lv95, gm.geo_poly_lv95) 										as intersection_geometry,
    ST_Area(ST_Intersection(pl.geo_poly_lv95, gm.geo_poly_lv95)) 								as intersection_area,
    ST_Area(pl.geo_poly_lv95) 																	as plz_area,
    ST_Area(ST_Intersection(pl.geo_poly_lv95, gm.geo_poly_lv95)) / ST_Area(pl.geo_poly_lv95) 	as plz_area_ratio,
    ST_Area(gm.geo_poly_lv95) 																	as gemeinde_area,
    ST_Area(ST_Intersection(pl.geo_poly_lv95, gm.geo_poly_lv95)) / ST_Area(gm.geo_poly_lv95)	as gmd_area_ratio,
    gm.icc
from  
	geo_afo_prod.imp_plz6_geo_neu pl
join
	geo_afo_prod.imp_gmd_geo_01_2025 gm --aktuell gmd 01.01.2025
on
	ST_Intersects(pl.geo_poly_lv95, gm.geo_poly_lv95)
;


--(intersection_area / SUM(intersection_area)) * 100 											as anteil_plz6_relative_to_total,
--(intersection_area / gemeinde_area) * 100 													as anteil_plz6_fm_gmd,

select  
	gemeinde, 
	plz6,
	plz_area_ratio,
	geo_poly_gmd_lv95,
	geo_poly_plz_lv95
from
	geo_afo_tmp.tmp_plz_gmd_mapping 
where
	plz6 = 852502
order by 
	plz_area_ratio desc 
;


select  
	plz6,
	gemeinde,
	gmd_area_ratio,
	geo_poly_gmd_lv95,
	geo_poly_plz_lv95
from
	geo_afo_tmp.tmp_plz_gmd_mapping 
where
	gemeinde = 'Neunforn'
order by 
	gmd_area_ratio desc
;


select * from geo_afo_tmp.tmp_plz_gmd_mapping ;





-----------------------
-- (II) Second approach
-----------------------   
--------------------------------------------
-- Create temporary table for Intersections
--------------------------------------------
drop table if exists tmp_intersections;
create temp table tmp_intersections 
as 
select   
    pl.plz6,
    pl.plz,
    pl.plz_ort 																					as ort,
    pl.geo_poly_lv95 																			as geo_poly_plz_lv95,
    pl.geo_poly_wgs84 																			as geo_poly_plz_wgs84,
    pl.geo_poly_lv03 																			as geo_poly_plz_lv03,
    gm.gemeinde,
    gm.gmd_nr,
    gm.bzr_nr,
    gm.kanton_nr,
    gm.geo_poly_lv95 																			as geo_poly_gmd_lv95,
    gm.geo_poly_lv03 																			as geo_poly_gmd_lv03,
    gm.geo_poly_wgs84 																			as geo_poly_gmd_wgs84,
    ST_Intersection(pl.geo_poly_lv95, gm.geo_poly_lv95) 										as intersection_geometry,
    ST_Area(ST_Intersection(pl.geo_poly_lv95, gm.geo_poly_lv95)) 								as intersection_area,
    ST_Area(pl.geo_poly_lv95) 																	as plz_area,
    ST_Area(ST_Intersection(pl.geo_poly_lv95, gm.geo_poly_lv95)) / ST_Area(pl.geo_poly_lv95) 	as plz_area_ratio,
    ST_Area(gm.geo_poly_lv95) 																	as gemeinde_area,
    ST_Area(ST_Intersection(pl.geo_poly_lv95, gm.geo_poly_lv95)) / ST_Area(gm.geo_poly_lv95)  	as gmd_area_ratio,
    gm.icc
from  
	geo_afo_prod.imp_plz6_geo_neu pl
join
	geo_afo_prod.imp_gmd_geo_01_2025 gm --aktuell gmd 01.01.2025
on
	ST_Intersects(pl.geo_poly_lv95, gm.geo_poly_lv95)
;


 
---------------------------------------------------
-- Create temporary table for Filtered Intersections
---------------------------------------------------
-- Removing empty intersections
-- Filter out empty intersection geometries and zero area intersections

drop table if exists tmp_filtered_intersections;
create temp table tmp_filtered_intersections 
as 
select
	*
from
	tmp_intersections
where
	not ST_IsEmpty(intersection_geometry) 
	and
	intersection_area > 0
;


---------------------------------------------------
-- Create temporary table for significant Intersections
---------------------------------------------------
-- Filtering intersections based on a minimum area beginning.(significant intersection area)
-- Keep intersections where the area is at least 0.01% of the larger of plz or gemeinde area.

drop table if exists tmp_significant_intersections;
create temp table tmp_significant_intersections 
as 
select
	*
from
	tmp_filtered_intersections
where
	intersection_area >= 0.0001 * GREATEST(plz_area, gemeinde_area)
;

-- test case >> plz6 = 852502 
-- with filter plz6 distrbuted on 2 gmd "Stammheim and Neunforn"
-- without filiter plz6 distrbuted on 4 gmd "Uesslingen-Buch, Hüttwilen, Stammheim, Neunforn"		 

select count(distinct plz6) from tmp_significant_intersections; --4072
select count(distinct gmd_nr) from tmp_significant_intersections; --2132
------------------------------------------------------
-- Create temporary table for Plz6 Intersection Summary
------------------------------------------------------
-- Calculate total intersection area for each plz6.
-- Sum the intersection areas for each plz6 to get the total intersected area.

drop table if exists tmp_plz6_intersection;
create temp table tmp_plz6_intersection 
as 
select  
    plz6,
    plz,
    ort,
    SUM(intersection_area) as total_intersection_area
from
 	tmp_significant_intersections
group by 
	plz6, 
	plz, 
	ort
;

-------------------------------------------------------------------------------------
-- Create the final table with calculated values
-------------------------------------------------------------------------------------
-- Calculate the percentage of the intersection area relative to the total intersection area for the plz6.
-- Calculate the percentage share of the intersection area relative to the gemeinde area.
-- Calculate the percentage of the plz6 area covered by all intersecting gemeinde.

drop table if exists geo_afo_prod.imp_plz6_gmd_aktuell; 
create table geo_afo_prod.imp_plz_gmd_mapping
as   
select  
	ci.gmd_nr,
	ci.gemeinde,
    ci.plz6,
    (ci.intersection_area / pis.total_intersection_area) * 100   	as anteil_plz6_relative_to_total,
    (ci.intersection_area / ci.gemeinde_area) * 100 				as anteil_plz6_fm_gmd,
    (pis.total_intersection_area / ci.plz_area) * 100 				as percentage_plz6_covered,    
    ci.geo_poly_plz_lv95,
    ci.geo_poly_gmd_lv95,
    ci.intersection_geometry,
    ci.intersection_area,
    ci.plz_area,
    ci.plz_area_ratio,
    ci.gemeinde_area,
    ci.gmd_area_ratio
from
	tmp_significant_intersections ci
join
	tmp_plz6_intersection pis 
on
	ci.plz6 = pis.plz6
;



select * from geo_afo_prod.imp_plz_gmd_mapping  order by plz6, gemeinde;
select * from geo_afo_prod.imp_plz6_gmd_aktuell  order by plz6, gemeinde;



select count(distinct plz6) from geo_afo_prod.imp_plz6_gmd; 	 	--4072
select count(distinct gmd_nr) from geo_afo_prod.imp_plz6_gmd;  		--2142



select 
	plz6 
from 
	geo_afo_prod.imp_plz6_geo_neu
where 
	plz6 not in (
				select 
					plz6 
				from 
					geo_afo_prod.imp_plz_gmd_mapping
	)
;




select * from geo_afo_prod.imp_plz_gmd_mapping;


 select
   	plz6
   	,count(*)
    ,array_agg(gemeinde) as gemeinde
    ,array_agg(anteil_plz6_relative_to_total) as anteil_plz6_rel_tot
  	,array_agg(anteil_plz6_fm_gmd) as anteil_plz6_gmd
from
   	geo_afo_prod.imp_plz_gmd_mapping
group by
   	plz6
order by 
	count(*) desc
;  


select
	gmd_nr
   	,gemeinde
   	,count(*) as anz_plz6
   	,array_agg(plz6) as plz6
from
   	geo_afo_prod.imp_plz_gmd_mapping
group by
   	gemeinde,
   	gmd_nr
order by
	anz_plz6 desc
;   
   






































--///////////////////////////////////
-- Draft 
--//////////////////////////////////


create temp table tmp_gmd_plz6_v1
as
select  
    pl.plz6,
    gm.gemeinde,
    pl.geo_poly_lv95 as geo_poly_lv95_plz6,
    gm.geo_poly_lv95 as geo_poly_lv95_gmd
from
	geo_afo_prod.imp_plz6_geo_neu as pl
join
	geo_afo_prod.imp_gmd_geo_neu as gm
on
	ST_Intersects(pl.geo_poly_lv95, gm.geo_poly_lv95)
order by 
	pl.plz6, gm.gemeinde
;



select * from tmp_gmd_plz6_v1;

 select
   	plz6
   	,count(*)
from
   	tmp_gmd_plz6_v1
group by
   	plz6
;  


select
   	gemeinde
   	,count(*)
from
   	tmp_gmd_plz6_v1
group by
   	gemeinde
;   


--============================
-- gmd with plz vice versa
--============================

select * from geo_afo_prod.imp_plz6_geo_neu; -- 4072

select * from geo_afo_prod.imp_gmd_geo_neu; --2142 / ohne LI 2131

drop table if exists geo_afo_tmp.tmp_gmd_plz6_v2;
create table geo_afo_tmp.tmp_gmd_plz6_v2
as
with intersections as (
    select  
        pl.plz6,
        pl.plz,
        pl.plz_ort 														as ort,
        pl.geo_poly_lv95 												as geo_poly_plz_lv95,
        pl.geo_poly_wgs84 												as geo_poly_plz_wgs84,
        pl.geo_poly_lv03 												as geo_poly_plz_lv03,
        gm.gemeinde,
        gm.gmd_nr,
        gm.kanton,
        gm.geo_poly_lv95 												as geo_poly_gmd_lv95,
        gm.geo_poly_lv03 												as geo_poly_gmd_lv03,
        gm.geo_poly_wgs84 												as geo_poly_gmd_wgs84,
        ST_Intersection(pl.geo_poly_lv95, gm.geo_poly_lv95) 			as intersection_geometry,
        ST_Area(ST_Intersection(pl.geo_poly_lv95, gm.geo_poly_lv95)) 	as intersection_area,
        ST_Area(pl.geo_poly_lv95) 										as plz_area,
        ST_Area(gm.geo_poly_lv95) 										as gemeinde_area
    from
    	geo_afo_prod.imp_plz6_geo_neu pl
    join
    	geo_afo_prod.imp_gmd_geo_neu gm
    on
    	ST_Intersects(pl.geo_poly_lv95, gm.geo_poly_lv95)
),
filtered_intersections as (
    select
    	*
    from
    	intersections
    where
    	not ST_IsEmpty(intersection_geometry) 
    	and 
    	intersection_area > 0
),
critical_intersections as ( --
    select
		*
    from
    	filtered_intersections
    where   --  checks if the intersection area is at least 1% of the larger of the plz_area or gemeinde_area
    	intersection_area >= 0.0005 * GREATEST(plz_area, gemeinde_area) --(adjust as needed)
),
calculated_percentages as (
    select 
		*,
       	(intersection_area / gemeinde_area) * 100 as anteil_plz6  -- Calculate percentage
    from
    	critical_intersections
)
select  
    gmd_nr,
    gemeinde,
    plz6,
    anteil_plz6,
    plz,
    geo_poly_plz_lv95,
    geo_poly_gmd_lv95
from
	calculated_percentages
order by 
	plz6, 
	gemeinde
;


 -- It allows us to specify what proportion of the plz6 or Gemeinde needs to overlap to be considered.  
 -- This gives us much more control and accuracy.




select sum(anteil_plz6) from geo_afo_tmp.tmp_gmd_plz6_v2 where plz6 = 852502;

select count(distinct plz6) from geo_afo_tmp.tmp_gmd_plz6_v1; 	 --4041
select count(distinct gmd_nr) from geo_afo_tmp.tmp_gmd_plz6_v1;  --2142


select 
	plz6 
from 
	geo_afo_prod.imp_plz6_geo_neu
where 
	plz6 not in (
				select 
					plz6 
				from 
					geo_afo_tmp.tmp_gmd_plz6_v1
	)
;



 select
   	plz6
   	,count(*)
from
   	geo_afo_tmp.tmp_gmd_plz6_v1
group by
   	plz6
;  


select
   	gemeinde
   	,count(*)
from
   	geo_afo_tmp.tmp_gmd_plz6_v1
group by
   	gemeinde
;


/*
create table geo_afo_prod.imp_plz6_gmd_test
as
select  
	gmd_nr,
	gemeinde,
    plz6,
    case  
        when
        	SUM(intersection_area) = 0 then 0  -- Handle division by zero for total intersection area
        else
        	(intersection_area / SUM(intersection_area)) * 100
    end as anteil_plz6_relative_to_total,
    case 
        when
        	gemeinde_area = 0 then 0  -- Handle division by zero for gemeinde_area
        else
        	(intersection_area / gemeinde_area) * 100
    end as anteil_plz6_fm_gmd,
    case 
        when
        	plz_area = 0 then 0  -- Handle division by zero for plz_area
        else
        	(SUM(intersection_area) / plz_area) * 100
    end as percentage_plz6_covered,
    geo_poly_plz_lv95,
    geo_poly_gmd_lv95,
    intersection_geometry,
    intersection_area,
    plz_area,
    gemeinde_area
from
	tmp_intersections
group by
	gmd_nr,
	gemeinde,
	plz6,
	geo_poly_plz_lv95,
    geo_poly_gmd_lv95,
    intersection_geometry,
    intersection_area,
    plz_area,
    gemeinde_area
;
*/
    
    
    
    