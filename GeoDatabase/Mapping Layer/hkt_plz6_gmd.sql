--==============================================
-- Project name: hkt100_plz6_gmd
-- Quellen: updated hkt and imp_plz_gmd_mapping
-- Date: 13.02.2025
-- update: 21.02.2025
--==============================================
--Quellen
select * from geo_afo_prod.imp_hkt100_gmd_aktuell_im2025; 	-- statpop und statent auf gemeinde ebene 
select * from geo_afo_prod.imp_hkt100_aktuell_im2025; 		-- statpop und statent auf hkt ebene 
select * from geo_afo_prod.imp_plz_gmd_mapping;				-- basis layer mit gemeinde und plz6 
------------------------------------
-- hkt aktuell mit plz6_gmd layer
------------------------------------
drop index if exists geo_afo_prod.idx_mitl_geo_point_lv95;
create index idx_mitl_geo_point_lv95 on geo_afo_prod.imp_hkt100_aktuell_im2025 using GIST(mitl_geo_point_lv95);

drop index if exists geo_afo_prod.idx_geo_poly_gmd_lv95;
create index idx_geo_poly_gmd_lv95 on geo_afo_prod.imp_plz_gmd_mapping using GIST(geo_poly_gmd_lv95);

drop index if exists geo_afo_prod.idx_geo_poly_plz_lv95;
create index idx_geo_poly_plz_lv95 on geo_afo_prod.imp_plz_gmd_mapping using GIST(geo_poly_plz_lv95);

-------------------------------
-- HKT auf Gemeinde PLZ6 Ebene
-------------------------------
-- test "geo_afo_tmp.tmp_hkt_auf_plz_gmd_layer_all"
-- 347108

drop index if exists 
	geo_afo_prod.idx_imp_plz_gmd_mapping_intersection_geometry; 
create index 
	idx_imp_plz_gmd_mapping_intersection_geometry 		
on 
	geo_afo_prod.imp_plz_gmd_mapping 		
using 
	GIST (intersection_geometry)
;

drop index if exists 
	geo_afo_prod.idx_imp_hkt100_aktuell_im2025_mitl_geo_point_lv95; 
create index 
	idx_imp_hkt100_aktuell_im2025_mitl_geo_point_lv95 	
on 
	geo_afo_prod.imp_hkt100_aktuell_im2025 	
using 
	GIST (mitl_geo_point_lv95)
;



---------------------------------------------------------------------------------------
-- Query 1: Get Hector rasters whose middle point is within Gemeinde/PLZ6 (restricted)
---------------------------------------------------------------------------------------
drop table if exists tmp_hkt_middle_point;
create temp table 	
		tmp_hkt_middle_point 
as 
select distinct 
    t1.gmd_nr,
    t1.gemeinde,
    t1.plz6,
    t2.reli,
    t2.geo_poly_lv95 				as geo_poly_hkt_lv95,
    t1.geo_poly_gmd_lv95,
    t1.geo_poly_plz_lv95,
    t1.intersection_geometry
from 
    geo_afo_prod.imp_plz_gmd_mapping t1 
left join 
    geo_afo_prod.imp_hkt100_aktuell_im2025 t2
on 
    ST_Contains(t1.intersection_geometry, t2.mitl_geo_point_lv95)
;

select * from tmp_hkt_middle_point; 

--------------------------------------------------------
-- Query 2: Get "reli" not included in the first query
--------------------------------------------------------
drop table if exists tmp_hkt_not_in_middle_point;
create temp table 
	tmp_hkt_not_in_middle_point 
as 
select distinct
	t1.reli
from
	geo_afo_prod.imp_hkt100_aktuell_im2025 t1
where
	not exists (
			    select
			    	1
			    from
			    	tmp_hkt_middle_point t2
			    where
			    	t1.reli = t2.reli
	)
;

select * from tmp_hkt_not_in_middle_point;


----------------------------------------------------
-- Query 3: Include those "reli" if they intersect
----------------------------------------------------
drop table if exists tmp_hkt_intersect; 
create temp table 
	tmp_hkt_intersect 
as 
with intersection_areas as (
    select  
        t1.gmd_nr,
        t1.gemeinde,
        t1.plz6,
        t2.reli,
        t2.geo_poly_lv95 		as geo_poly_hkt_lv95,
        t1.geo_poly_gmd_lv95,
        t1.geo_poly_plz_lv95,
        t1.intersection_geometry,
        ST_Area(ST_Intersection(t1.intersection_geometry, t2.geo_poly_lv95)) AS intersection_area
    from  
        geo_afo_prod.imp_plz_gmd_mapping t1 
    join   
        geo_afo_prod.imp_hkt100_aktuell_im2025 t2
    on  
        ST_Intersects(t1.intersection_geometry, t2.geo_poly_lv95)
    where 
        t2.reli in (select reli from tmp_hkt_not_in_middle_point)
),
ranked_intersections as (
    select  
        gmd_nr,
        gemeinde,
        plz6,
        reli,
        geo_poly_hkt_lv95,
        geo_poly_gmd_lv95,
        geo_poly_plz_lv95,
        intersection_geometry,
       -- intersection_area,
        ROW_NUMBER() over (partition by reli order by intersection_area desc) as rn  -- Rank by intersection area
    from
    	intersection_areas
)
select  
    gmd_nr,
    gemeinde,
    plz6,
    reli,
    geo_poly_hkt_lv95,
    geo_poly_gmd_lv95,
    geo_poly_plz_lv95,
    intersection_geometry
from
	ranked_intersections
where
	rn = 1 ; -- Select only the top-ranked intersection for each reli



select * from tmp_hkt_intersect;

------------------------------------
-- Final view combining all results
------------------------------------
drop table if exists 
		geo_afo_prod.imp_hkt_auf_plz_gmd_layer;
create table 
		geo_afo_prod.imp_hkt_auf_plz_gmd_layer
as 
select * from tmp_hkt_middle_point
union all 
select * from tmp_hkt_intersect
;


select * from geo_afo_prod.imp_hkt_auf_plz_gmd_layer;





------------------------
-- Verification
------------------------


-- Verify Grenzgebiete
drop table if exists geo_afo_tmp.tmp_grenzgebiete_hkt;
create table geo_afo_tmp.tmp_grenzgebiete_hkt
as
select
	t1.reli,
	t1.mitl_geo_point_lv95,
	t1.geo_poly_lv95
from
	geo_afo_prod.imp_hkt100_aktuell_im2025 t1
where
	not	exists (
			    select
			    	1
			    from
			    	geo_afo_prod.imp_plz_gmd_mapping t2
			    where
			    	ST_Intersects(t2.intersection_geometry, t1.mitl_geo_point_lv95) 
			    	and 
			        ST_Contains(t2.intersection_geometry, t1.mitl_geo_point_lv95)
	)
;   
    
select * from geo_afo_tmp.tmp_grenzgebiete_hkt; -- 300


-- all 300 already exists in the final table 
select 
	*
from 
	geo_afo_tmp.tmp_grenzgebiete_hkt 
where 
	reli not in (
				select 
					reli 
				from
					geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v1
	)
;



-- verify numbers
select count(distinct reli) from geo_afo_prod.imp_hkt_auf_plz_gmd_layer;				-- 347408 correct
--select count(distinct reli) from geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v4; 			-- 347408 correct
select count(distinct reli) from geo_afo_prod.imp_hkt100_aktuell_im2025;				-- 347408


select count(distinct plz6) from geo_afo_prod.imp_hkt_auf_plz_gmd_layer;				-- 4072 correct
--select count(distinct plz6) from geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v4; 			-- 4072 correct
select count(distinct plz6) from geo_afo_prod.imp_plz_gmd_mapping;						-- 4072		


select count(distinct gmd_nr) from geo_afo_prod.imp_hkt_auf_plz_gmd_layer;				-- 2132 correct
--select count(distinct gmd_nr) from geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v4; 			-- 2132 correct
select count(distinct gmd_nr) from geo_afo_prod.imp_plz_gmd_mapping;	 				-- 2132 




select count(*) from geo_afo_prod.imp_hkt100_aktuell_im2025 where reli is null;			-- 0

select count(*) from geo_afo_prod.imp_hkt_auf_plz_gmd_layer where reli is null; 		--944
--select count(*) from geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v4 where reli is null; 		--944  

select * from geo_afo_prod.imp_hkt_auf_plz_gmd_layer where reli is null;






-- verify duplication 
select 
	gemeinde,
	plz6,
	reli,
	count(*)
from
	geo_afo_prod.imp_hkt_auf_plz_gmd_layer
group by 
	reli, 
	plz6,
	gemeinde
having 
	count(*) > 1
;



-- 24 cases it need to be corrected
select 
	reli,
	count(*)
from (
		select 
			distinct plz6 , reli 
		from 
			geo_afo_prod.imp_hkt_auf_plz_gmd_layer
	)
group by 
	reli 
having 
 count(*) > 1
;



select * from geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v4 where reli = 70732792;

select distinct 
	plz6 
from 
	geo_afo_prod.imp_hkt_auf_plz_gmd_layer
where 
 reli is null 
;




select * from geo_afo_prod.imp_hkt_auf_plz_gmd_layer where gmd_nr= 5589;







-- final table 
select * from geo_afo_prod.imp_hkt_auf_plz_gmd_layer;














--////////////////////////////////////////////////////////
--                       DRAFT
--///////////////////////////////////////////////////////

/*
select distinct 
    t1.gmd_nr,
    t1.gemeinde,
    t1.plz6,
    t2.reli,
    t2.geo_poly_lv95 				as geo_poly_hkt_lv95,
    t1.geo_poly_gmd_lv95,
    t1.geo_poly_plz_lv95,
    t1.intersection_geometry
from 
    geo_afo_prod.imp_plz_gmd_mapping t1 
left join 
    geo_afo_prod.imp_hkt100_aktuell_im2025 t2
on 
    ST_Intersects(t1.intersection_geometry, t2.geo_poly_lv95)
where
	t2.reli in (
				select
					reli 
				from
					tmp_hkt_not_in_middle_point
	)
;
*/




/*
-- distributed table results
drop table if exists tmp_gmd_hkt;	 
create temp table tmp_gmd_hkt
as
select 
	gmd_nr
	,gemeinde
	,sum(pers_tot) as v_pers_tot
	,sum(m_tot) as v_m_tot
	,sum(w_tot) as v_f_tot
from 
	geo_afo_tmp.tmp_hkt_auf_plz_gmd_layer_gm_all
group by 
	gmd_nr
	,gemeinde 
;



select 
	*
from 
	tmp_gmd_hkt
where 
	gmd_nr = 1702 --Cham
;


-- given table by Swisstopo
select 
	gmd_nr
	,pers_tot
	,m_tot
	,w_tot
from 
	geo_afo_prod.imp_hkt100_gmd_aktuell_im2025 
where 
	gmd_nr = 1702 --Cham 
;



-- Compare the distributed table with the given one. 
select 
	t1.gmd_nr  							as hkt_gmd_nr
	,t2.gemeinde
	,t1.pers_tot 						as hkt_pers_tot
	,t2.v_pers_tot
	,(t1.pers_tot - t2.v_pers_tot) 		as diff_hkt_gmd
	,t2.v_m_tot
	,t1.m_tot  							as hkt_m_tot
	,t2.v_f_tot
	,t1.w_tot 							as hkt_f_tot
from 
	geo_afo_prod.imp_hkt100_gmd_aktuell_im2025 t1
left join 
	tmp_gmd_hkt t2
on
	t1.gmd_nr = t2.gmd_nr
;


select 
	*
from 
	geo_afo_prod.imp_gmd_geo_neu
where
	gmd_nr  in (	993
					,947
					,6773
					,4042
					,2456
					,6775
	)
;

select * from geo_afo_prod.imp_hkt100_aktuell_im2025;


--------------------------
-- HKT auf Plz6 Ebene
--------------------------
drop table if exists 
		geo_afo_tmp.tmp_hkt_auf_plz_gmd_layer_pl_all;
create table 
		geo_afo_tmp.tmp_hkt_auf_plz_gmd_layer_pl_all
as
select distinct 
	 t1.plz6
	,t2.*  
	,t1.geo_poly_plz_lv95 
from 
	geo_afo_prod.imp_plz_gmd_mapping t1
join
	geo_afo_prod.imp_hkt100_aktuell_im2025 t2
on 
	ST_Contains(t1.geo_poly_plz_lv95, t2.mitl_geo_point_lv95) 
	--ST_Within(t2.mitl_geo_point_lv95, t1.geo_poly_plz_lv95)
;

select * from geo_afo_tmp.tmp_hkt_auf_plz_gmd_layer_pl_all;

-- distributed table results
drop table if exists tmp_plz6_hkt;	 
create temp table tmp_plz6_hkt
as
select 
	plz6
	,sum(pers_tot) as v_pers_tot
	,sum(m_tot) as v_m_tot
	,sum(w_tot) as v_f_tot
from 
	geo_afo_tmp.tmp_hkt_auf_plz_gmd_layer_pl_all
group by 
	plz6
;


select * from tmp_plz6_hkt;

*/









/*


-------------------
-- 	APPROACH (I)
-------------------
drop table if exists 
		geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v1;
create table 
		geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v1
as
select distinct 
    t2.gmd_nr,
    t2.gemeinde,
    t2.plz6,
    t1.reli,
    t2.geo_poly_gmd_lv95,
    t2.geo_poly_plz_lv95,
    t2.intersection_geometry,
    t1.geo_poly_lv95 				as geo_poly_hkt_lv95
from 
    geo_afo_prod.imp_hkt100_aktuell_im2025 t1
left join 
    geo_afo_prod.imp_plz_gmd_mapping t2
on 
    ST_Intersects(t2.intersection_geometry, t1.mitl_geo_point_lv95) 	-- Bounding box check
    and   
    ST_Contains(t2.intersection_geometry, t1.mitl_geo_point_lv95)  		-- Actual containment check
;



-- analyze query 
explain analyze 
select
	-- query here
;  


-------------------
-- 	APPROACH (II)
-------------------
-- Query 1: Matching data (your original query, but without DISTINCT)
drop table if exists 
		geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v2;
create table 
		geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v2
as
select 
    t2.gmd_nr,
    t2.gemeinde,
    t2.plz6,
    t1.reli,
    t2.geo_poly_gmd_lv95,
    t2.geo_poly_plz_lv95,
    t2.intersection_geometry,
    t1.geo_poly_lv95 AS geo_poly_hkt_lv95
from 
    geo_afo_prod.imp_hkt100_aktuell_im2025 t1
join   -- Use INNER JOIN here
    geo_afo_prod.imp_plz_gmd_mapping t2
on 
    ST_Intersects(t2.intersection_geometry, t1.mitl_geo_point_lv95) 
    and 
    ST_Contains(t2.intersection_geometry, t1.mitl_geo_point_lv95)

union all   -- Combine results

-- Query 2: Data from t2 with no match
select 
    t2.gmd_nr,
    t2.gemeinde,
    t2.plz6,
    null  							as reli,  -- reli will be NULL for these rows
    t2.geo_poly_gmd_lv95,
    t2.geo_poly_plz_lv95,
    t2.intersection_geometry,
    null  							as geo_poly_hkt_lv95 -- geo_poly_hkt_lv95 will be NULL
from 
    geo_afo_prod.imp_plz_gmd_mapping t2
where
	not exists (
			    select
			    	1
			    from
			    	geo_afo_prod.imp_hkt100_aktuell_im2025 t1
			    where
			    	ST_Intersects(t2.intersection_geometry, t1.mitl_geo_point_lv95) 
			    	and 
			        ST_Contains(t2.intersection_geometry, t1.mitl_geo_point_lv95)
);





-------------------
-- 	APPROACH (III)
-------------------
drop table if exists 
		geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v3;
create table 
		geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v3
as
-- Part 1: Get Hector rasters matched to Gemeinde/PLZ6 based on middle point OR intersection
select distinct 
    t2.gmd_nr,
    t2.gemeinde,
    t2.plz6,
    t1.reli,
    t1.geo_poly_lv95 			as geo_poly_hkt_lv95,
    t2.geo_poly_gmd_lv95,
    t2.geo_poly_plz_lv95,
    t2.intersection_geometry
from 
    geo_afo_prod.imp_hkt100_aktuell_im2025 t1
left join 
    geo_afo_prod.imp_plz_gmd_mapping t2
on 
    ST_Contains(t2.intersection_geometry, t1.mitl_geo_point_lv95)
    or
    ST_Intersects(t2.intersection_geometry, t1.geo_poly_lv95)
union all 
-- Part 2: Get PLZ6 areas that have NO matching Hector rasters (to avoid losing them)
select distinct 
    t2.gmd_nr,
    t2.gemeinde,
    t2.plz6,
    cast(null as numeric)				as reli,  -- No Hector raster associated
    cast(null as geometry) 				as geo_poly_hkt_lv95, -- No Hector raster geometry
    t2.geo_poly_gmd_lv95,
    t2.geo_poly_plz_lv95,
    t2.intersection_geometry
from 
    geo_afo_prod.imp_plz_gmd_mapping t2
where
	not exists (
    select
    	1
    from
    	geo_afo_prod.imp_hkt100_aktuell_im2025 t1
    where
    	ST_Contains(t2.intersection_geometry, t1.mitl_geo_point_lv95)
       	or
       	ST_Intersects(t2.intersection_geometry, t1.geo_poly_lv95)
);





------------------------
-- TEST THREE APPROACHES
------------------------
-- V3 IS THE CORRECTED ONE

select *  from geo_afo_prod.imp_hkt_auf_plz_gmd_layer;		--347108
select *  from geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v1; 	--347408
select *  from geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v2; 	--348052
select *  from geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v3; 	--377414
select *  from geo_afo_prod.imp_hkt100_aktuell_im2025;		--347408 
select *  from geo_afo_prod.imp_plz_gmd_mapping; 			--6193


select count(distinct reli)  from geo_afo_prod.imp_hkt100_aktuell_im2025;   	--347408 
select count(distinct reli)  from geo_afo_prod.imp_hkt_auf_plz_gmd_layer;    	--347108 --347108
select count(distinct reli)  from geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v1; 	--347408 --347408


select count(distinct plz6)  from geo_afo_prod.imp_hkt_auf_plz_gmd_layer;    	--4050	--4072
select count(distinct plz6)  from geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v1; 	--4050  --4072 
select count(distinct plz6)  from geo_afo_prod.imp_plz_gmd_mapping; 			--4072



select count(distinct gmd_nr)  from geo_afo_prod.imp_hkt_auf_plz_gmd_layer;    	--2121 --2132
select count(distinct gmd_nr)  from geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v1; 	--2121 --2132
select count(distinct gmd_nr)  from geo_afo_prod.imp_plz_gmd_mapping; 			--2132 - 11 LS = 2121







--////////////////////////////////////////////
--TEST


drop table if exists 
		geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v2;
create table 
		geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v2
as
-- Part 1: Get Hector rasters whose middle point is within Gemeinde/PLZ6
select distinct 
    t2.gmd_nr,
    t2.gemeinde,
    t2.plz6,
    t1.reli,
    t1.geo_poly_lv95 			as geo_poly_hkt_lv95,
    t2.geo_poly_gmd_lv95,
    t2.geo_poly_plz_lv95,
    t2.intersection_geometry
from 
    geo_afo_prod.imp_hkt100_aktuell_im2025 t1
left join 
    geo_afo_prod.imp_plz_gmd_mapping t2
on 
	ST_Intersects(t2.intersection_geometry, t1.mitl_geo_point_lv95) 	-- Bounding box check
    and   
    ST_Contains(t2.intersection_geometry, t1.mitl_geo_point_lv95)
union all 
-- Part 2: Get Hector rasters that intersect Gemeinde/PLZ6 ONLY if they are NOT already included in Part 1
select distinct 
    t2.gmd_nr,
    t2.gemeinde,
    t2.plz6,
    t1.reli,
    t1.geo_poly_lv95 			as geo_poly_hkt_lv95,
    t2.geo_poly_gmd_lv95,
    t2.geo_poly_plz_lv95,
    t2.intersection_geometry
from 
    geo_afo_prod.imp_hkt100_aktuell_im2025 t1 
left join 
    geo_afo_prod.imp_plz_gmd_mapping t2
on 
    ST_Intersects(t2.intersection_geometry, t1.geo_poly_lv95)
where  
	t1.reli not in ( -- Exclude rasters already matched in Part 1
	 				select
	 					reli
    				from
    					geo_afo_prod.imp_hkt100_aktuell_im2025 t1 
    				left join 
    					geo_afo_prod.imp_plz_gmd_mapping t2
    				on
    					ST_Intersects(t2.intersection_geometry, t1.mitl_geo_point_lv95) 	-- Bounding box check
    					and   
    					ST_Contains(t2.intersection_geometry, t1.mitl_geo_point_lv95)
	)
;

select * from geo_afo_prod.imp_hkt_auf_plz_gmd_layer_v2 where reli is null;


 */












