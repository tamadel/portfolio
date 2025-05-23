--====================================
-- Project name: update hkt100
-- Quellen: STATPOP / STATENT  
-- Date: 03.02.2025
--====================================
-- Data snsicht 
-- Wohnbevölkerung und Privathaushalte 
select * from geo_afo_tmp.hkt100_statpop2023;
select * from geo_afo_tmp.statpop2023_gmd;

-- Arbeitsstätten und Beschäftigte
select * from geo_afo_tmp.hkt100_statent2021;


-- test before update the tables 
select 
	reli 
	,ST_SetSRID(ST_MakePoint(e_koord + 50, n_koord  + 50), 2056) AS middle_geo_point_lv95
	,ST_SetSRID(ST_MakePolygon(
                ST_MakeLine(ARRAY[
                    ST_MakePoint(e_koord, n_koord),         	-- SW corner
                    ST_MakePoint(e_koord + 100, n_koord),   	-- SE corner
                    ST_MakePoint(e_koord + 100, n_koord + 100), -- NE corner
                    ST_MakePoint(e_koord, n_koord + 100),   	-- NW corner
                    ST_MakePoint(e_koord, n_koord)          	-- Closing the polygon
                ])
            ),
        2056) as geo_poly_lv95
from 
	geo_afo_tmp.hkt100_statpop2023
;



-- add 2 columns one for the middle point and the other for a polygone 
-- 1) STATPOP: Wohnbevölkerung und Privathaushalte
alter table geo_afo_tmp.hkt100_statpop2023
add column 
		geo_point_lv95 geometry,
add column 
		geo_poly_lv95 geometry
;

-- update the table 
update geo_afo_tmp.hkt100_statpop2023
set --middle point
	geo_point_lv95 = ST_SetSRID(ST_MakePoint(e_koord + 50, n_koord  + 50), 2056),
	geo_poly_lv95 = ST_SetSRID(ST_MakePolygon(
					                ST_MakeLine(ARRAY[
					                    ST_MakePoint(e_koord, n_koord),         	-- SW corner
					                    ST_MakePoint(e_koord + 100, n_koord),   	-- SE corner
					                    ST_MakePoint(e_koord + 100, n_koord + 100), -- NE corner
					                    ST_MakePoint(e_koord, n_koord + 100),   	-- NW corner
					                    ST_MakePoint(e_koord, n_koord)          	-- Closing the polygon
					                ])
								), 2056
					)
;




-- 2) STATENT: Arbeitsstätten und Beschäftigte
alter table geo_afo_tmp.hkt100_statent2021
add column 
		geo_point_lv59 geometry,
add column 
		geo_poly_lv95 geometry
;

-- update the table 
update geo_afo_tmp.hkt100_statent2021
set --middle point
	geo_point_lv59 = ST_SetSRID(ST_MakePoint(e_koord + 50, n_koord  + 50), 2056),
	geo_poly_lv95 = ST_SetSRID(ST_MakePolygon(
					                ST_MakeLine(ARRAY[
					                    ST_MakePoint(e_koord, n_koord),         	-- SW corner
					                    ST_MakePoint(e_koord + 100, n_koord),   	-- SE corner
					                    ST_MakePoint(e_koord + 100, n_koord + 100), -- NE corner
					                    ST_MakePoint(e_koord, n_koord + 100),   	-- NW corner
					                    ST_MakePoint(e_koord, n_koord)          	-- Closing the polygon
					                ])
								), 2056
					)
;

select * from geo_afo_tmp.hkt100_statent2021;
select * from geo_afo_tmp.hkt100_statpop2023;


drop table if exists geo_afo_prod.imp_hkt100_aktuell_im2025;
create table 
	geo_afo_prod.imp_hkt100_aktuell_im2025
as
select 
	t1.reli 		
	,t1.e_koord 	
	,t1.n_koord 	
	,t1.bbtot 			as pers_tot 
	,t1.bb11  			as pers_ch
	,t1.bb12  			as pers_aus 
	,t1.bbmtot 			as m_tot 
	,t1.bbm01  			as m_0_4
	,t1.bbm02  			as m_5_9
	,t1.bbm03  			as m_10_14
	,t1.bbm04  			as m_15_19
	,t1.bbm05  			as m_20_24
	,t1.bbm06  			as m_25_29
	,t1.bbm07  			as m_30_34
	,t1.bbm08  			as m_35_39
	,t1.bbm09  			as m_40_44
	,t1.bbm10  			as m_45_49
	,t1.bbm11  			as m_50_54
	,t1.bbm12 			as m_55_59
	,t1.bbm13 			as m_60_64
	,t1.bbm14 			as m_65_69
	,t1.bbm15 			as m_70_74
	,t1.bbm16 			as m_75_79
	,t1.bbm17 			as m_80_84
	,t1.bbm18 			as m_85_89
	,t1.bbm19 			as m_90
	,t1.bbwtot 			as w_tot
	,t1.bbw01 			as w_0_4
 	,t1.bbw02 			as w_5_9
  	,t1.bbw03			as w_10_14
 	,t1.bbw04 			as w_15_19
  	,t1.bbw05 			as w_20_24
  	,t1.bbw06 			as w_25_29
 	,t1.bbw07			as w_30_34
	,t1.bbw08 			as w_35_39
 	,t1.bbw09 			as w_40_44
  	,t1.bbw10 			as w_45_49
  	,t1.bbw11 			as w_50_54
 	,t1.bbw12			as w_55_59
	,t1.bbw13 			as w_60_64
 	,t1.bbw14			as w_65_69
 	,t1.bbw15			as w_70_74
	,t1.bbw16			as w_75_79
 	,t1.bbw17			as w_80_84
	,t1.bbw18 			as w_85_89
	,t1.bbw19			as w_90
	,t1.hptot 			as hh_tot
	,t1.hp01 			as hh_1
	,t1.hp02 			as hh_2
	,t1.hp03 			as hh_3
	,t1.hp04 			as hh_4
	,t1.hp05 			as hh_5
	,t1.hp06 			as hh_6
	,t1.hpi 			as hh_plausibilität
	--,t2.e_koord		as e_koord_ent 
	--,t2.n_koord 		as n_koord_ent
	--,t2.reli 			as reli_ent
	,t2.b08t 			as arbeitsstaetten_tot
	,t2.b08s1 			as arbeitsstaetten_s1
	,t2.b08s2 			as arbeitsstaetten_s2
	,t2.b08s3 			as arbeitsstaetten_s3
	,t2.b08empt 		as besch_tot
	,t2.b08empts1 		as besch_s1
	,t2.b08empfs1 		as besch_f_s1
	,t2.b08empms1 		as besch_m_s1
	,t2.b08empts2 		as besch_s2
	,t2.b08empfs2 		as besch_f_s2
	,t2.b08empms2 		as besch_m_s2
	,t2.b08empts3 		as besch_s3
	,t2.b08empfs3 		as besch_f_s3
	,t2.b08empms3 		as besch_m_s3
	,t1.geo_point_lv95 	as mitl_geo_point_lv95 
	,t1.geo_poly_lv95 
	--,t2.geo_point_lv95 
	--,t2.geo_poly_lv95 
from
	geo_afo_tmp.hkt100_statpop2023 t1
left join 
	geo_afo_tmp.hkt100_statent2021 t2
on
	t1.e_koord = t2.e_koord 
	and 
	t1.n_koord = t2.n_koord 
	and 
	t1.reli = t2.reli 
;

--347408
select * from geo_afo_prod.imp_hkt100_aktuell_im2025;

--------------------------
-- gmd level 
--------------------------
create table geo_afo_prod.imp_hkt100_aktuell_neu_gmd
as
select 
	t1.gmde 		as gmd_nr
	,t1.hist_gmde 	as hist_gmd
	,t1.bbtot 		as pers_tot 
	,t1.bb11  		as pers_ch
	,t1.bb12  		as pers_aus 
	,t1.bbmtot 		as m_tot 
	,t1.bbm01  		as m_0_4
	,t1.bbm02  		as m_5_9
	,t1.bbm03  		as m_10_14
	,t1.bbm04  		as m_15_19
	,t1.bbm05  		as m_20_24
	,t1.bbm06  		as m_25_29
	,t1.bbm07  		as m_30_34
	,t1.bbm08  		as m_35_39
	,t1.bbm09  		as m_40_44
	,t1.bbm10  		as m_45_49
	,t1.bbm11  		as m_50_54
	,t1.bbm12 		as m_55_59
	,t1.bbm13 		as m_60_64
	,t1.bbm14 		as m_65_69
	,t1.bbm15 		as m_70_74
	,t1.bbm16 		as m_75_79
	,t1.bbm17 		as m_80_84
	,t1.bbm18 		as m_85_89
	,t1.bbm19 		as m_90
	,t1.bbwtot 		as w_tot
	,t1.bbw01 		as w_0_4
 	,t1.bbw02 		as w_5_9
  	,t1.bbw03		as w_10_14
 	,t1.bbw04 		as w_15_19
  	,t1.bbw05 		as w_20_24
  	,t1.bbw06 		as w_25_29
 	,t1.bbw07		as w_30_34
	,t1.bbw08 		as w_35_39
 	,t1.bbw09 		as w_40_44
  	,t1.bbw10 		as w_45_49
  	,t1.bbw11 		as w_50_54
 	,t1.bbw12		as w_55_59
	,t1.bbw13 		as w_60_64
 	,t1.bbw14		as w_65_69
 	,t1.bbw15		as w_70_74
	,t1.bbw16		as w_75_79
 	,t1.bbw17		as w_80_84
	,t1.bbw18 		as w_85_89
	,t1.bbw19		as w_90
	,t1.hptot 		as hh_tot
	,t1.hp01 		as hh_1
	,t1.hp02 		as hh_2
	,t1.hp03 		as hh_3
	,t1.hp04 		as hh_4
	,t1.hp05 		as hh_5
	,t1.hp06 		as hh_6
	,t2.b08t 		as arbeitsstaetten_tot
	,t2.b08s1 		as arbeitsstaetten_s1
	,t2.b08s2 		as arbeitsstaetten_s2
	,t2.b08s3 		as arbeitsstaetten_s3
	,t2.b08empt 	as besch_tot
	,t2.b08empts1 	as besch_s1
	,t2.b08empfs1 	as besch_f_s1
	,t2.b08empms1 	as besch_m_s1
	,t2.b08empts2 	as besch_s2
	,t2.b08empfs2 	as besch_f_s2
	,t2.b08empms2 	as besch_m_s2
	,t2.b08empts3 	as besch_s3
	,t2.b08empfs3 	as besch_f_s3
	,t2.b08empms3 	as besch_m_s3
from
	geo_afo_tmp.statpop2023_gmd t1
left join 
	geo_afo_tmp.statent2021_gmd t2
on
	t1.gmde = t2.gdenr 
;



select * from geo_afo_prod.imp_hkt100_aktuell_neu_gmd;

 alter table geo_afo_prod.imp_hkt100_aktuell_neu_gmd
 add column gmd_alt numeric;

----------------------------------------------
--GMD-Mutation-Aktualisierung >> HKT
--------------------------------------------
-- geo_afo.gmd_bfs_mutationen nach Serverless migrieren.

drop table if exists
	geo_afo.gmd_bfs_mutationen;
create table
	geo_afo.gmd_bfs_mutationen
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
				geo_afo.gmd_bfs_mutationen
		$POSTGRES$
	) as gmd_bfs_mutationen (
			mutation_id int4 
			,gmd_alt int4 
			,gmd_neu int4 
			,gmd_name varchar(50) 
			,date_mut timestamp 
			,factor float8
	)
;


select * from geo_afo.gmd_bfs_mutationen;

---------------------------------------------------------
-- gmd_nr bei HKT zahlen auf gemeinde ebene aktulisieren 
---------------------------------------------------------
update geo_afo_prod.imp_hkt100_aktuell_neu_gmd set
	gmd_nr = case when gmd_bfs_mutationen.gmd_alt is null then gmd_nr else gmd_neu end
	--gemeinde = case when gmd_bfs_mutationen.gmd_alt is null then gemeinde else gmd_name end
from geo_afo.gmd_bfs_mutationen 
where geo_afo_prod.imp_hkt100_aktuell_neu_gmd.gmd_nr=gmd_bfs_mutationen.gmd_alt AND date_mut >= '20240101' AND date_mut <= '20241231' --- Plus 1 Jahr
;

update geo_afo_prod.imp_hkt100_aktuell_neu_gmd set
	gmd_nr = case when gmd_bfs_mutationen.gmd_alt is null then gmd_nr else gmd_neu end
	--gemeinde = case when gmd_bfs_mutationen.gmd_alt is null then gemeinde else gmd_name end
from geo_afo.gmd_bfs_mutationen 
where geo_afo_prod.imp_hkt100_aktuell_neu_gmd.gmd_nr=gmd_bfs_mutationen.gmd_alt AND date_mut >= '20250101' AND date_mut <= '20251231' --- Plus 1 Jahr
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
						geo_afo_prod.imp_gmd_geo_neu 
	)
;

/*
gmd_nr_alt bei hkt auf gemd ebene 
947
993
2456
4042
6773
6775
*/

select 
	*
from
	geo_afo.gmd_bfs_mutationen
where 
	gmd_alt in (
				947
				,993
				,2456
				,4042
				,6773
				,6775
	)
;

/*
gmd_alt | gmd_neu | gmd_name
947   		767		Reutigen
993   		992		Wangen an der Aare
2456		2465	Buchegg
6773		6812	Basse-Vendline
6775		6812	Basse-Vendline
4042		4021	Baden
*/


----------------------------------------------
-- aggregate hkt data according to new gmd_nr
----------------------------------------------

drop table if exists 
		geo_afo_prod.imp_hkt100_gmd_aktuell_im2025;
create table 
		geo_afo_prod.imp_hkt100_gmd_aktuell_im2025
as
select  
    gmd_nr,
    MIN(hist_gmd) 				as hist_gmd,  -- Assuming this should remain the same (taking the smallest value)
    SUM(pers_tot) 				as pers_tot,
    SUM(pers_ch) 				as pers_ch,
    SUM(pers_aus) 				as pers_aus,
    -- Male Population by Age Groups
    SUM(m_tot) 					as m_tot,
    SUM(m_0_4) 					as m_0_4,
    SUM(m_5_9) 					as m_5_9,
    SUM(m_10_14) 				as m_10_14,
    SUM(m_15_19) 				as m_15_19,
    SUM(m_20_24) 				as m_20_24,
    SUM(m_25_29) 				as m_25_29,
    SUM(m_30_34) 				as m_30_34,
    SUM(m_35_39) 				as m_35_39,
    SUM(m_40_44) 				as m_40_44,
    SUM(m_45_49) 				as m_45_49,
    SUM(m_50_54) 				as m_50_54,
    SUM(m_55_59) 				as m_55_59,
    SUM(m_60_64) 				as m_60_64,
    SUM(m_65_69) 				as m_65_69,
    SUM(m_70_74) 				as m_70_74,
    SUM(m_75_79) 				as m_75_79,
    SUM(m_80_84) 				as m_80_84,
    SUM(m_85_89) 				as m_85_89,
    SUM(m_90) 					as m_90,
    -- Female Population by Age Groups
    SUM(w_tot) 					as w_tot,
    SUM(w_0_4) 					as w_0_4,
    SUM(w_5_9) 					as w_5_9,
    SUM(w_10_14) 				as w_10_14,
    SUM(w_15_19) 				as w_15_19,
    SUM(w_20_24) 				as w_20_24,
    SUM(w_25_29) 				as w_25_29,
    SUM(w_30_34) 				as w_30_34,
    SUM(w_35_39) 				as w_35_39,
    SUM(w_40_44) 				as w_40_44,
    SUM(w_45_49) 				as w_45_49,
    SUM(w_50_54) 				as w_50_54,
    SUM(w_55_59) 				as w_55_59,
    SUM(w_60_64) 				as w_60_64,
    SUM(w_65_69) 				as w_65_69,
    SUM(w_70_74) 				as w_70_74,
    SUM(w_75_79) 				as w_75_79,
    SUM(w_80_84) 				as w_80_84,
    SUM(w_85_89) 				as w_85_89,
    SUM(w_90) 					as w_90,
    -- Households
    SUM(hh_tot) 				as hh_tot,
    SUM(hh_1) 					as hh_1,
    SUM(hh_2) 					as hh_2,
    SUM(hh_3) 					as hh_3,
    SUM(hh_4) 					as hh_4,
    SUM(hh_5) 					as hh_5,
    SUM(hh_6) 					as hh_6,
    -- Workplaces
    SUM(arbeitsstaetten_tot) 	as arbeitsstaetten_tot,
    SUM(arbeitsstaetten_s1) 	as arbeitsstaetten_s1,
    SUM(arbeitsstaetten_s2) 	as arbeitsstaetten_s2,
    SUM(arbeitsstaetten_s3) 	as arbeitsstaetten_s3,
    -- Employees
    SUM(besch_tot) 				as besch_tot,
    SUM(besch_s1) 				as besch_s1,
    SUM(besch_f_s1) 			as besch_f_s1,
    SUM(besch_m_s1) 			as besch_m_s1,
    SUM(besch_s2) 				as besch_s2,
    SUM(besch_f_s2) 			as besch_f_s2,
    SUM(besch_m_s2) 			as besch_m_s2,
    SUM(besch_s3) 				as besch_s3,
    SUM(besch_f_s3) 			as besch_f_s3,
    SUM(besch_m_s3) 			as besch_m_s3,
    -- Old Municipality Code
    AVG(gmd_alt) 				as gmd_alt  -- Assuming it represents an averageable value
from
	geo_afo_prod.imp_hkt100_aktuell_neu_gmd
group by
	gmd_nr
;


select 
	gmd_nr
	,count(*)
from
	geo_afo_prod.imp_hkt100_gmd_aktuell_im2025   --imp_hkt100_aktuell_neu_gmd
group by 
	gmd_nr
having 
	count(*) > 1
;



select 
	*
from 
	geo_afo_prod.imp_hkt100_gmd_aktuell_im2025     --imp_hkt100_aktuell_neu_gmd
where 
	gmd_nr in (2097, 6513, 2465, 4021, 992, 767, 2102, 6812, 3901, 1065, 2239)
;


select count(*) from geo_afo_prod.imp_hkt100_gmd_aktuell_im2025; 
















	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	






--///////////////////////// DRAFT ////////////////////////////
/*
select 
	h.e_koord
	,h.n_koord
	,g.gmd_nr
	,g.gemeinde
	,g.plz6
	,g.geo_poly_gmd_lv95
	,g.geo_poly_plz_lv95
	,h.geo_poly_lv95
from
	geo_afo_prod.imp_hkt100_aktuell_neu h
join 
	








-- Index on hkt poly
create index idx_hkt100_geom 
on geo_afo_prod.imp_hkt100_aktuell_neu using GIST (geo_poly_lv95);

-- Index on gmd poly
create index idx_plz6_geom 
on geo_afo_prod.imp_plz6_gmd using GIST (geo_poly_gmd_lv95);



create table 
	geo_afo_tmp.hkt100_gmd_plz6_test 
as 
select   
    h.e_koord,
    h.n_koord,
    m.gmd_nr,
    m.gemeinde,
    m.plz6,
    m.anteil_plz6_relative_to_total,
    m.anteil_plz6_fm_gmd, 
    h.geo_poly_lv95,
    m.geo_poly_gmd_lv95,
    (ST_Area(ST_Intersection(h.geo_poly_lv95, m.geo_poly_gmd_lv95)) / NULLIF(ST_Area(h.geo_poly_lv95), 0)) * h.pers_tot as distributed_population
from
	geo_afo_prod.imp_hkt100_aktuell_neu h
join
	geo_afo_prod.imp_plz6_gmd m
on  
	h.geo_poly_lv95 && m.geo_poly_gmd_lv95 -- Bounding Box First (Fast!)
	and
	ST_Intersects(h.geo_poly_lv95, m.geo_poly_gmd_lv95); -- Exact Intersection
;




select 
	*
from 
	geo_afo_tmp.hkt100_gmd_plz6_test
where 
	gmd_nr = 3112
	--gmd_nr in (3112, 3002, 3006, 3360, 3359, 3396, 3003)
;

 


select 
	round(sum(distributed_population)) as tot_per_gmd 
from 
	geo_afo_tmp.hkt100_gmd_plz6_test
where 
	gmd_nr = 3112
	--gmd_nr in (3112, 3002, 3006, 3360, 3359, 3396, 3003)
; 



select 
	bbtot as tot_per_gmd 
from 
	geo_afo_tmp.statpop2023_gmd
where 
	gmde = 3112
;


select 
	*
from 
	geo_afo_prod.imp_plz6_gmd
where 
	plz6 = 910700;

*/









/*
create table geo_afo_tmp.hkt100_gmd_plz6_test
as
select  
    h.e_koord,
    h.n_koord,
    m.gmd_nr,
    m.gemeinde,
    m.plz6,
    m.anteil_plz6_relative_to_total,
    m.anteil_plz6_fm_gmd, 
    h.geo_poly_lv95,
    m.geo_poly_gmd_lv95,
    (ST_Area(ST_Intersection(h.geo_poly_lv95, m.geo_poly_gmd_lv95)) / ST_Area(h.geo_poly_lv95)) * h.pers_tot as distributed_population
from  
	geo_afo_prod.imp_hkt100_aktuell_neu h
join  
    geo_afo_prod.imp_plz6_gmd m
ON 
    ST_Intersects(h.geo_poly_lv95, m.geo_poly_gmd_lv95);
*/




/*
with corrected_hh as (
    select
    	reli,
    	pers_tot,
        agg_alter_katg,
        hh_1,
        hh_2,
        hh_3,
        case 
            when hh_1 = 0 then 0  -- If hh_1 is 0, keep it as 0
            when agg_alter_katg = 1 and hh_1 > 0 then 1	
            when agg_alter_katg = 1 and hh_1 > 0 then 3	
            
            
            when agg_alter_katg = 2 and hh_1 > 0 and hh_2 = 0 and hh_3 = 0 then 2		
            when agg_alter_katg = 2 and hh_1 = 0 and hh_2 > 0 and hh_3 = 0 then 1		
            when agg_alter_katg = 3 and hh_1 = 0 and hh_2 = 0 and hh_3 > 0 then 1		
            when agg_alter_katg = 3 and hh_1 = 0 and hh_2 > 0 and hh_3 = 0 then 3		
            when agg_alter_katg = 3 and hh_1 > 0 and hh_2 = 0 and hh_3 = 0 then 1		
            else hh_1  
        end as corr_hh_1,
        case 
            when hh_2 = 0 then 0  -- If hh_2 is 0, keep it as 0
            when agg_alter_katg = 1 and hh_2 > 0 then 1									-- pers_tot = 1 or 2 or 3
            when agg_alter_katg = 2 and hh_1 = 0 and hh_2 > 0 and hh_3 = 0 then 1		-- pers_tot = 2 or 3
            when agg_alter_katg = 3 and hh_1 = 0 and hh_2 > 0 and hh_3 = 0 then 1		-- pers_tot = 3
            else hh_2  
        end as corr_hh_2, 
        case 
            when hh_3 = 0 then 0  -- If hh_3 is 0, keep it as 0
            when agg_alter_katg = 1 and hh_3 > 0 then 1									-- pers_tot = 1 or 2 or 3
            when agg_alter_katg = 2 and hh_1 = 0 and hh_2 = 0 and hh_3 > 0 then 1		-- pers_tot = 2 or 3
            when agg_alter_katg = 3 and hh_1 = 0 and hh_2 = 0 and hh_3 > 0 then 1		-- pers_tot = 3
            else hh_3  -- Keep the original hh_3 value if no conditions are met
        end as corr_hh_3
    from 
        geo_afo_tmp.tmp_agg_alter_katg
)
select 
	reli,
    agg_alter_katg,
    pers_tot,
    case -- agg_alter_katg = 1 there are 3 possibilitie for pers_tot 
	    when agg_alter_katg = 1 and corr_hh_1 = 1 and corr_hh_2 = 0 and corr_hh_3 = 0 then 1 		-- has only one case hh_1 and hh_2 & hh_3 must be 0
	    when agg_alter_katg = 1 and corr_hh_1 = 0 and corr_hh_2 = 1 and corr_hh_3 = 0 then 2		-- has 2 cases either hh_2 = 1 then hh_1 & hh_3 must be 0 
	    when agg_alter_katg = 1 and corr_hh_1 = 2 and corr_hh_2 = 0 and corr_hh_3 = 0 then 2		-- (OR) hh_1 = 2 and hh_2 & hh_3 must be 0
	    when agg_alter_katg = 1 and corr_hh_1 = 0 and corr_hh_2 = 0 and corr_hh_3 = 1 then 3		-- has 3 cases either hh_3 = 1 then hh_1 & hh_2 must be 0 
	    when agg_alter_katg = 1 and corr_hh_1 = 3 and corr_hh_2 = 0 and corr_hh_3 = 0 then 3		-- (OR)	hh_1 = 3 then hh_2 & hh_3 must be 0
	    when agg_alter_katg = 1 and corr_hh_1 = 1 and corr_hh_2 = 1 and corr_hh_3 = 0 then 3		-- (OR) hh_1 = 1 & hh_2 = 1 then hh_3 must be 0
	 	-- agg_alter_katg = 2 there are 2 possibilitie for pers_tot 
	    when agg_alter_katg = 2 and corr_hh_1 = 0 and corr_hh_2 = 1 and corr_hh_3 = 0 then 2
	    when agg_alter_katg = 2 and corr_hh_1 = 2 and corr_hh_2 = 0 and corr_hh_3 = 0 then 2
	    when agg_alter_katg = 2 and corr_hh_1 = 0 and corr_hh_2 = 0 and corr_hh_3 = 1 then 3
	    when agg_alter_katg = 2 and corr_hh_1 = 1 and corr_hh_2 = 1 and corr_hh_3 = 0 then 3
	    -- agg_alter_katg = 3 there is only 1 possibility for pers_tot 
	    when agg_alter_katg = 3 and corr_hh_1 = 0 and corr_hh_2 = 0 and corr_hh_3 = 1 then 3
	    when agg_alter_katg = 3 and corr_hh_1 = 3 and corr_hh_2 = 0 and corr_hh_3 = 0 then 3
	    when agg_alter_katg = 3 and corr_hh_1 = 1 and corr_hh_2 = 1 and corr_hh_3 = 0 then 3
		else null  -- null if no other conditions are met
    end as final_pers_tot,
    hh_1,
    corr_hh_1,
    hh_2,
    corr_hh_2,
    hh_3,
    corr_hh_3
from  
	corrected_hh
;
 */













