--==================================
-- Project: HKT100 deanonymization
-- Date:
-- Update:
--==================================
--======================================
-- anz_perso der HKT Deanonymisierung
--======================================
--------------------------------
-- create Tables for the Project 
--------------------------------
-- hkt data set 
-- 347408
select * from geo_afo_prod.imp_hkt100_aktuell_im2025;


-- Create a temporary table for criteria where pers_tot = 3.
-- 74789
drop table if exists 
		geo_afo_tmp.tmp_anonym_anz_pers;
create table 
		geo_afo_tmp.tmp_anonym_anz_pers
as
select 
	*
from 
	geo_afo_prod.imp_hkt100_aktuell_im2025
where 
	pers_tot = 3
;

select * from geo_afo_tmp.tmp_anonym_anz_pers;

--add new column for agg age_categories

alter table 
		geo_afo_tmp.tmp_anonym_anz_pers
add column 
		agg_alter_katg numeric 
; 


------------------------------------------
-- Agg Ages categories with Houshold data
------------------------------------------
-- create table where the age categories are aggregated and hh data is not 0 "hh_plausibilität is not null"
--74454 reli with houshold data 
drop table if exists geo_afo_tmp.tmp_agg_alter_katg;
create table 
		geo_afo_tmp.tmp_agg_alter_katg
as
select 
	reli 
	,e_koord 
	,n_koord 
	,pers_tot 
	,(m_0_4 
		+ m_5_9 
		+ m_10_14 
		+ m_15_19 
		+ m_20_24 
		+ m_25_29 
		+ m_30_34 
		+ m_35_39 
		+ m_40_44 
		+ m_45_49 
		+ m_50_54 
		+ m_55_59 
		+ m_60_64 
		+ m_65_69 
		+ m_70_74 
		+ m_75_79 
		+ m_80_84 
		+ m_85_89 
		+ m_90 
		+ w_0_4 
		+ w_5_9 
		+ w_10_14 
		+ w_15_19 
		+ w_20_24 
		+ w_25_29 
		+ w_30_34 
		+ w_35_39 
		+ w_40_44 
		+ w_45_49 
		+ w_50_54 
		+ w_55_59 
		+ w_60_64 
		+ w_65_69 
		+ w_70_74 
		+ w_75_79 
		+ w_80_84 
		+ w_85_89 
		+ w_90 ) / 3  as agg_alter_katg
	--,hh_tot 
	,hh_1 
	,hh_2 
	,hh_3 
	,hh_plausibilität 
from 
	geo_afo_tmp.tmp_anonym_anz_pers
where 
	hh_plausibilität is not null 
order by
	agg_alter_katg,
	hh_1,
	hh_2,
	hh_3
;

select * from geo_afo_tmp.tmp_agg_alter_katg;

---------------------
-- EDIT SP 1 - START
---------------------
SELECT 
	COUNT(*) 
FROM 
	geo_afo_tmp.tmp_agg_alter_katg
WHERE 
	agg_alter_katg = 3;

-- 1: 19'992
-- 2: 34'368
-- 3: 20'094

-- Stichproben
SELECT 
	*
FROM 
	geo_afo_prod.imp_hkt100_aktuell_im2025
WHERE 
	reli = 73752713;

--------------------
-- EDIT SP 1 - ENDE
--------------------

------------------------------------------
-- Logic to deanonymise using hh data 
------------------------------------------
/*
Rules:

1. pers_tot is always 3.
2. hh_1, hh_2, and hh_3 always sum to 3 initially.
3. hh_2 and hh_3 can never be greater than 1.
4. If hh_3 has a value, hh_1 and hh_2 must be 0. However, if hh_3 and either hh_1 or hh_2 have values, this rule is ignored.
5. We need to adjust the values of hh_1, hh_2, and hh_3 to be consistent with the above rules, using agg_alter_katg as a guide.

------------------------------
Logic based on agg_alter_katg:
------------------------------
- When agg_alter_katg is 1:
	- There are three possibilities for pers_tot: 1, 2, or 3.
	- If pers_tot is 1, then hh_1 should be 1 and the rest should be 0.
	- If pers_tot is 2, then either hh_1 is 2 and the rest are 0, or hh_2 is 1 and the rest are 0.
	- If pers_tot is 3, then either hh_1 is 3 and the rest are 0, or hh_1 is 1 and hh_2 is 1 and the rest is 0, or hh_3 is 1 and the rest are 0.
	
- When agg_alter_katg is 2:
	- There are two possibilities for pers_tot: 2 or 3.
	- If pers_tot is 2, then either hh_1 is 2 and the rest are 0, or hh_2 is 1 and the rest are 0.
	- If pers_tot is 3, then either hh_1 is 3 and the rest are 0, or hh_1 is 1 and hh_2 is 1 and the rest is 0, or hh_3 is 1 and the rest are 0.
	
- When agg_alter_katg is 3:
	- pers_tot is always 3.
	- Either hh_1 is 3 and the rest are 0, or hh_1 is 1 and hh_2 is 1 and the rest is 0, or hh_3 is 1 and the rest are 0.
	
Important Note: We should prioritize correcting pers_tot first, as its value directly impacts the other hh values.

*/



-- When agg_alter_katg = 3, there are one possibilitie:
	--(I)  	pers_tot = 3, there will be three cases:
				-- case(1) - if hh_1 && hh_2 = 0 then hh_3 has to be = 1 
				-- case(2) - if hh_2 && hh_3 = 0 then hh_1 has to be = 3	
				-- case(3) - if hh_3 = 0 		 then hh_1 has to be = 1 && hh_2 has to be = 1


-- When agg_alter_katg = 2, there are two possibilities:
	--(I)  	pers_tot = 2, there will be two cases:
				-- case(1) - if hh_2 && hh_3 = 0 then hh_1 has to be = 2	
				-- case(3) - if hh_1 && hh_3 = 0 then hh_2 has to be = 1

	--(II) 	pers_tot = 3, there will be three cases:
				-- case(1) - if hh_1 && hh_2 = 0 then hh_3 has to be = 1 
				-- case(2) - if hh_2 && hh_3 = 0 then hh_1 has to be = 3	
				-- case(3) - if hh_3 = 0 		 then hh_1 has to be = 1 && hh_2 has to be = 1



-- When agg_alter_katg = 1, there are three possibilities:
	--(I)   pers_tot = 1, there will be only one case:
				-- case(1) - hh_3 && hh_2 has to be = 0 and hh_1 = 1

	--(II)  pers_tot = 2, there will be two cases:
	 			-- case(1) - if hh_2 && hh_3 = 0 then hh_1 has to be = 2	
				-- case(2) - if hh_1 && hh_3 = 0 then hh_2 has to be = 1

	--(III) pers_tot = 3, there will be three cases:
				-- case(1) - if hh_1 && hh_2 = 0 then hh_3 has to be = 1 
				-- case(2) - if hh_2 && hh_3 = 0 then hh_1 has to be = 3	
				-- case(3) - if hh_3 = 0 		 then hh_1 has to be = 1 && hh_2 has to be = 1


/* logic
hh_1 = 0 && hh_2 = 0 && hh_3 > 0 then pers_tot = 3 and hh_3 = 1 and hh_1 = 0 and hh_2 = 0
hh_1 > 0 && hh_2 > 0 && hh_3 = 0 then pers_tot = 3 and hh_1 = 1 and hh_2 = 1 and hh_3 = 0
hh_1 > 0 && hh_2 = 0 && hh_3 = 0 then pers_tot = 3 and hh_1 = 3 and hh_2 = 0 and hh_3 = 0
hh_1 = 0 && hh_2 > 0 && hh_3 = 0 then pers_tot = 2 and hh_2 = 1 and hh_1 = 0 and hh_3 = 0

hh_1 > 0 && hh_2 = 0 && hh_3 = 0 then pers_tot = 3 if agg_alter_katg = 3
											   = 2 if agg_alter_katg = 2 or 1
											   = 3 if agg_alter_katg = 2 or 
*/	




drop table if exists 
		geo_afo_tmp.tmp_corr_hh_pers_tot;
create table 
		geo_afo_tmp.tmp_corr_hh_pers_tot
as
with pers_tot_adjusted as (
  select
    reli,
    pers_tot,
    agg_alter_katg,
    hh_1,
    hh_2,
    hh_3,
    case 
      when hh_1 = 0 and hh_2 = 0 and hh_3 > 0 						 	then 3
      when hh_1 > 0 and hh_2 > 0 and hh_3 = 0 						 	then 3
      when hh_1 = 0 and hh_2 > 0 and hh_3 = 0 						 	then 2
      when hh_1 > 0 and hh_2 = 0 and hh_3 = 0 and agg_alter_katg = 3 	then 3
      when hh_1 > 0 and hh_2 = 0 and hh_3 = 0 and agg_alter_katg = 2 	then 2
      when hh_1 > 0 and hh_2 = 0 and hh_3 = 0 and agg_alter_katg = 1 	then 1
      else null
    end as pers_tot_adj
  from
  	geo_afo_tmp.tmp_agg_alter_katg
)
select
  reli,
  agg_alter_katg,
  pers_tot,
  pers_tot_adj,
  hh_1,
  case 
    when hh_1 = 0 														then 0
    when pers_tot_adj = 3 and hh_1 > 0 and hh_2 > 0 and hh_3 = 0 		then 1
    when pers_tot_adj = 3 and hh_1 > 0 and hh_2 = 0 and hh_3 = 0 		then 3
    when pers_tot_adj = 2 and hh_1 > 0 and hh_2 = 0 and hh_3 = 0 		then 2
    when pers_tot_adj = 1 and hh_1 > 0 and hh_2 = 0 and hh_3 = 0 		then 1
    else hh_1
  end as corr_hh_1,
  hh_2,
  case 
    when hh_2 = 0 														then 0
    when pers_tot_adj = 3 and hh_1 > 0 and hh_2 > 0 and hh_3 = 0 		then 1
    when pers_tot_adj = 2 and hh_1 = 0 and hh_2 > 0 and hh_3 = 0 		then 1
    else hh_2
  end as corr_hh_2,
  hh_3,
  case 
    when hh_3 = 0 														then 0
    when pers_tot_adj = 3 and hh_1 = 0 and hh_2 = 0 and hh_3 > 0 		then 1
    else hh_3
  end as corr_hh_3
from
	pers_tot_adjusted
;


--(74789) mit "hh data" und "ohne hh = 0"
select * from geo_afo_tmp.tmp_anonym_anz_pers;
--(74454) there are still 335 with no hoshold data 
select * from geo_afo_tmp.tmp_agg_alter_katg;
--(74454)
select * from geo_afo_tmp.tmp_corr_hh_pers_tot;


-----------------
-- verification
-----------------
-- agg_alter_katg = 1 && pers_tot = 1 (19646)
-- agg_alter_katg = 1 && pers_tot = 2 (319)
-- agg_alter_katg = 1 && pers_tot = 3 (27)
select 
	*
from 
	geo_afo_tmp.tmp_corr_hh_pers_tot
where 
 	agg_alter_katg = 1 
 	and 
 	pers_tot_adj = 1 --2 --3
;


-- agg_alter_katg = 2 && pers_tot = 2 (33070)
-- agg_alter_katg = 2 && pers_tot = 3 (1298)
select 
	*
from 
	geo_afo_tmp.tmp_corr_hh_pers_tot
where 
 	agg_alter_katg = 2
 	and 
 	pers_tot_adj = 2 --3 
;


-- agg_alter_katg = 3 && pers_tot = 3 (20056)
-- 
select 
	*
from 
	geo_afo_tmp.tmp_corr_hh_pers_tot
where 
 	agg_alter_katg = 3
 	and 
 	pers_tot_adj = 3
;


-- verify the numbers 
select 
	sum(pers_tot_adj) 	as adj, 							--150643
	sum(corr_hh_1)  	as corr_hh_1,						--39068
	sum(corr_hh_2) * 2 	as corr_hh_2, 						--77678
	sum(corr_hh_3) * 3	as corr_hh_3, 						--33897
	sum(corr_hh_1 + corr_hh_2 + corr_hh_3) as hh_tot 		--89206
from 
	geo_afo_tmp.tmp_corr_hh_pers_tot
;


select 
	sum(pers_tot) 	as pers_tot,							--223362
	sum(hh_1) 	 	as hh_1, 								--100818
	sum(hh_2) 	 	as hh_2, 								--116517
	sum(hh_3)		as hh_3, 								--33897
	sum(hh_tot) 	as hh_tot 								--223362
from
	geo_afo_prod.imp_hkt100_aktuell_im2025
where 
	pers_tot = 3
	and 
	hh_plausibilität is not null
;

------------------------------------------
-- Aggr. Ages categories without Houshold data
------------------------------------------

-- There are 335 reli with no houshold data 
select 
	*
from 
	geo_afo_tmp.tmp_anonym_anz_pers
where 
	reli not in(
				select 
					reli 
				from
					geo_afo_tmp.tmp_agg_alter_katg
	)
;


-- create table where the age categories are aggregated and hh data is equal to 0 "hh_plausibilität is null"
-- 335 reli with 0 houshold data 
drop table if exists geo_afo_tmp.tmp_agg_alter_katg_no_hh;
create table 
		geo_afo_tmp.tmp_agg_alter_katg_no_hh
as
select 
	reli 
	,e_koord 
	,n_koord 
	,pers_tot 
	,(m_0_4 
		+ m_5_9 
		+ m_10_14 
		+ m_15_19 
		+ m_20_24 
		+ m_25_29 
		+ m_30_34 
		+ m_35_39 
		+ m_40_44 
		+ m_45_49 
		+ m_50_54 
		+ m_55_59 
		+ m_60_64 
		+ m_65_69 
		+ m_70_74 
		+ m_75_79 
		+ m_80_84 
		+ m_85_89 
		+ m_90 
		+ w_0_4 
		+ w_5_9 
		+ w_10_14 
		+ w_15_19 
		+ w_20_24 
		+ w_25_29 
		+ w_30_34 
		+ w_35_39 
		+ w_40_44 
		+ w_45_49 
		+ w_50_54 
		+ w_55_59 
		+ w_60_64 
		+ w_65_69 
		+ w_70_74 
		+ w_75_79 
		+ w_80_84 
		+ w_85_89 
		+ w_90 ) / 3  as agg_alter_katg
	--,hh_tot 
	,hh_1 
	,hh_2 
	,hh_3 
	,hh_plausibilität 
from 
	geo_afo_tmp.tmp_anonym_anz_pers
where 
	hh_plausibilität is null 
order by
	agg_alter_katg,
	hh_1,
	hh_2,
	hh_3
;

-- 292 Under the possibilities 
select * from geo_afo_tmp.tmp_agg_alter_katg_no_hh where agg_alter_katg <> 3;




select * from geo_afo_prod.imp_hkt100_aktuell_im2025;  															--347408
select * from geo_afo_prod.imp_hkt100_aktuell_im2025 where pers_tot <> 3;   									--272619
select * from geo_afo_prod.imp_hkt100_aktuell_im2025 where pers_tot = 3;    									--74789 all
select * from geo_afo_prod.imp_hkt100_aktuell_im2025 where pers_tot = 3 and hh_plausibilität is not null;  		--74454 with hh data
select * from geo_afo_prod.imp_hkt100_aktuell_im2025 where pers_tot = 3 and hh_plausibilität is null;   		--335 without hh data 
select * from geo_afo_tmp.tmp_corr_hh_pers_tot; 																--74454	corrected data with hh data






--========================================================
-- adjust hkt table with the correction number of persons
--========================================================
-- original table 
select * from geo_afo_prod.imp_hkt100_aktuell_im2025;
-- corrected number of persons 
select * from geo_afo_tmp.tmp_corr_hh_pers_tot;


-- create table for adjustment 
drop table if exists geo_afo_tmp.imp_hkt100_korr_im2025;

create table 
		geo_afo_tmp.imp_hkt100_korr_im2025
as
select 
	*
from geo_afo_prod.imp_hkt100_aktuell_im2025
;

-- count to compare before the update 
select -- original table 
	sum(pers_tot) 	as tot_pers  	--224367
	,sum(hh_1) 		as tot_hh1		--100818
	,sum(hh_2) 		as tot_hh2		--116517
	,sum(hh_3) 		as tot_hh3      --33897
from 
	--geo_afo_prod.imp_hkt100_aktuell_im2025
	geo_afo_tmp.imp_hkt100_korr_im2025
where 
	pers_tot <= 3
;	

select -- corrected table 
	sum(pers_tot_adj) 	as tot_pers		--150643
	,sum(corr_hh_1) 	as tot_hh1		--39068
	,sum(corr_hh_2) 	as tot_hh2		--38839
	,sum(corr_hh_3) 	as tot_hh3		--11299
from 
	geo_afo_tmp.tmp_corr_hh_pers_tot
;




-- update the table with corrected numbers 

update geo_afo_tmp.imp_hkt100_korr_im2025 t0
set 
	pers_tot = t1.pers_tot_adj,
	hh_1 = t1.corr_hh_1,
	hh_2 = t1.corr_hh_2,
	hh_3 = t1.corr_hh_3
from 
	geo_afo_tmp.tmp_corr_hh_pers_tot t1
where 
	t0.reli = t1.reli
;


-- hkt lyer after the correction 
select * from geo_afo_tmp.imp_hkt100_korr_im2025;




--======================================================
-- aggregate "anz_pers" from Hkt group by "Gemeinde" 
-- then copmare the outcome with the source table  
--======================================================
-- using two tables 
-- corrected hkt anz_pers
select * from geo_afo_tmp.imp_hkt100_korr_im2025;
-- one basis layer hkt_gmd_plz6
select * from geo_afo_prod.imp_hkt_auf_plz_gmd_layer;
-- anz_pers from source table 
select * from geo_afo_prod.imp_hkt100_gmd_aktuell_im2025;



drop table if exists tmp_agg_anz_pers;
create temp table tmp_agg_anz_pers
as
select 
	t2.gmd_nr
	,t2.gemeinde
	,sum(t1.pers_tot) 			as sum_pers_tot
	,sum(t1.hh_1) 				as tot_hh1
	,sum(t1.hh_2)				as tot_hh2
	,sum(t1.hh_3) 				as tot_hh3
from 
	geo_afo_tmp.imp_hkt100_korr_im2025 t1
left join 
	geo_afo_prod.imp_hkt_auf_plz_gmd_layer t2 
on
	t1.reli = t2.reli
group by 
	  t2.gmd_nr
	 ,t2.gemeinde
;

select * from tmp_agg_anz_pers;

create temp table tmp_vergleich
as
select 
	 t2.gmd_nr
	,t1.gemeinde
	,t1.sum_pers_tot as agg_pers_tot
	,t2.pers_tot as quel_pers_tot
	,(t1.sum_pers_tot - t2.pers_tot) as diff
from 
	tmp_agg_anz_pers t1
join
	geo_afo_prod.imp_hkt100_gmd_aktuell_im2025 t2
on
	t1.gmd_nr = t2.gmd_nr 
;

select * from tmp_vergleich;

-- Zahlen stimmt für 529 gmd

-- agg zahlen grossser als quelle Zahlen in 803 gmd

-- agg zahlen kleiner als quelle Zahlen in 789 gmd


