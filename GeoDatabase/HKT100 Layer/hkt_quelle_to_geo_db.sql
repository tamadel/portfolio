--============================
-- move table to Geo Database
--============================
-- 1- geo_afo_tmp.hkt100_statpop2023
-- 2- geo_afo_tmp.statpop2023_gmd
-- 3- geo_afo_tmp.statpop2023_noloc
-- 4- geo_afo_tmp.hkt100_statent2021
-- 5- geo_afo_tmp.statent2021_gmd
-- 6- geo_afo_tmp.statent2021_noloc


drop table if exists
	temp.statpop2023_gmd;
create table
	temp.statpop2023_gmd
as
select  
	*
from  
	dblink(
		'geo_database_serverless',
		$POSTGRES$
			select  	  
				*
			from  
				geo_afo_tmp.statpop2023_gmd
		$POSTGRES$
	) as hkt100_statpop2023 (
				erhjahr int4 ,
				pubjahr int4 ,
				gmde int4 ,
				hist_gmde int4 ,
				bbtot int4 ,
				bb11 int4 ,
				bb12 int4 ,
				bb13 int4 ,
				bb14 int4 ,
				bb15 int4 ,
				bb16 int4 ,
				bb21 int4 ,
				bb22 int4 ,
				bb23 int4 ,
				bb24 int4 ,
				bb25 int4 ,
				bb26 int4 ,
				bb27 int4 ,
				bb28 int4 ,
				bb29 int4 ,
				bb30 int4 ,
				bbmtot int4 ,
				bbm01 int4 ,
				bbm02 int4 ,
				bbm03 int4 ,
				bbm04 int4 ,
				bbm05 int4 ,
				bbm06 int4 ,
				bbm07 int4 ,
				bbm08 int4 ,
				bbm09 int4 ,
				bbm10 int4 ,
				bbm11 int4 ,
				bbm12 int4 ,
				bbm13 int4 ,
				bbm14 int4 ,
				bbm15 int4 ,
				bbm16 int4 ,
				bbm17 int4 ,
				bbm18 int4 ,
				bbm19 int4 ,
				bbwtot int4 ,
				bbw01 int4 ,
				bbw02 int4 ,
				bbw03 int4 ,
				bbw04 int4 ,
				bbw05 int4 ,
				bbw06 int4 ,
				bbw07 int4 ,
				bbw08 int4 ,
				bbw09 int4 ,
				bbw10 int4 ,
				bbw11 int4 ,
				bbw12 int4 ,
				bbw13 int4 ,
				bbw14 int4 ,
				bbw15 int4 ,
				bbw16 int4 ,
				bbw17 int4 ,
				bbw18 int4 ,
				bbw19 int4 ,
				bb41 int4 ,
				bb42 int4 ,
				bb43 int4 ,
				bb44 int4 ,
				bb45 int4 ,
				bb46 int4 ,
				bb51 int4 ,
				bb52 int4 ,
				bb53 int4 ,
				bb54 int4 ,
				bb55 int4 ,
				bb56 int4 ,
				hptot int4 ,
				hp01 int4 ,
				hp02 int4 ,
				hp03 int4 ,
				hp04 int4 ,
				hp05 int4 ,
				hp06 int4 ,
				hpi int4 
	)
;


select * from geo_afo_tmp.hkt100_statpop2023;