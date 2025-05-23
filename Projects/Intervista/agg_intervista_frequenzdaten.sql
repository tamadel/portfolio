--============================================
-- final average intervista freq 2023
--============================================
drop table if exists intervista_frequenzdaten.avg_intervista_freq_data;
create table intervista_frequenzdaten.avg_intervista_freq_data
as
select 
	standort_id
	,avg_anzahl_passagen
	,avg_unique_passagen
	,round(avg_0_2km, 5) as avg_0_2km
	,round(avg_2_5km, 5) as avg_2_5km
	,round(avg_5_10km, 5) as avg_5_10km
	,round(avg_10_20km, 5) as avg_10_20km
	,round(avg_20_50km, 5) as avg_20_50km
	,round(avg__50_100km, 5) as avg__50_100km
	,round(avg_100km_plus, 5) as avg_100km_plus
	,round(avg_miv, 5) as avg_miv
	,round(avg_oev, 5) as avg_oev
	,round(avg_fahrrad, 5) as avg_fahrrad
	,round(avg_zu_fuss, 5) as avg_zu_fuss
	,round(avg_sonstige, 5) as avg_sonstige
	,round(avg_arbeit, 5) as avg_arbeit
	,round(avg_einkaufen, 5) as avg_einkaufen
	,round(avg_freizeit, 5) as avg_freizeit
	,round(avg_anderes, 5) as avg_anderes
	,avg_stillstände
	,avg_unique_stillstände
from
	tmp_avg_pecent_intervista_freq
;

select 
	*
from 
	intervista_frequenzdaten.avg_intervista_freq_data
;	


--=======================================================
-- layer erstellen
--=======================================================
drop table if exists intervista_frequenzdaten.agg_intervista_frequ;
create table intervista_frequenzdaten.agg_intervista_frequ
as
select 
	t0.*,
	t1.geo_poly_lv95
from
	intervista_frequenzdaten.avg_intervista_freq_data t0
join
	intervista_frequenzdaten.schweiz_punkte t1
on
	t0.standort_id = t1.id
;

--===================================
-- Average values of the tables(CORRECT)
--===================================
drop table if exists intervista_frequenzdaten.avg_percent_intervista_freq ;
create table intervista_frequenzdaten.avg_percent_intervista_freq 
as                      
select 
	standort_id
	,avg_anzahl_passagen  as avg_anzahl_passagen
	,avg_unique_passagen  as avg_unique_passagen
	,(avg_0_2km::numeric /nullif((avg_0_2km + avg_2_5km + avg_5_10km + avg_10_20km + avg_20_50km + avg__50_100km + avg_100km_plus), 0))*100  as avg_0_2km
	,(avg_2_5km::numeric /nullif((avg_0_2km + avg_2_5km + avg_5_10km + avg_10_20km + avg_20_50km + avg__50_100km + avg_100km_plus), 0)) * 100  as avg_2_5km
	,(avg_5_10km::numeric /nullif((avg_0_2km + avg_2_5km + avg_5_10km + avg_10_20km + avg_20_50km + avg__50_100km + avg_100km_plus), 0)) * 100  as avg_5_10km
	,(avg_10_20km::numeric /nullif((avg_0_2km + avg_2_5km + avg_5_10km + avg_10_20km + avg_20_50km + avg__50_100km + avg_100km_plus), 0)) * 100  as avg_10_20km
	,(avg_20_50km::numeric /nullif((avg_0_2km + avg_2_5km + avg_5_10km + avg_10_20km + avg_20_50km + avg__50_100km + avg_100km_plus), 0)) * 100  as avg_20_50km
	,(avg__50_100km::numeric /nullif((avg_0_2km + avg_2_5km + avg_5_10km + avg_10_20km + avg_20_50km + avg__50_100km + avg_100km_plus), 0)) * 100  as avg__50_100km
	,(avg_100km_plus::numeric /nullif((avg_0_2km + avg_2_5km + avg_5_10km + avg_10_20km + avg_20_50km + avg__50_100km + avg_100km_plus), 0)) * 100  as avg_100km_plus
	,(avg_miv::numeric /nullif((avg_miv + avg_oev + avg_fahrrad + avg_zu_fuss + avg_sonstige), 0)) * 100  as avg_miv
	,(avg_oev::numeric /nullif((avg_miv + avg_oev + avg_fahrrad + avg_zu_fuss + avg_sonstige), 0)) * 100  as avg_oev
	,(avg_fahrrad::numeric /nullif((avg_miv + avg_oev + avg_fahrrad + avg_zu_fuss + avg_sonstige), 0)) * 100  as avg_fahrrad
	,(avg_zu_fuss::numeric /nullif((avg_miv + avg_oev + avg_fahrrad + avg_zu_fuss + avg_sonstige), 0)) * 100  as avg_zu_fuss
	,(avg_sonstige::numeric /nullif((avg_miv + avg_oev + avg_fahrrad + avg_zu_fuss + avg_sonstige), 0)) * 100  as avg_sonstige
	,(avg_arbeit::numeric /nullif((avg_arbeit + avg_einkaufen + avg_freizeit + avg_anderes), 0)) * 100  as avg_arbeit
	,(avg_einkaufen::numeric /nullif((avg_arbeit + avg_einkaufen + avg_freizeit + avg_anderes), 0)) * 100  as avg_einkaufen
	,(avg_freizeit::numeric /nullif((avg_arbeit + avg_einkaufen + avg_freizeit + avg_anderes), 0)) * 100  as avg_freizeit
	,(avg_anderes::numeric /nullif((avg_arbeit + avg_einkaufen + avg_freizeit + avg_anderes), 0)) * 100  as avg_anderes
	,avg_stillstände as avg_stillstände
	,avg_unique_stillstände as avg_unique_stillstände
from 
	tmp_avg_intervista_freq
;

--(sum_0_2km + sum_2_5km + sum_5_10km + sum_10_20km + sum_20_50km + sum__50_100km + sum_100km_plus)
--(sum_miv + sum_oev + sum_fahrrad + sum_zu_fuss + sum_sonstige)
--(sum_arbeit + sum_einkaufen + sum_freizeit + sum_anderes)

select 
	*
from 
	intervista_frequenzdaten.avg_percent_intervista_freq 
where 
	standort_id = 'p_51464'
;

--==================================================
-- SUMME VONE 13 FILES UND PERCENTAGE ZU ANZAHL  (CORRECT)
--==================================================
drop table if exists tmp_avg_intervista_freq;
create temp table tmp_avg_intervista_freq
as
select 
	standort_id
	,sum(anzahl_passagen::numeric) as sum_anzahl_passagen
	,sum(unique_passagen::numeric) as sum_unique_passagen
	,(sum(_0_2km::numeric)) as sum_0_2km
	,(sum(_2_5km::numeric)) as sum_2_5km
	,(sum(_5_10km::numeric)) as sum_5_10km
	,(sum(_10_20km::numeric)) as sum_10_20km
	,(sum(_20_50km::numeric)) as sum_20_50km
	,(sum(_50_100km::numeric)) as sum__50_100km
	,(sum(_100km_plus::numeric)) as sum_100km_plus
	,(sum(miv::numeric)) as sum_miv
	,(sum(oev::numeric)) as sum_oev
	,(sum(fahrrad::numeric)) as sum_fahrrad
	,(sum(zu_fuss::numeric)) as sum_zu_fuss
	,(sum(sonstige::numeric)) as sum_sonstige
	,(sum(arbeit::numeric)) as sum_arbeit
	,(sum(einkaufen::numeric)) as sum_einkaufen
	,(sum(freizeit::numeric)) as sum_freizeit
	,(sum(anderes::numeric)) as sum_anderes
	,sum(stillstände::numeric) as sum_stillstände
	,sum(unique_stillstände::numeric) as sum_unique_stillstände
	from (
	    select
		    "Standort_ID" as standort_id
		    ,"Anzahl_Passagen" as anzahl_passagen
		    ,"Unique_Passagen" as unique_passagen
		    ,"Anzahl_Passagen" * ("0-2km"/100)  as _0_2km
			,"Anzahl_Passagen" * ("2-5km"/100) as _2_5km
			,"Anzahl_Passagen" * ("5-10km"/100) as _5_10km
			,"Anzahl_Passagen" * ("10-20km"/100) as _10_20km
			,"Anzahl_Passagen" * ("20-50km"/100) as _20_50km
			,"Anzahl_Passagen" * ("50-100km"/100) as _50_100km
			,"Anzahl_Passagen" * ("100km+"/100) as _100km_plus
			,"Anzahl_Passagen" * ("MIV"/100) as miv
			,"Anzahl_Passagen" * ("OeV"/100) as oev
			,"Anzahl_Passagen" * ("Fahrrad"/100) as fahrrad
			,"Anzahl_Passagen" * ("Zu Fuss"/100) as zu_fuss
			,"Anzahl_Passagen" * ("Sonstige"/100) as sonstige
			,"Anzahl_Passagen" * ("Arbeit"/100) as arbeit
			,"Anzahl_Passagen" * ("Einkaufen"/100) as einkaufen
			,"Anzahl_Passagen" * ("Freizeit"/100) as freizeit
			,"Anzahl_Passagen" * ("Anderes"/100) as anderes
			,"Stillstände" as stillstände
			,"Unique_Stillstände" as unique_stillstände  
	    from
	    	intervista_frequenzdaten.daten_m01
	    union all 
	    select
		    "Standort_ID" as standort_id
		    ,"Anzahl_Passagen" as anzahl_passagen
		    ,"Unique_Passagen" as unique_passagen
		    ,"Anzahl_Passagen" * ("0-2km"/100)  as _0_2km
			,"Anzahl_Passagen" * ("2-5km"/100) as _2_5km
			,"Anzahl_Passagen" * ("5-10km"/100) as _5_10km
			,"Anzahl_Passagen" * ("10-20km"/100) as _10_20km
			,"Anzahl_Passagen" * ("20-50km"/100) as _20_50km
			,"Anzahl_Passagen" * ("50-100km"/100) as _50_100km
			,"Anzahl_Passagen" * ("100km+"/100) as _100km_plus
			,"Anzahl_Passagen" * ("MIV"/100) as miv
			,"Anzahl_Passagen" * ("OeV"/100) as oev
			,"Anzahl_Passagen" * ("Fahrrad"/100) as fahrrad
			,"Anzahl_Passagen" * ("Zu Fuss"/100) as zu_fuss
			,"Anzahl_Passagen" * ("Sonstige"/100) as sonstige
			,"Anzahl_Passagen" * ("Arbeit"/100) as arbeit
			,"Anzahl_Passagen" * ("Einkaufen"/100) as einkaufen
			,"Anzahl_Passagen" * ("Freizeit"/100) as freizeit
			,"Anzahl_Passagen" * ("Anderes"/100) as anderes
			,"Stillstände" as stillstände
			,"Unique_Stillstände" as unique_stillstände     	 
	    from
	    	intervista_frequenzdaten.daten_m02
	    union all 
	    select 
		    "Standort_ID" as standort_id
		    ,"Anzahl_Passagen" as anzahl_passagen
		    ,"Unique_Passagen" as unique_passagen
		    ,"Anzahl_Passagen" * ("0-2km"/100)  as _0_2km
			,"Anzahl_Passagen" * ("2-5km"/100) as _2_5km
			,"Anzahl_Passagen" * ("5-10km"/100) as _5_10km
			,"Anzahl_Passagen" * ("10-20km"/100) as _10_20km
			,"Anzahl_Passagen" * ("20-50km"/100) as _20_50km
			,"Anzahl_Passagen" * ("50-100km"/100) as _50_100km
			,"Anzahl_Passagen" * ("100km+"/100) as _100km_plus
			,"Anzahl_Passagen" * ("MIV"/100) as miv
			,"Anzahl_Passagen" * ("OeV"/100) as oev
			,"Anzahl_Passagen" * ("Fahrrad"/100) as fahrrad
			,"Anzahl_Passagen" * ("Zu Fuss"/100) as zu_fuss
			,"Anzahl_Passagen" * ("Sonstige"/100) as sonstige
			,"Anzahl_Passagen" * ("Arbeit"/100) as arbeit
			,"Anzahl_Passagen" * ("Einkaufen"/100) as einkaufen
			,"Anzahl_Passagen" * ("Freizeit"/100) as freizeit
			,"Anzahl_Passagen" * ("Anderes"/100) as anderes
			,"Stillstände" as stillstände
			,"Unique_Stillstände" as unique_stillstände 
	    from
	    	intervista_frequenzdaten.daten_m03
	    union all 
	    select
		    "Standort_ID" as standort_id
		    ,"Anzahl_Passagen" as anzahl_passagen
		    ,"Unique_Passagen" as unique_passagen
		    ,"Anzahl_Passagen" * ("0-2km"/100)  as _0_2km
			,"Anzahl_Passagen" * ("2-5km"/100) as _2_5km
			,"Anzahl_Passagen" * ("5-10km"/100) as _5_10km
			,"Anzahl_Passagen" * ("10-20km"/100) as _10_20km
			,"Anzahl_Passagen" * ("20-50km"/100) as _20_50km
			,"Anzahl_Passagen" * ("50-100km"/100) as _50_100km
			,"Anzahl_Passagen" * ("100km+"/100) as _100km_plus
			,"Anzahl_Passagen" * ("MIV"/100) as miv
			,"Anzahl_Passagen" * ("OeV"/100) as oev
			,"Anzahl_Passagen" * ("Fahrrad"/100) as fahrrad
			,"Anzahl_Passagen" * ("Zu Fuss"/100) as zu_fuss
			,"Anzahl_Passagen" * ("Sonstige"/100) as sonstige
			,"Anzahl_Passagen" * ("Arbeit"/100) as arbeit
			,"Anzahl_Passagen" * ("Einkaufen"/100) as einkaufen
			,"Anzahl_Passagen" * ("Freizeit"/100) as freizeit
			,"Anzahl_Passagen" * ("Anderes"/100) as anderes
			,"Stillstände" as stillstände
			,"Unique_Stillstände" as unique_stillstände 
	    from
	    	intervista_frequenzdaten.daten_m04
	    union all 
	    select
		    "Standort_ID" as standort_id
		    ,"Anzahl_Passagen" as anzahl_passagen
		    ,"Unique_Passagen" as unique_passagen
		    ,"Anzahl_Passagen" * ("0-2km"/100)  as _0_2km
			,"Anzahl_Passagen" * ("2-5km"/100) as _2_5km
			,"Anzahl_Passagen" * ("5-10km"/100) as _5_10km
			,"Anzahl_Passagen" * ("10-20km"/100) as _10_20km
			,"Anzahl_Passagen" * ("20-50km"/100) as _20_50km
			,"Anzahl_Passagen" * ("50-100km"/100) as _50_100km
			,"Anzahl_Passagen" * ("100km+"/100) as _100km_plus
			,"Anzahl_Passagen" * ("MIV"/100) as miv
			,"Anzahl_Passagen" * ("OeV"/100) as oev
			,"Anzahl_Passagen" * ("Fahrrad"/100) as fahrrad
			,"Anzahl_Passagen" * ("Zu Fuss"/100) as zu_fuss
			,"Anzahl_Passagen" * ("Sonstige"/100) as sonstige
			,"Anzahl_Passagen" * ("Arbeit"/100) as arbeit
			,"Anzahl_Passagen" * ("Einkaufen"/100) as einkaufen
			,"Anzahl_Passagen" * ("Freizeit"/100) as freizeit
			,"Anzahl_Passagen" * ("Anderes"/100) as anderes
			,"Stillstände" as stillstände
			,"Unique_Stillstände" as unique_stillstände 
	    from
	    	intervista_frequenzdaten.daten_m05
	    union all 
	    select
		   "Standort_ID" as standort_id
		    ,"Anzahl_Passagen" as anzahl_passagen
		    ,"Unique_Passagen" as unique_passagen
		    ,"Anzahl_Passagen" * ("0-2km"/100)  as _0_2km
			,"Anzahl_Passagen" * ("2-5km"/100) as _2_5km
			,"Anzahl_Passagen" * ("5-10km"/100) as _5_10km
			,"Anzahl_Passagen" * ("10-20km"/100) as _10_20km
			,"Anzahl_Passagen" * ("20-50km"/100) as _20_50km
			,"Anzahl_Passagen" * ("50-100km"/100) as _50_100km
			,"Anzahl_Passagen" * ("100km+"/100) as _100km_plus
			,"Anzahl_Passagen" * ("MIV"/100) as miv
			,"Anzahl_Passagen" * ("OeV"/100) as oev
			,"Anzahl_Passagen" * ("Fahrrad"/100) as fahrrad
			,"Anzahl_Passagen" * ("Zu Fuss"/100) as zu_fuss
			,"Anzahl_Passagen" * ("Sonstige"/100) as sonstige
			,"Anzahl_Passagen" * ("Arbeit"/100) as arbeit
			,"Anzahl_Passagen" * ("Einkaufen"/100) as einkaufen
			,"Anzahl_Passagen" * ("Freizeit"/100) as freizeit
			,"Anzahl_Passagen" * ("Anderes"/100) as anderes
			,"Stillstände" as stillstände
			,"Unique_Stillstände" as unique_stillstände  
	    from
	    	intervista_frequenzdaten.daten_m06
	    union all 
	    select
		    "Standort_ID" as standort_id
		    ,"Anzahl_Passagen" as anzahl_passagen
		    ,"Unique_Passagen" as unique_passagen
		    ,"Anzahl_Passagen" * ("0-2km"/100)  as _0_2km
			,"Anzahl_Passagen" * ("2-5km"/100) as _2_5km
			,"Anzahl_Passagen" * ("5-10km"/100) as _5_10km
			,"Anzahl_Passagen" * ("10-20km"/100) as _10_20km
			,"Anzahl_Passagen" * ("20-50km"/100) as _20_50km
			,"Anzahl_Passagen" * ("50-100km"/100) as _50_100km
			,"Anzahl_Passagen" * ("100km+"/100) as _100km_plus
			,"Anzahl_Passagen" * ("MIV"/100) as miv
			,"Anzahl_Passagen" * ("OeV"/100) as oev
			,"Anzahl_Passagen" * ("Fahrrad"/100) as fahrrad
			,"Anzahl_Passagen" * ("Zu Fuss"/100) as zu_fuss
			,"Anzahl_Passagen" * ("Sonstige"/100) as sonstige
			,"Anzahl_Passagen" * ("Arbeit"/100) as arbeit
			,"Anzahl_Passagen" * ("Einkaufen"/100) as einkaufen
			,"Anzahl_Passagen" * ("Freizeit"/100) as freizeit
			,"Anzahl_Passagen" * ("Anderes"/100) as anderes
			,"Stillstände" as stillstände
			,"Unique_Stillstände" as unique_stillstände 
	    from
	    	intervista_frequenzdaten.daten_m07
	    union all 
	    select
		    "Standort_ID" as standort_id
		    ,"Anzahl_Passagen" as anzahl_passagen
		    ,"Unique_Passagen" as unique_passagen
		    ,"Anzahl_Passagen" * ("0-2km"/100)  as _0_2km
			,"Anzahl_Passagen" * ("2-5km"/100) as _2_5km
			,"Anzahl_Passagen" * ("5-10km"/100) as _5_10km
			,"Anzahl_Passagen" * ("10-20km"/100) as _10_20km
			,"Anzahl_Passagen" * ("20-50km"/100) as _20_50km
			,"Anzahl_Passagen" * ("50-100km"/100) as _50_100km
			,"Anzahl_Passagen" * ("100km+"/100) as _100km_plus
			,"Anzahl_Passagen" * ("MIV"/100) as miv
			,"Anzahl_Passagen" * ("OeV"/100) as oev
			,"Anzahl_Passagen" * ("Fahrrad"/100) as fahrrad
			,"Anzahl_Passagen" * ("Zu Fuss"/100) as zu_fuss
			,"Anzahl_Passagen" * ("Sonstige"/100) as sonstige
			,"Anzahl_Passagen" * ("Arbeit"/100) as arbeit
			,"Anzahl_Passagen" * ("Einkaufen"/100) as einkaufen
			,"Anzahl_Passagen" * ("Freizeit"/100) as freizeit
			,"Anzahl_Passagen" * ("Anderes"/100) as anderes
			,"Stillstände" as stillstände
			,"Unique_Stillstände" as unique_stillstände 
	    from
	    	intervista_frequenzdaten.daten_m08
	    union all 
	    select
		    "Standort_ID" as standort_id
		    ,"Anzahl_Passagen" as anzahl_passagen
		    ,"Unique_Passagen" as unique_passagen
		    ,"Anzahl_Passagen" * ("0-2km"/100)  as _0_2km
			,"Anzahl_Passagen" * ("2-5km"/100) as _2_5km
			,"Anzahl_Passagen" * ("5-10km"/100) as _5_10km
			,"Anzahl_Passagen" * ("10-20km"/100) as _10_20km
			,"Anzahl_Passagen" * ("20-50km"/100) as _20_50km
			,"Anzahl_Passagen" * ("50-100km"/100) as _50_100km
			,"Anzahl_Passagen" * ("100km+"/100) as __100km_plus
			,"Anzahl_Passagen" * ("MIV"/100) as miv
			,"Anzahl_Passagen" * ("OeV"/100) as oev
			,"Anzahl_Passagen" * ("Fahrrad"/100) as fahrrad
			,"Anzahl_Passagen" * ("Zu Fuss"/100) as zu_fuss
			,"Anzahl_Passagen" * ("Sonstige"/100) as sonstige
			,"Anzahl_Passagen" * ("Arbeit"/100) as arbeit
			,"Anzahl_Passagen" * ("Einkaufen"/100) as einkaufen
			,"Anzahl_Passagen" * ("Freizeit"/100) as freizeit
			,"Anzahl_Passagen" * ("Anderes"/100) as anderes
			,"Stillstände" as stillstände
			,"Unique_Stillstände" as unique_stillstände  
	    from
	    	intervista_frequenzdaten.daten_m09
	    union all 
	    select
		   "Standort_ID" as standort_id
		    ,"Anzahl_Passagen" as anzahl_passagen
		    ,"Unique_Passagen" as unique_passagen
		    ,"Anzahl_Passagen" * ("0-2km"/100)  as _0_2km
			,"Anzahl_Passagen" * ("2-5km"/100) as _2_5km
			,"Anzahl_Passagen" * ("5-10km"/100) as _5_10km
			,"Anzahl_Passagen" * ("10-20km"/100) as _10_20km
			,"Anzahl_Passagen" * ("20-50km"/100) as _20_50km
			,"Anzahl_Passagen" * ("50-100km"/100) as _50_100km
			,"Anzahl_Passagen" * ("100km+"/100) as __100km_plus
			,"Anzahl_Passagen" * ("MIV"/100) as miv
			,"Anzahl_Passagen" * ("OeV"/100) as oev
			,"Anzahl_Passagen" * ("Fahrrad"/100) as fahrrad
			,"Anzahl_Passagen" * ("Zu Fuss"/100) as zu_fuss
			,"Anzahl_Passagen" * ("Sonstige"/100) as sonstige
			,"Anzahl_Passagen" * ("Arbeit"/100) as arbeit
			,"Anzahl_Passagen" * ("Einkaufen"/100) as einkaufen
			,"Anzahl_Passagen" * ("Freizeit"/100) as freizeit
			,"Anzahl_Passagen" * ("Anderes"/100) as anderes
			,"Stillstände" as stillstände
			,"Unique_Stillstände" as unique_stillstände 
	    from
	    	intervista_frequenzdaten.daten_m10
	    union all 
	    select
		   "Standort_ID" as standort_id
		    ,"Anzahl_Passagen" as anzahl_passagen
		    ,"Unique_Passagen" as unique_passagen
		    ,"Anzahl_Passagen" * ("0-2km"/100)  as _0_2km
			,"Anzahl_Passagen" * ("2-5km"/100) as _2_5km
			,"Anzahl_Passagen" * ("5-10km"/100) as _5_10km
			,"Anzahl_Passagen" * ("10-20km"/100) as _10_20km
			,"Anzahl_Passagen" * ("20-50km"/100) as _20_50km
			,"Anzahl_Passagen" * ("50-100km"/100) as _50_100km
			,"Anzahl_Passagen" * ("100km+"/100) as __100km_plus
			,"Anzahl_Passagen" * ("MIV"/100) as miv
			,"Anzahl_Passagen" * ("OeV"/100) as oev
			,"Anzahl_Passagen" * ("Fahrrad"/100) as fahrrad
			,"Anzahl_Passagen" * ("Zu Fuss"/100) as zu_fuss
			,"Anzahl_Passagen" * ("Sonstige"/100) as sonstige
			,"Anzahl_Passagen" * ("Arbeit"/100) as arbeit
			,"Anzahl_Passagen" * ("Einkaufen"/100) as einkaufen
			,"Anzahl_Passagen" * ("Freizeit"/100) as freizeit
			,"Anzahl_Passagen" * ("Anderes"/100) as anderes
			,"Stillstände" as stillstände
			,"Unique_Stillstände" as unique_stillstände  
	    from
	    	intervista_frequenzdaten.daten_m11
	    union all 
	    select
		   "Standort_ID" as standort_id
		    ,"Anzahl_Passagen" as anzahl_passagen
		    ,"Unique_Passagen" as unique_passagen
		    ,"Anzahl_Passagen" * ("0-2km"/100)  as _0_2km
			,"Anzahl_Passagen" * ("2-5km"/100) as _2_5km
			,"Anzahl_Passagen" * ("5-10km"/100) as _5_10km
			,"Anzahl_Passagen" * ("10-20km"/100) as _10_20km
			,"Anzahl_Passagen" * ("20-50km"/100) as _20_50km
			,"Anzahl_Passagen" * ("50-100km"/100) as _50_100km
			,"Anzahl_Passagen" * ("100km+"/100) as __100km_plus
			,"Anzahl_Passagen" * ("MIV"/100) as miv
			,"Anzahl_Passagen" * ("OeV"/100) as oev
			,"Anzahl_Passagen" * ("Fahrrad"/100) as fahrrad
			,"Anzahl_Passagen" * ("Zu Fuss"/100) as zu_fuss
			,"Anzahl_Passagen" * ("Sonstige"/100) as sonstige
			,"Anzahl_Passagen" * ("Arbeit"/100) as arbeit
			,"Anzahl_Passagen" * ("Einkaufen"/100) as einkaufen
			,"Anzahl_Passagen" * ("Freizeit"/100) as freizeit
			,"Anzahl_Passagen" * ("Anderes"/100) as anderes
			,"Stillstände" as stillstände
			,"Unique_Stillstände" as unique_stillstände 
	    from
	    	intervista_frequenzdaten.daten_m12
	    union all 
	    select
		    "Standort_ID" as standort_id
		    ,"Anzahl_Passagen" as anzahl_passagen
		    ,"Unique_Passagen" as unique_passagen
		    ,"Anzahl_Passagen" * ("0-2km"/100)  as _0_2km
			,"Anzahl_Passagen" * ("2-5km"/100) as _2_5km
			,"Anzahl_Passagen" * ("5-10km"/100) as _5_10km
			,"Anzahl_Passagen" * ("10-20km"/100) as _10_20km
			,"Anzahl_Passagen" * ("20-50km"/100) as _20_50km
			,"Anzahl_Passagen" * ("50-100km"/100) as _50_100km
			,"Anzahl_Passagen" * ("100km+"/100) as __100km_plus
			,"Anzahl_Passagen" * ("MIV"/100) as miv
			,"Anzahl_Passagen" * ("OeV"/100) as oev
			,"Anzahl_Passagen" * ("Fahrrad"/100) as fahrrad
			,"Anzahl_Passagen" * ("Zu Fuss"/100) as zu_fuss
			,"Anzahl_Passagen" * ("Sonstige"/100) as sonstige
			,"Anzahl_Passagen" * ("Arbeit"/100) as arbeit
			,"Anzahl_Passagen" * ("Einkaufen"/100) as einkaufen
			,"Anzahl_Passagen" * ("Freizeit"/100) as freizeit
			,"Anzahl_Passagen" * ("Anderes"/100) as anderes
			,"Stillstände" as stillstände
			,"Unique_Stillstände" as unique_stillstände 
	    from
	    	intervista_frequenzdaten.daten_m13
) as combined_data
group by 
    standort_id
;

select 
	*
from 
	tmp_sum_intervista_freq
where 
	standort_id = 'p_51464'
;	


select 
	*
from 
	intervista_frequenzdaten.daten_m13
;  


--====================================================================================================
-- TEST STANDORT_ID 
-- gibt es Fälle, die Anzahl_Passagen gibt aber keine Verteilung (0 bei alle) or (0 bei eien Branche)
--====================================================================================================
select 
    *
from (
    select
    	* 
    from
    	intervista_frequenzdaten.daten_m01
    union all 
    select
    	* 
    from
    	intervista_frequenzdaten.daten_m02
    union all 
    select 
    	* 
    from
    	intervista_frequenzdaten.daten_m03
    union all 
    select
    	* 
    from
    	intervista_frequenzdaten.daten_m04
    union all 
    select
    	* 
    from
    	intervista_frequenzdaten.daten_m05
    union all 
    select
    	* 
    from
    	intervista_frequenzdaten.daten_m06
    union all 
    select
    	* 
    from
    	intervista_frequenzdaten.daten_m07
    union all 
    select
    	* 
    from
    	intervista_frequenzdaten.daten_m08
    union all 
    select
    	* 
    from
    	intervista_frequenzdaten.daten_m09
    union all 
    select
    	* 
    from
    	intervista_frequenzdaten.daten_m10
    union all 
    select
    	* 
    from
    	intervista_frequenzdaten.daten_m11
    union all 
    select
    	* 
    from
    	intervista_frequenzdaten.daten_m12
    union all 
    select
    	* 
    from
    	intervista_frequenzdaten.daten_m13
) as combined_data
where 
	"Standort_ID" = 'p_51464'
;



--=============================================
-- Polygone Transformation
--============================================	
create table intervista_frequenzdaten.schweiz_punkte
as
select 
   	id,
   	st_setsrid(geom, 2056) as geo_poly_lv95
from 
   	public.intervista_schweiz
;
   	
select 
	*
from 
	intervista_frequenzdaten.schweiz_punkte
;

--=======================================================
-- layer erstellen
--=======================================================
drop table if exists intervista_frequenzdaten.agg_intervista_frequ_poly;
create table intervista_frequenzdaten.agg_intervista_frequ_poly
as
select 
	t0.*,
	t1.geo_poly_lv95
from
	intervista_frequenzdaten.avg_percent_intervista_freq t0 -- FALS table 
join
	intervista_frequenzdaten.schweiz_punkte t1
on
	t0.standort_id = t1.id
order by 
	t0.standort_id
;

select 
	*
from 
	intervista_frequenzdaten.agg_intervista_frequ_poly
;	

select 
	*
from 
	intervista_frequenzdaten.avg_percent_intervista_freq
;
--===========TEST=======================

select 
	standort_id
	,str_id
from
	intervista_frequenzdaten.agg_intervista_frequ_poly
where 
	standort_id like '%s_%'
;




select 
	*
from
	geo_afo_prod.mv_lay_poi_aktuell
where 
	poi_id = 377065
;







































