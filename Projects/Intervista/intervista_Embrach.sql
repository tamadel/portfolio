--==============================
-- Intervista - Embrach Data
--==============================
--============================
-- Embrach Tabelle 
--===========================
drop table if exists intervista_frequenzdaten.embrach_intervista_freq_daten;
create table intervista_frequenzdaten.embrach_intervista_freq_daten
as
select 
	*
from
	intervista_frequenzdaten.agg_intervista_frequ_poly
where
	standort_id in (
					'p_223934',
					'p_158565',
					'p_192237',
					'p_208302',
					'p_15702',
					'p_204698',
					'p_214549',
					'p_5321',
					'p_61179',
					'p_193596',
					'p_1499',
					'p_745',
					'p_29415',
					'p_2520',
					'p_247903',
					'p_27908',
					's_234627',
					'p_199688',
					'p_29416',
					'p_15696',
					'p_1929',
					'p_214517',
					'p_194381',
					'p_24489',
					'p_214104',
					'p_50674',
					'p_50673',
					's_231936',
					'p_200043'
)
;

--====================================
-- Embrach Tabelle und ihre Umgebung
--====================================

drop table if exists intervista_frequenzdaten.um_embrach_intervista_freq_daten;
create table intervista_frequenzdaten.um_embrach_intervista_freq_daten
as
select 
	t0.*
	,t1.poi_typ_list
	,t1.poi_name_list
	,t1.ort
from
	intervista_frequenzdaten.agg_intervista_frequ_poly t0
left join 
	intervista_frequenzdaten.intervesta_freq_mit_poi_id t1
on
	t0.standort_id = t1.standort_id
where
	t0.standort_id in (
					'p_15705','p_199950','p_63829','p_168999','p_193391','p_198912','p_32110','p_51351','p_51352','p_58979'
					,'s_17567','s_17916','s_365938','s_432292','p_17576','p_192635','p_199188','p_201107','p_207039','p_225583'
					,'p_50748','p_1499','p_15696','p_15702','p_158565','p_192237','p_1929','p_193596','p_194381','p_199688','p_200043'
					,'p_204698','p_208302','p_214104','p_214517','p_214549','p_223934','p_24489','p_2520','p_27908','p_29415','p_29416'
					,'p_50673','p_50674','p_5321','p_61179','p_745','s_231936','s_234627','s_247903','p_130061','p_15284','p_15285','p_198917'
					,'p_201017','p_202031','p_203190','p_224054','p_226200','p_48498','p_51870','s_16015','s_238301','s_348796','s_36826'
					,'s_377065','s_39644','p_15711','p_191206','p_200119','p_207039','p_226060','p_23996','p_3355','p_51418','s_26762','s_27380'
					,'s_350485','p_130171','p_191714','p_201832','p_206595','p_3353','p_51126','p_51127','p_15281','p_186415','p_187229','p_188169'
					,'p_196322','p_214480','p_226919','p_27445','p_3304','p_63260','p_7830','p_8324','s_14668','s_291652','s_291654','s_296405'
					,'s_298608','s_32034','s_321612','s_330578','s_370358','s_40501','s_407224','s_449770','s_5355','s_5356','s_5357','p_129859'
					,'p_131432','p_136648','p_1470','p_15249','p_15250','p_15252','p_15253','p_15266','p_15267','p_15268','p_15269','p_15271'
					,'p_158568','p_17749','p_185855','p_193133','p_194859','p_195693','p_196918','p_197334','p_197358','p_197820','p_197983','p_199250'
					,'p_199912','p_200006','p_200385','p_201492','p_201916','p_202129','p_202599','p_203337','p_203520','p_203793','p_204085'
					,'p_204270','p_205856','p_206545','p_207670','p_207788','p_214547','p_224007','p_225798','p_225908','p_226198','p_23117','p_24561'
					,'p_27713','p_29346','p_3299','p_3301','p_33914','p_34769','p_44877','p_50554','p_59130','p_59651','p_7829','p_8323','s_14668'
					,'s_226527','s_227959','s_230754','s_230808','s_231753','s_232285','s_232570','s_234780','s_239438','s_244028','s_24693','s_263860','s_278980'
					,'s_278981','s_290140','s_291654','s_293281','s_302366','s_303266','s_303894','s_319415','s_319416','s_319417','s_319418','s_321612','s_323599'
					,'s_327574','s_327577','s_338791','s_349675','s_352010','s_354273','s_362817','s_365187','s_365188','s_366150','s_370282','s_370358','s_376488'
					,'s_376490','s_376491','s_376492','s_376493','s_376494','s_376495','s_376498','s_376504','s_377374','s_377375','s_377376','s_400963','s_402430'
					,'s_404601','s_404602','s_40501','s_407224','s_412903','s_43439','s_440427','s_446449','s_50675','s_5352','s_5355','s_5356','s_5357'
	)
;



--////////////////////////////////////////////////////////
--==================================
-- Embrach iso-5min + oberembrach
-- Tabelle für Peter
--==================================

-- Daten sichten
select 
	*
from 
	intervista_frequenzdaten.avg_percent_intervista_freq
;

select 
	*
from 
	intervista_frequenzdaten.agg_intervista_frequ_poly
;

select 
	*
from 
	intervista_frequenzdaten.embrach_intervista_freq_daten
;


select 
	*
from 
	intervista_frequenzdaten.um_embrach_intervista_freq_daten
;

-- intervista Frequ Tabelle bearbeiten und poi_id von standort_id ziehen  
alter table
	intervista_frequenzdaten.agg_intervista_frequ_poly
add column poi_id INT,
add column str_id INT
;

update
	intervista_frequenzdaten.agg_intervista_frequ_poly
set 
    poi_id = case 
                when
                	standort_id like 'p_%' then CAST(SUBSTRING(standort_id, 3, LENGTH(standort_id) - 2) as INT)
                else 
                	null 
             end,
    str_id = case 
                when
                	standort_id like 's_%' then CAST(SUBSTRING(standort_id, 3, LENGTH(standort_id) - 2) as INT)
                else 
                	null 
             end
;

select 
	*
from
	intervista_frequenzdaten.agg_intervista_frequ_poly
;





-- map the table to poi_id
drop table if exists tmp_embrach_data_poi;
create temp table tmp_embrach_data_poi
as
select 
	t0.*
	,t1.poi_typ
	,t1.company 
	,t1.ort
	,t1.plz4
from 
	intervista_frequenzdaten.agg_intervista_frequ_poly t0
left join
	geo_afo_prod.mv_lay_poi_aktuell t1
on
	t1.poi_id = t0.poi_id
;



-- map the table to str_id
drop table if exists tmp_embrach_data_str;
create temp table tmp_embrach_data_str
as
select 
	t0.*
	,t1.str_type 
	,t1.dwv_alle
	,t1.dtv_alle 
from 
	intervista_frequenzdaten.agg_intervista_frequ_poly t0
left join
	geo_afo_prod.mv_lay_str_freq_aktuell t1
on
	t1.str_id = t0.str_id
	or 
	t1.str_id = t0.poi_id
;


--vollständig Tabelle Embrach 
drop table if exists intervista_frequenzdaten.afo_embrach_intervista_data;
create table intervista_frequenzdaten.afo_embrach_intervista_data
as
select
	t0.*
	,t1.str_type 
	,t1.dwv_alle
	,t1.dtv_alle
from 
	tmp_embrach_data_poi t0
left join
	intervista_frequenzdaten.embrach_str_freq_data t1
on
	t0.standort_id = t1.standort_id
	or 
	t0.poi_id = t1.str_id 
where
	t0.standort_id in (
					'p_223934',
					'p_158565', 'p_3353', 'p_51126', 'p_51127', 'p_15705', 'p_63829', 'p_199950',
					'p_192237', 'p_51352', 'p_51351', 'p_168999', 'p_198912', 'p_58979', 'p_32110',
					'p_208302', 'p_93391', 's_432292', 's_365938', 's_17567', 's_17916', 'p_3355',
					'p_15702', 'p_51418', 'p_15711', 'p_191206', 's_350485', 's_26762', 'p_200119',
					'p_204698', 'p_23996', 'p_27380', 'p_226060', 
					'p_214549','p_5321','p_61179','p_193596','p_1499','p_745','p_29415','p_2520',
					'p_247903','p_27908','s_234627','p_199688','p_29416','p_15696','p_1929','p_214517',
					'p_194381','p_24489','p_214104','p_50674','p_50673','s_231936','p_200043','p_207039'			
	)
;




-- Table for Peter
select 
	standort_id
	,avg_anzahl_passagen
	,avg_unique_passagen
	,avg_0_2km
	,avg_2_5km
	,avg_5_10km
	,avg_10_20km
	,avg_20_50km
	,avg_50_100km
	,avg_100km_plus
	,avg_miv
	,avg_oev
	,avg_fahrrad
	,avg_zu_fuss
	,avg_sonstige
	,avg_arbeit
	,avg_einkaufen
	,avg_freizeit
	,avg_anderes
	,avg_anz_stillstände 
	,avg_anz_unique_stillstände
	,poi_typ 
	,company
	,str_type 
	,dwv_alle  
	,dtv_alle 
from 
	intervista_frequenzdaten.afo_embrach_intervista_data
;

--===================================================================





--vollständig Tabelle Embrach 

drop table if exists intervista_frequenzdaten.afo_embrach_intervista_data;
create table intervista_frequenzdaten.afo_embrach_intervista_data
as
select
	t0.*
	,t1.str_type 
	,t1.dwv_alle
	,t1.dtv_alle
from 
	tmp_embrach_data_poi t0
left join
	tmp_embrach_data_str t1
on
	t0.standort_id = t1.standort_id
	or 
	t0.poi_id = t1.str_id 
where
	t0.standort_id in (
					'p_223934',
					'p_158565', 'p_3353', 'p_51126', 'p_51127', 'p_15705', 'p_63829', 'p_199950',
					'p_192237', 'p_51352', 'p_51351', 'p_168999', 'p_198912', 'p_58979', 'p_32110',
					'p_208302', 'p_93391', 's_432292', 's_365938', 's_17567', 's_17916', 'p_3355',
					'p_15702', 'p_51418', 'p_15711', 'p_191206', 's_350485', 's_26762', 'p_200119',
					'p_204698', 'p_23996', 'p_27380', 'p_226060', 
					'p_214549','p_5321','p_61179','p_193596','p_1499','p_745','p_29415','p_2520',
					'p_247903','p_27908','s_234627','p_199688','p_29416','p_15696','p_1929','p_214517',
					'p_194381','p_24489','p_214104','p_50674','p_50673','s_231936','p_200043','p_207039'			
	)
;


select 
	standort_id
	,avg_anzahl_passagen
	,avg_unique_passagen
	,avg_0_2km
	,avg_2_5km
	,avg_5_10km
	,avg_10_20km
	,avg_20_50km
	,avg_50_100km
	,avg_100km_plus
	,avg_miv
	,avg_oev
	,avg_fahrrad
	,avg_zu_fuss
	,avg_sonstige
	,avg_arbeit
	,avg_einkaufen
	,avg_freizeit
	,avg_anderes
	,avg_anz_stillstände 
	,avg_anz_unique_stillstände
	,poi_typ 
	,company
	,ort
	,str_type 
	,dwv_alle  
	,dtv_alle 
from 
	intervista_frequenzdaten.afo_embrach_intervista_data
;



--Embrach POI
-- map the table to poi_id
drop table if exists intervista_frequenzdaten.embrach_poi_data;
create table intervista_frequenzdaten.embrach_poi_data
as
select 
	t0.*
	,t1.poi_typ
	,t1.company 
	,t1.ort
	,t1.plz4
from 
	intervista_frequenzdaten.agg_intervista_frequ_poly t0
left join
	geo_afo_prod.mv_lay_poi_aktuell t1
on
	t1.poi_id = t0.poi_id
	or
	t1.poi_id = t0.str_id
where 
	standort_id in (
					'p_223934',
					'p_158565', 'p_3353', 'p_51126', 'p_51127', 'p_15705', 'p_63829', 'p_199950',
					'p_192237', 'p_51352', 'p_51351', 'p_168999', 'p_198912', 'p_58979', 'p_32110',
					'p_208302', 'p_93391', 's_432292', 's_365938', 's_17567', 's_17916', 'p_3355',
					'p_15702', 'p_51418', 'p_15711', 'p_191206', 's_350485', 's_26762', 'p_200119',
					'p_204698', 'p_23996', 'p_27380', 'p_226060', 
					'p_214549','p_5321','p_61179','p_193596','p_1499','p_745','p_29415','p_2520',
					'p_247903','p_27908','s_234627','p_199688','p_29416','p_15696','p_1929','p_214517',
					'p_194381','p_24489','p_214104','p_50674','p_50673','s_231936','p_200043','p_207039'			
	)
;



--Embrach Str_freq
drop table if exists intervista_frequenzdaten.embrach_str_freq_data_line;
create table intervista_frequenzdaten.embrach_str_freq_data_line
as
select 
	t0.standort_id
	,t0.avg_anzahl_passagen
	,t0.avg_unique_passagen
	,t0.avg_0_2km
	,t0.avg_2_5km
	,t0.avg_5_10km
	,t0.avg_10_20km
	,t0.avg_20_50km
	,t0.avg_50_100km
	,t0.avg_100km_plus
	,t0.avg_miv
	,t0.avg_oev
	,t0.avg_fahrrad
	,t0.avg_zu_fuss
	,t0.avg_sonstige
	,t0.avg_arbeit
	,t0.avg_einkaufen
	,t0.avg_freizeit
	,t0.avg_anderes
	,t0.avg_anz_stillstände 
	,t0.avg_anz_unique_stillstände
	,t1.str_type 
	,t1.dwv_alle
	,t1.dtv_alle
	,t1.geo_line_lv95
from 
	intervista_frequenzdaten.agg_intervista_frequ_poly t0
left join
	geo_afo_prod.mv_lay_str_freq_aktuell t1
on
	t1.str_id = t0.str_id
	or 
	t1.str_id = t0.poi_id
where 
	standort_id in (
					'p_223934',
					'p_158565', 'p_3353', 'p_51126', 'p_51127', 'p_15705', 'p_63829', 'p_199950',
					'p_192237', 'p_51352', 'p_51351', 'p_168999', 'p_198912', 'p_58979', 'p_32110',
					'p_208302', 'p_93391', 's_432292', 's_365938', 's_17567', 's_17916', 'p_3355',
					'p_15702', 'p_51418', 'p_15711', 'p_191206', 's_350485', 's_26762', 'p_200119',
					'p_204698', 'p_23996', 'p_27380', 'p_226060', 
					'p_214549','p_5321','p_61179','p_193596','p_1499','p_745','p_29415','p_2520',
					'p_247903','p_27908','s_234627','p_199688','p_29416','p_15696','p_1929','p_214517',
					'p_194381','p_24489','p_214104','p_50674','p_50673','s_231936','p_200043','p_207039'			
	)
;




select 
	*
from 
	geo_afo_prod.mv_lay_str_freq_aktuell;








--/////////////////////////////// OLD VERSION /////////////////////////////////////
/*
 * 
 * --===================================
-- Nur Embrach
--===================================
drop table if exists intervista_frequenzdaten.embrach_intervista_freq_daten;
create table intervista_frequenzdaten.embrach_intervista_freq_daten
as
select 
	t0.*
	,t1.poi_typ_list
	,t1.poi_name_list
	,t1.ort
from
	intervista_frequenzdaten.agg_intervista_frequ_poly t0
left join 
	intervista_frequenzdaten.intervesta_freq_mit_poi_id t1
on
	t0.standort_id = t1.standort_id
where
	t0.standort_id in (
					'p_223934',
					'p_158565',
					'p_192237',
					'p_208302',
					'p_15702',
					'p_204698',
					'p_214549',
					'p_5321',
					'p_61179',
					'p_193596',
					'p_1499',
					'p_745',
					'p_29415',
					'p_2520',
					'p_247903',
					'p_27908',
					's_234627',
					'p_199688',
					'p_29416',
					'p_15696',
					'p_1929',
					'p_214517',
					'p_194381',
					'p_24489',
					'p_214104',
					'p_50674',
					'p_50673',
					's_231936',
					'p_200043'
	)
;

select 
	standort_id
	,avg_anzahl_passagen 
	,avg_unique_passagen 
	,avg_0_2km as perce_0_2km
	,avg_2_5km as perce_2_5km
	,avg_5_10km as perce_5_10km
	,avg_10_20km as perce_10_20km
	,avg_20_50km as perce_20_50km
	,avg_50_100km as perce_50_100km
	,avg_100km_plus as perce_100km_plus
	,avg_miv as miv
	,avg_oev as oev 
	,avg_fahrrad as fahrrad
	,avg_zu_fuss as zu_fuss 
	,avg_sonstige as sonstige
	,avg_arbeit as arbeit 
	,avg_einkaufen as einkaufen
	,avg_freizeit as freizeit 
	,avg_anderes as anderes
	,avg_stillstände 
	,avg_unique_stillstände
	,ort
	,poi_typ_list as poi_typ
	,poi_name_list as poi_name
from 
	intervista_frequenzdaten.embrach_intervista_freq_daten
;
--====================================
-- adding Poi_typ
--====================================
select 
	t0.*
	--,t1.poi_id_list
	,t1.poi_typ_list
	,t1.poi_name_lise
from
	intervista_frequenzdaten.avg_percent_intervista_freq t0
left join
	intervista_frequenzdaten.intervesta_freq_mit_poi_id t1
on
	t0.standort_id = t1.standort_id 
;

--=============================
-- Poi_typ für jede standort_id
--=============================
-- Ganz Liste
create index idx_agg_intervista_frequ_poly on intervista_frequenzdaten.agg_intervista_frequ_poly using GIST(geo_poly_lv95);
create index idx_mv_lay_poi_aktuell on geo_afo_prod.mv_lay_poi_aktuell using GIST(geo_point_lv95);
--create index idx_tmp_afo_poi_zürich_geo_point_lv95 on tmp_afo_poi_zürich using GIST(geo_point_lv95);


DROP TABLE IF EXISTS intervista_frequenzdaten.intervesta_freq_mit_poi_id;
CREATE TABLE intervista_frequenzdaten.intervesta_freq_mit_poi_id 
AS
SELECT 
    t0.standort_id,
    '[' || array_to_string(array_agg(t1.poi_id), ',') || ']' AS poi_id_list,
    '[' || array_to_string(array_agg(t1.poi_typ), ',') || ']' AS poi_typ_list,
    '[' || array_to_string(array_agg(t1.bezeichnung_kurz), ',') || ']' AS poi_name_list,
    t1.ort
    --, '[' || array_to_string(array_agg(t1.geo_point_lv95), ',') || ']' -- Uncomment if you want to include geo points
FROM 
    intervista_frequenzdaten.agg_intervista_frequ_poly t0
LEFT JOIN
    geo_afo_prod.mv_lay_poi_aktuell t1
ON 
    ST_Intersects(t0.geo_poly_lv95, t1.geo_point_lv95)
GROUP BY 
    t0.standort_id,
    t1.ort;



select
	*
from
	intervista_frequenzdaten.intervesta_freq_mit_poi_id 
;
where 
	standort_id in (
		'p_15705'
		,'p_199950'
		,'p_63829'
		,'p_168999'
		,'p_193391'
		,'p_198912'
		,'p_32110'
		,'p_51351'
		,'p_51352'
		,'p_58979'
		,'s_17567'
		,'s_17916'
		,'s_365938'
		,'s_432292'
		,'p_17576'
		,'p_192635'
		,'p_199188'
		,'p_201107'
		,'p_207039'
		,'p_225583'
		,'p_50748'
		,'p_1499'
		,'p_15696'
		,'p_15702'
		,'p_158565'
		,'p_192237'
		,'p_1929'
		,'p_193596'
		,'p_194381'
		,'p_199688'
		,'p_200043'
		,'p_204698'
		,'p_208302'
		,'p_214104'
		,'p_214517'
		,'p_214549'
		,'p_223934'
		,'p_24489'
		,'p_2520'
		,'p_27908'
		,'p_29415'
		,'p_29416'
		,'p_50673'
		,'p_50674'
		,'p_5321'
		,'p_61179'
		,'p_745'
		,'s_231936'
		,'s_234627'
		,'s_247903'
		,'p_130061'
		,'p_15284'
		,'p_15285'
		,'p_198917'
		,'p_201017'
		,'p_202031'
		,'p_203190'
		,'p_224054'
		,'p_226200'
		,'p_48498'
		,'p_51870'
		,'s_16015'
		,'s_238301'
		,'s_348796'
		,'s_36826'
		,'s_377065'
		,'s_39644'
		,'p_15711'
		,'p_191206'
		,'p_200119'
		,'p_207039'
		,'p_226060'
		,'p_23996'
		,'p_3355'
		,'p_51418'
		,'s_26762'
		,'s_27380'
		,'s_350485'
		,'p_130171'
		,'p_191714'
		,'p_201832'
		,'p_206595'
		,'p_3353'
		,'p_51126'
		,'p_51127'
		,'p_15281'
		,'p_186415'
		,'p_187229'
		,'p_188169'
		,'p_196322'
		,'p_214480'
		,'p_226919'
		,'p_27445'
		,'p_3304'
		,'p_63260'
		,'p_7830'
		,'p_8324'
		,'s_14668'
		,'s_291652'
		,'s_291654'
		,'s_296405'
		,'s_298608'
		,'s_32034'
		,'s_321612'
		,'s_330578'
		,'s_370358'
		,'s_40501'
		,'s_407224'
		,'s_449770'
		,'s_5355'
		,'s_5356'
		,'s_5357'
		,'p_129859'
		,'p_131432'
		,'p_136648'
		,'p_1470'
		,'p_15249'
		,'p_15250'
		,'p_15252'
		,'p_15253'
		,'p_15266'
		,'p_15267'
		,'p_15268'
		,'p_15269'
		,'p_15271'
		,'p_158568'
		,'p_17749'
		,'p_185855'
		,'p_193133'
		,'p_194859'
		,'p_195693'
		,'p_196918'
		,'p_197334'
		,'p_197358'
		,'p_197820'
		,'p_197983'
		,'p_199250'
		,'p_199912'
		,'p_200006'
		,'p_200385'
		,'p_201492'
		,'p_201916'
		,'p_202129'
		,'p_202599'
		,'p_203337'
		,'p_203520'
		,'p_203793'
		,'p_204085'
		,'p_204270'
		,'p_205856'
		,'p_206545'
		,'p_207670'
		,'p_207788'
		,'p_214547'
		,'p_224007'
		,'p_225798'
		,'p_225908'
		,'p_226198'
		,'p_23117'
		,'p_24561'
		,'p_27713'
		,'p_29346'
		,'p_3299'
		,'p_3301'
		,'p_33914'
		,'p_34769'
		,'p_44877'
		,'p_50554'
		,'p_59130'
		,'p_59651'
		,'p_7829'
		,'p_8323'
		,'s_14668'
		,'s_226527'
		,'s_227959'
		,'s_230754'
		,'s_230808'
		,'s_231753'
		,'s_232285'
		,'s_232570'
		,'s_234780'
		,'s_239438'
		,'s_244028'
		,'s_24693'
		,'s_263860'
		,'s_278980'
		,'s_278981'
		,'s_290140'
		,'s_291654'
		,'s_293281'
		,'s_302366'
		,'s_303266'
		,'s_303894'
		,'s_319415'
		,'s_319416'
		,'s_319417'
		,'s_319418'
		,'s_321612'
		,'s_323599'
		,'s_327574'
		,'s_327577'
		,'s_338791'
		,'s_349675'
		,'s_352010'
		,'s_354273'
		,'s_362817'
		,'s_365187'
		,'s_365188'
		,'s_366150'
		,'s_370282'
		,'s_370358'
		,'s_376488'
		,'s_376490'
		,'s_376491'
		,'s_376492'
		,'s_376493'
		,'s_376494'
		,'s_376495'
		,'s_376498'
		,'s_376504'
		,'s_377374'
		,'s_377375'
		,'s_377376'
		,'s_400963'
		,'s_402430'
		,'s_404601'
		,'s_404602'
		,'s_40501'
		,'s_407224'
		,'s_412903'
		,'s_43439'
		,'s_440427'
		,'s_446449'
		,'s_50675'
		,'s_5352'
		,'s_5355'
		,'s_5356'
		,'s_5357'
	)
;





create index idx_mv_lay_gmd_aktuell on geo_afo_prod.mv_lay_gmd_aktuell using GIST(geo_poly_lv95);

select 
	t0.standort_id 
	,t1.gmd_nr 
	,t1.gemeinde 
	,t0.geo_poly_lv95 
	,t1.geo_poly_lv95
from 
	intervista_frequenzdaten.agg_intervista_frequ_poly t0
left join
	geo_afo_prod.mv_lay_gmd_aktuell t1
on 
	ST_Intersects(t0.geo_poly_lv95, t1.geo_poly_lv95) 
where
	t1.gemeinde in (
			'Embrach'
			,'Lufingen'
			,'Oberembrach'
			,'Freienstein-Teufen' 
			,'Dättlikon'
			,'Pfungen'
			,'Bachenbülach'
			,'Bülach'
			,'Rorbas'
			,'Winkel'
	)
;
	
	
	
	
	

select 
	*
from 
	geo_afo_prod.mv_lay_poi_aktuell
where 
	ort = 'Embrach'
;
select 
	*
from 
	geo_afo_prod.mv_lay_gmd_aktuell



select 
	*
from 
	intervista_frequenzdaten.embrach_intervista_freq_daten
	;


*/






