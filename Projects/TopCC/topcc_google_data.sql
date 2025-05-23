--=========================
-- Project: TOPCC
-- Date: 23.01.2025
-- update: 05.02.2025
-- Tamer Adel
--=========================

-- data in to serverless
--(I) Hotel Gastro
drop table if exists
	google_maps_dev.google_map_hotel_gastronomie_v5;

create table
	google_maps_dev.google_map_hotel_gastronomie_v5
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
				google_maps_dev.google_map_hotel_gastronomie_v5 
		$POSTGRES$
	) as google_map_hotel_gastronomie_v5 (
			cid text 
			,bezeichnung text 
			,category_en_ids text 
			,category_de_ids text 
			,poi_typ text 
			,kategorie text 
			,hauptkategorie text 
			,korr_strasse text 
			,korr_hausnum text 
			,korr_plz4 text 
			,korr_ort text 
			,telefon text 
			,adresse text 
			,google_strasse text 
			,google_strasse_std text 
			,google_hausnum text 
			,google_plz4 text 
			,google_ort text 
			,gwr_strasse varchar(60) 
			,gwr_strasse_std text 
			,gwr_hausnum text 
			,gwr_plz4 int4 
			,gwr_ort varchar(100) 
			,distance float8 
			,plz6 text 
			,plz4 text 
			,ort text 
			,gemeinde text 
			,gmd_nr numeric 
			,url text 
			,"domain" text 
			,anz_fotos int4 
			,google_bewertung text 
			,anz_bewertungen text 
			,relevant numeric 
			,status text 
			,opening_times text 
			,geo_point_google public.geometry 
			,geo_point_gwr public.geometry(point, 2056) 
			,category_ids jsonb 
			,category_ids_de jsonb
	)
;

select * from google_maps_dev.google_map_hotel_gastronomie_v5 --where lower(bezeichnung) like '%Hallenbad Muttenz%';
where 
	cid in (
			'10933191611443861530'
			,'3483112495823060930'
		)
;


--(II) Freizeit
DROP table if exists
	google_maps_dev.google_map_freizeit_v1;

create table
	google_maps_dev.google_map_freizeit_v1
as
SELECT 
	*
FROM 
	dblink(
		'geo_database',
		$POSTGRES$
			select
				* 
			from
				google_maps_dev.google_map_freizeit_v1 
		$POSTGRES$
	) AS google_map_freizeit_v1 (
			cid text 
			,bezeichnung text 
			,category_en_ids text 
			,category_de_ids text 
			,poi_typ text 
			,kategorie text 
			,hauptkategorie text 
			,korr_strasse text 
			,korr_hausnum text 
			,korr_plz4 text 
			,korr_ort text 
			,telefon text 
			,adresse text 
			,google_strasse text 
			,google_strasse_std text 
			,google_hausnum text 
			,google_plz4 text 
			,google_ort text 
			,gwr_strasse varchar(60) 
			,gwr_strasse_std text 
			,gwr_hausnum text 
			,gwr_plz4 int4 
			,gwr_ort varchar(100) 
			,distance float8 
			,plz6 text 
			,plz4 text 
			,ort text 
			,gemeinde text 
			,gmd_nr numeric 
			,url text 
			,"domain" text 
			,anz_fotos int4 
			,google_bewertung text 
			,anz_bewertungen text 
			,relevant numeric 
			,status text 
			,opening_times text 
			,geo_point_google public.geometry 
			,geo_point_gwr public.geometry(point, 2056) 
			,category_ids jsonb 
			,category_ids_de jsonb
	)
;

select * from google_maps_dev.google_map_freizeit_v1
where 
	cid in (
			'1092625333666337012'
			,'10933191611443861530'
			,'3483112495823060930'
			,'15446699419309246361'
		)
--where lower(bezeichnung) like '%hallenbad muttenz%';

--(III) plz for swiss german speaking

DROP table if exists
	geo_afo.plz6_po_tot;

create table
	geo_afo.plz6_po_tot
as
SELECT 
	*
FROM 
	dblink(
		'geo_database',
		$POSTGRES$
			select
				* 
			from
				geo_afo.plz6_po_tot 
		$POSTGRES$
	) AS plz6_po_tot (
			plz6 int4 
			,plz int4 
			,plzzus int4 
			,plz_grob int4 
			,sprache int4 
			,sprache_abw int4 
			,kanton text 
			,ort text 
			,tarif text 
			,promopost int4 
			,ohne_einwohner int4 
			,kommerziell_std int4 
			,kommerziell_std_plus int4 
			,offiziell_std int4 
			,offiziell_std_plus int4 
			,kommerziell_samstag int4 
			,offiziell_samstag int4 
			,kommerziell_efh int4 
			,offiziell_efh int4 
			,kommerziell_lws int4 
			,offiziell_lws int4 
			,kommerziell_pch int4 
			,offiziell_pch int4 
			,kommerziell_ges_pch int4 
			,offiziell_ges_pch int4 
			,kommerziell_domizil int4 
			,wemf_region_nr int4 
			,wemf_region text 
			,wemf_gebiet_nr int4 
			,wemf_gebiet text 
			,wemf_agglo_nr int4 
			,wemf_agglo text 
			,gmd_nr int4 
			,gemeinde varchar(255) 
			,amtsbezirk_nr int4 
			,amtsbezirk text 
			,bezirk_nr int4 
			,bezirk varchar(255)	
	)
;

select * from geo_afo.plz6_po_tot;


--(III) TopCC plz 
--public.topcc_next_plz4
DROP table if exists
	top_cc.topcc_next_plz4;

create table
	top_cc.topcc_next_plz4
as
SELECT 
	*
FROM 
	dblink(
		'geo_database',
		$POSTGRES$
			select
				* 
			from
				public.topcc_next_plz4
		$POSTGRES$
	) AS topcc_next_plz4 (
				poi_id numeric 
				,bezeichnung_lang text 
				,plz4 numeric 
				,fz numeric
	)
;

select * from top_cc.topcc_next_plz4;



--=========================================================
-- Filtering the Data
--=========================================================
---------------------------------------------
-- Temporary table for Takeaway/Delivery data
---------------------------------------------
--3139
drop table if exists tmp_takeaway_data;
create table
		top_cc.tmp_takeaway_data
as 
select 
	*
from 
    google_maps_dev.google_map_hotel_gastronomie_v5
where 
    (
        -- Filter by categories "Take-Away" or "Delivery" (English or German)
    	category_ids ? 		'delivery'
    	or 
    	category_ids ? 		'takeaway'
    	or 
    	category_ids ? 		'meal_delivery'
    	or 
    	category_ids ?		'delivery_service'
    	or 
    	category_ids ?		'outdoor_bath'
    	or 
    	category_ids ?		'pizzatakeaway'
    	or 
    	category_ids ? 		'kebab_shop'
    	or 
    	category_ids ?		'snack_bar'
    	or 
    	--category_ids ?	'fast_food_restaurant'
    	--or 
    	category_ids_de ?	'Take Away'
    	or 
    	category_ids_de ?	'Pizza-Lieferdienst'
    	or 
    	category_ids_de ?	'Imbiss'
    	or 
    	--category_ids_de ?	'Pizzeria'
    	--or 
    	category_ids_de ?	'Lieferdienst'
    	or 
    	category_ids_de ?	'Zustellservice'
    )
    or 
    (
        bezeichnung ilike '%take away%' 
        or 
        bezeichnung ilike '%takeaway%' 
        or 
        bezeichnung ilike '%imbiss%' 
        or 
        bezeichnung ilike '%delivery%' 
        or 
        bezeichnung ilike '%lieferdienst%'
    )
   	and 
    not (category_ids ? 'hotel')
    and 
    lower(bezeichnung)  not like '%hotel%'
;   
 
select 
	* 
from 
	top_cc.tmp_takeaway_data
where 
	plz4 = '6331'


	cid not in(
				select 
					cid
				from
					tmp_takeaway_data
	) 
;


--------------------------
-- filiter nach topcc plz4
--------------------------
drop table if exists top_cc.takeaway_data_topcc;

create table
	top_cc.takeaway_data_topcc
as
select 
	t1.*,
	t2.poi_id as topcc_poi_id,
	t2.bezeichnung_lang as topcc_bezeichnung
from 
	top_cc.tmp_takeaway_data t1 --5910
join
	top_cc.topcc_next_plz4 t2
on 
	t1.plz4 = t2.plz4::text
	or 
	t1.korr_plz4 = t2.plz4::text
order by 
	topcc_bezeichnung
;
















---------------------------------------------
-- Temporary table for all gastro data
---------------------------------------------
-- all gastro excluded Hotels and bakery without takeaway or restaurant
drop table if exists tmp_gastro_data;
create table
		top_cc.tmp_gastro_data
as 
select 
	*
from 
    google_maps_dev.google_map_hotel_gastronomie_v5
where  
    not (category_ids ? 'hotel')
    and 
    lower(bezeichnung)  not like '%hotel%'
    and
    not ( 
    	category_ids @> '["bakery"]'
		and not
			(category_ids ? 'meal_takeaway' 
			OR 
			category_ids ? 'Restaurant')
		)
;

        
        
--=========================================
-- Temporary table for Swimming Pools data
--=========================================
--1194
drop table if exists top_cc.tmp_schwimmbaeder_data;
create table
	top_cc.tmp_schwimmbaeder_data
as 
select 
	*
from 
    google_maps_dev.google_map_freizeit_v1
where 
    -- Include only public indoor and outdoor swimming pools   
    (
    	category_ids ? 		'public_swimming_pool'
    	or 
    	category_ids ? 		'swimming_pool'
    	or 
    	category_ids ? 		'indoor_swimming_pool'
    	or 
    	category_ids ?		'outdoor_swimming_pool'
    	or 
    	category_ids ?		'outdoor_bath'
    	or 
    	category_ids ?		'swimming_basin'
    	or 
    	category_ids ? 		'public_bath'
    	or 
    	category_ids ?		'swimming_facility'
    	or 
    	category_ids ?		'lake_shore_swimming_area'
    	or 
    	category_ids_de ?	'Schwimmbad'
    	or 
    	category_ids_de ?	'Hallenbad'
    	or 
    	category_ids_de ?	'Hallenschwimmbad'
    	or 
    	category_ids_de ?	'Freibad'
    	or 
    	category_ids_de ?	'Öffentliches Bad'
    	or 
    	category_ids_de ?	'Öffentliches Schwimmbad'
    )
    and 
    not(
    	category_ids ? 		'hotel'
    	or 
    	category_ids ? 		'fitness_center'
    	or
    	category_ids ? 		'gym'
    	or
    	category_ids ? 		'swimming_pool_contractor'
    	or 
    	category_ids ? 		'swimming_pool_supply_store'
    	or 
    	category_ids ? 		'bathroom_remodeler'
    	or 
    	category_ids ?		'heating_equipment_supplier'
    	or 
    	category_ids ?		'landscaper'
    	or 
    	category_ids ?		'hot_tub_store'
    	or 
    	category_ids ?		'hot_tub_repair_service'
    	or
    	category_ids_de ?	'Spenglerei'
    	or 
    	category_ids_de ?	'Dachdecker'
    	or 
    	category_ids_de ?	'Dachrinnen-Reinigungsservice'
    	or 
    	category_ids_de ?	'Schwimmbeckenbauer'
    	or 
    	category_ids_de ?	'Schwimmbecken-Reparaturdienst'
    	or 
    	category_ids_de ?	'Gartenbauer'
    	or
    	category_ids_de ?	'Whirlpool-Fachhandel'
    	or 
    	category_ids_de ?   'Whirlpool-Reparaturdienst'
    	or 
    	category_ids_de ?	'Bademodengeschäft'
    	or 
    	category_ids_de ?	'Boutique'
    	or 
    	category_ids_de ?	'Ärztehaus'
    	or 
    	category_ids_de ?	'Physiotherapeut'
    	or 
    	category_ids_de ?	'Heizungsmonteur'
    	or 
    	category_ids_de ?	'Wohnanlage'
    	or 
    	category_ids_de ?	'Bauunternehmen'
    	or 
    	category_ids_de ?	'Erdbauunternehmen'
    	or 
    	category_ids_de ?	'Resort-Hotel'
    )
    and 
    lower(bezeichnung)  not like '%hotel%'
    and
    lower(bezeichnung)  not like '%hôtel%'
   	and 
   	lower(bezeichnung)  not like '%inn%'
   	and 
   	lower(bezeichnung)  not like '%immobilier%'
   	and 
   	lower(bezeichnung)  not like '%gasthaus%'
   	and 
   	bezeichnung  not like 'bawatec GmbH'
;


select 
	* 
from 
	tmp_swimming_pools_data
where 
	cid not in (
				select 
					cid 
				from
					tmp_swimming_pools_data_v1
	)
;


select 
	* 
from 
	tmp_swimming_pools_data
where 
	cid in (
			-- 9800955667796865244   ["Boutique", "Fachgeschäft für Damenmode", "Wollgeschäft", "Strandbad", "Schuhgeschäft", "Geschäft", "Bademodengeschäft"]
			'13774577297978065205'  -- [Freibad | Lounge | Restaurant] Rive Droite by Michel Roth - Hôtel Président
			,'7119097213020877575'	-- [Schwimmbad] >> schrott
			,'4660558655738828808'	-- [lake_shore_swimming_area] >> schrott
			-- 2622033144915715710	["Ärztehaus", "Hallenschwimmbad", "Fitnessprogramm", "Physiotherapeut"]
			,'791255195096572101'	-- [Bar | Fitnessraum | Hallenschwimmbad | Öffentliches Schwimmbad | Sauna | Schwimmanlage | Schwimmbad | Wellness-Programm] Radisson Blu Pool Inn Club, Basel
			-- 11611254044755825099 ["Generalunternehmer", "Schrankmöbelhaus", "Heizungsmonteur", "Maler", "Schwimmbad"]
			-- 17162567666481476548 ["Ferienwohnung", "Wandern", "Resort-Hotel", "Skigebiet", "Schwimmbad"]
			,'17991469507500281735'	-- [Ärztehaus | Häuslicher Pflegedienst | Masseur | Physiotherapeut | Schwimmanlage]
			-- 222686972893069040 	["Wohnanlage", "Schwimmbad"]
			-- 13420239617127192667 ["Bauunternehmen", "Erdbauunternehmen", "Maurer", "Freibad"]
			-- 11065971642905700153 ["Fitnesszentrum", "Fitnessraum", "Schwimmanlage", "Wellnesszentrum"]
			-- 17991469507500281735 ["Physiotherapeut", "Häuslicher Pflegedienst", "Masseur", "Ärztehaus", "Schwimmanlage"]
			-- 9800955667796865244 	["Boutique", "Fachgeschäft für Damenmode", "Wollgeschäft", "Strandbad", "Schuhgeschäft", "Geschäft", "Bademodengeschäft"]
			-- 4413716706204402752 	["Resort-Hotel", "Abenteuersportarten", "Velogeschäft", "Spa", "Hallenschwimmbad", "Unterkunft", "Pizzeria", "Restaurant", "Sportgeschäft"]
			-- 2031362134223369124 	["Wellnesszentrum", "Gesundheitsberater", "Spa", "Freibad", "Sauna", "Sauna Club", "Spa und Fitness-Studio", "Schwimmbad", "Wellness-Programm"]
	)
	
	
	
	
	
--cid = 6377997520870256904
--cid = 12195884727890602184
--cid = 14253702430913395729
--cid = 4075601913756857258
--cid = 7996207657089712252
--cid = 13371149858468801625
--cid = 8814434736804456973
--cid = 153004613097927135
--cid = 8283174203830271475
--cid = 15647941707375924787

--neu
-- 9800955667796865244   ["Boutique", "Fachgeschäft für Damenmode", "Wollgeschäft", "Strandbad", "Schuhgeschäft", "Geschäft", "Bademodengeschäft"]
-- 13774577297978065205
-- 7119097213020877575
-- 4660558655738828808	
-- 2622033144915715710	["Ärztehaus", "Hallenschwimmbad", "Fitnessprogramm", "Physiotherapeut"]
-- 791255195096572101	
-- 11611254044755825099 ["Generalunternehmer", "Schrankmöbelhaus", "Heizungsmonteur", "Maler", "Schwimmbad"]
-- 17162567666481476548 ["Ferienwohnung", "Wandern", "Resort-Hotel", "Skigebiet", "Schwimmbad"]
-- 17991469507500281735	
-- 222686972893069040 ["Wohnanlage", "Schwimmbad"]
-- 13420239617127192667 ["Bauunternehmen", "Erdbauunternehmen", "Maurer", "Freibad"]
-- 11065971642905700153 ["Fitnesszentrum", "Fitnessraum", "Schwimmanlage", "Wellnesszentrum"]
-- 17991469507500281735 ["Physiotherapeut", "Häuslicher Pflegedienst", "Masseur", "Ärztehaus", "Schwimmanlage"]
-- 9800955667796865244 ["Boutique", "Fachgeschäft für Damenmode", "Wollgeschäft", "Strandbad", "Schuhgeschäft", "Geschäft", "Bademodengeschäft"]
-- 4413716706204402752 ["Resort-Hotel", "Abenteuersportarten", "Velogeschäft", "Spa", "Hallenschwimmbad", "Unterkunft", "Pizzeria", "Restaurant", "Sportgeschäft"]
-- 2031362134223369124 ["Wellnesszentrum", "Gesundheitsberater", "Spa", "Freibad", "Sauna", "Sauna Club", "Spa und Fitness-Studio", "Schwimmbad", "Wellness-Programm"]
-- 	
	


--indoor_swimming_pool	    		Hallenschwimmbad
--lake_shore_swimming_area			Strandbad
--outdoor_bath						Freibad
--outdoor_swimming_pool				Freibad
--public_bath						Öffentliches Bad
--public_swimming_pool				Öffentliches Schwimmbad
--swimming_basin					Schwimmbecken
--swimming_facility					Schwimmanlage
--swimming_pool						Schwimmbad   


-----------------------------------------
-- Schwimmbäder mit Restaurants und Bars
-----------------------------------------
drop table if exists tmp_schwimmbäder_mit_gastro;
create temp table tmp_schwimmbäder_mit_gastro
as
select 
	*
from 
	tmp_swimming_pools_data
where 
	category_ids ? 'restaurant'
	or 
	category_ids ? 'bar'
	or 
	category_ids ? 'cafe'
	or 
	category_ids ? 'imbiss'
	or 
	category_ids ? 'snack_bar'
	or 
	category_ids_de ? 'Imbiss'
;



---------------------------------------------
-- Temporary table for German Speaking CH
---------------------------------------------
drop table if exists tmp_de_ch;
create temp table tmp_de_ch
as
select 
	plz6
	,plz
	,sprache
	,kanton
	,ort
	,gemeinde
	,gmd_nr
from
	geo_afo.plz6_po_tot
where 
	sprache = 1
;

---------------------------------------------
-- Combine and filter results
---------------------------------------------
drop table if exists top_cc.grundselektion_ch;
create table top_cc.grundselektion_ch
as
select distinct
	cid 
	,array_agg(hauptkategorie) 		as hauptkategorie
	,array_agg(poi_typ) 			as poi_typs
	,bezeichnung  
	,korr_strasse 					as strasse  
	,korr_hausnum 					as hausnum
	,korr_plz4 						as plz4
	,korr_ort 						as ort
	,telefon 
	,adresse 
	,url 
	,"domain"
	,geo_point_google 				as geo_point_lv95 
	,array_agg(category_ids) 		as category_ids
from (
    select  
		*
   	from
   		tmp_takeaway_data_v1
    union all 
    select
		*
    from
    	top_cc.tmp_schwimmbaeder_data
) as combined_data
group by 
	cid 
	,bezeichnung  
	,korr_strasse 					
	,korr_hausnum 					
	,korr_plz4 						
	,korr_ort 						
	,telefon 
	,adresse 
	,url 
	,"domain"
	,geo_point_google 				 
;



-- 20 Schwimmbad und gastro
select 
	* 
from 
	top_cc.grundselektion_ch
where 
	hauptkategorie in ('{Hotel & Gastronomie,Freizeit}')
	or 
	hauptkategorie in ('{Freizeit,Hotel & Gastronomie}')
;

select cid, count(*) from top_cc.grundselektion_ch group by cid having count(*)>1;


--cid = '13645013592175997266'

--==========================
-- Match mit plz4 TopCC pois
--==========================
drop table if exists top_cc.grundselektion_topcc;
create table top_cc.grundselektion_topcc
as
select 
	t1.*,
	t2.poi_id as topcc_poi_id,
	t2.bezeichnung_lang as topcc_bezeichnung
	--t2.plz4 as topcc_plz4,
	--t2.fz
from 
	top_cc.grundselektion_ch t1
join
	top_cc.topcc_next_plz4 t2
on 
	t1.plz4 = t2.plz4::text
order by 
	topcc_bezeichnung
;


select * from top_cc.grundselektion_topcc;


select * from top_cc.topcc_next_plz4;





--==============================
-- 150m buffer
--==============================
-- Option 1
-- 296 davon 37 Schwimmbäder mit gastronomie
drop table if exists top_cc.gastro_um_schwimmbaeder_ch;
create table top_cc.gastro_um_schwimmbaeder_ch
as
select 
    s.cid              			as sb_cid,
    s.bezeichnung             	as sb_name,
    s.adresse					as sb_adresse,
    s.korr_plz4 				as sb_plz4,
    s.category_ids_de			as sb_category,
    s.url 						as sb_url,
    t.cid               		as gs_cid,
    t.bezeichnung             	as gs_name,
    t.adresse					as gs_adresse,
    t.korr_plz4 				as gs_plz4,
    t.category_ids_de			as gs_category,
    ST_Distance(
        s.geo_point_google,
        t.geo_point_google
    ) as distance_meters,
    case  
      	when
      		ST_Distance(s.geo_point_google,t.geo_point_google) = 0
      		or (
	      		s.category_ids ? 		'restaurant'
				or 
				s.category_ids ?		'takeaway'
				or
				s.category_ids ? 		'bar'
				or 
				s.category_ids ? 		'cafe'
				or 
				s.category_ids ? 		'imbiss'
				or 	
				s.category_ids ? 		'snack_bar'
				or 
				s.category_ids_de ? 	'Imbiss'
				or 
				s.category_ids_de ? 	'Take Away'
				or 
				s.category_ids_de ? 	'Pizzeria'
			)
			and 
			poi_ty like '%Schwimmbad%' 
		then 
			true  
      	else
      		false  
    end as has_gastro,
    s.geo_point_google as geo_point_schwim,
    t.geo_point_google as geo_point_gastro
from  
    top_cc.tmp_schwimmbaeder_data as s
join  
    tmp_takeaway_data_v1 as t
on
  	ST_DWithin(
       s.geo_point_google,
       t.geo_point_google,
       150
     )
order by 
	has_gastro desc,
	distance_meters
;


-- ganz schweiz 239
select * from top_cc.gastro_um_schwimmbaeder_ch;


--------------------------
-- filiter nach topcc plz4
--------------------------
create table top_cc.gastro_um_schwimmbaeder_topcc
as
select 
	t1.*,
	t2.poi_id 				as topcc_poi_id,
	t2.bezeichnung_lang 	as topcc_bezeichnung
	--t2.plz4 				as topcc_plz4,
	--t2.fz
from
	top_cc.gastro_um_schwimmbaeder_ch t1
join
	top_cc.topcc_next_plz4 t2
on 
	t1.sb_plz4 = t2.plz4::text
order by 
	topcc_bezeichnung
	--fz
;


--===================================
--V2: version nach dem Meeting mit Peter
--===================================
--takeaway/Imbiss
select * from top_cc.tmp_takeaway_data;
--schwimmbäder 
select * from top_cc.tmp_swimming_pools_data;
--all gastro without hotels or bakery
select * from top_cc.tmp_gastro_data;


--filiter out schwimmbäder that not exists in toppcc areas 
drop table if exists top_cc.schwimmbaeder_topcc;

create table
	top_cc.schwimmbaeder_topcc
as
select 
	t1.*,
	t2.poi_id as topcc_poi_id,
	t2.bezeichnung_lang as topcc_bezeichnung
from 
	top_cc.tmp_swimming_pools_data t1
join
	top_cc.topcc_next_plz4 t2
on 
	t1.plz4 = t2.plz4::text
	or 
	t1.korr_plz4 = t2.plz4::text
order by 
	topcc_bezeichnung
;


/*
 * 	t1.cid,
	t1.bezeichnung,
	t1.poi_typ,
	t1.adresse,
	t1.korr_plz4,
	t1.plz4,
	t1.ort,
	t1.url,
	t1.domain,
	t1.geo_point_google,
	t1.category_ids,
	t1.category_ids_de,
 * 
 */


----------------------------------------------
-- takaway/imbiss um 150-m Schwimmbäder topcc 
----------------------------------------------
drop table if exists top_cc.gastro_um_schwimmbaeder;
create table top_cc.gastro_um_schwimmbaeder
as
select 
    s.cid              			as sb_id,
    t.cid               		as gs_id,
    s.hauptkategorie			as sp_hauptkategorie,
    t.hauptkategorie			as gs_hauptkategorie,
    s.bezeichnung             	as sb_name,
    t.bezeichnung             	as gs_name,
    s.poi_typ 					as sb_poi_typ,
    t.poi_typ 					as gs_poi_typ,
    s.adresse					as sb_adresse,
    t.adresse					as gs_adresse,
    s.telefon					as sb_telefon,
    t.telefon 					as gs_telefon,
    ST_Distance(
        s.geo_point_google,
        t.geo_point_google
    ) as distance_meters,
    case  
      	when
      		ST_Distance(s.geo_point_google,t.geo_point_google) is not null then true 
      	else
      		false  
    end as has_gastro,
    case 
    	when
    		ST_Distance(s.geo_point_google, t.geo_point_google) = 0 then 		'Schwimmbad mit Gastro'
    	when
    		ST_Distance(s.geo_point_google, t.geo_point_google) > 0 then 		'Gastro in der Nähe Schwimmbad'
    	when
    		ST_Distance(s.geo_point_google, t.geo_point_google) is null then 	'Schwimmbad'
    else 
    	null 
	end as branchenfeld,
    s.plz4 						as sb_plz4,
    t.korr_plz4 				as gs_plz4,
    s.ort						as sb_ort,
    t.ort						as gs_ort,
    s.category_ids				as sb_category,
    t.category_ids				as gs_category,
    s.url 						as sb_url,
    t.url						as gs_url,
    s.domain					as sb_domain,
    t.domain					as gs_domain,
    s.geo_point_google 			as geo_point_schwim,
    t.geo_point_google 			as geo_point_gastro,
    s.topcc_bezeichnung,
    s.topcc_poi_id
from  
    top_cc.schwimmbaeder_topcc as s
left join  
    top_cc.tmp_takeaway_data as t
on
  	ST_DWithin(
       s.geo_point_google,
       t.geo_point_google,
       150
     )
     and(
     	s.plz4 = t.plz4
     	or 
     	s.korr_plz4 = t.plz4
     	or
     	s.plz4 = t.korr_plz4
     	or 
     	s.korr_plz4 = t.korr_plz4
     ) 
order by 
	has_gastro desc,
	distance_meters
;
    
select * from top_cc.gastro_um_schwimmbaeder where sb_id = gs_id; --sb_id <> gs_id or gs_id is null;
--=========================
-- Finale Table
--=========================
-- Vertical structure  
drop table if exists top_cc.schwimmbaeder_gastro_topcc;
create table 
	top_cc.schwimmbaeder_gastro_topcc
as
select         
	id,
    name, 
    STRING_AGG(poi_typ, ',') 	as poi_typ,  
    adresse,
    '''' || telefon 			as telefon,
    sb_id,
    MIN(distance_meters) 		as distance_meters,
    BOOL_OR(has_gastro) 		as has_gastro,  
    branchenfeld,
    plz4,
    ort,
    category,
    url,
    domain,
    topcc_bezeichnung,
    topcc_poi_id,
    geo_point
from(
    -- Step 1: Select swimming pool itself (sb_id entry)
    select
        sb_id,         
        sb_id 					as id,   -- Keep the swimming pool itself
        sb_name 				as name,  
        sb_poi_typ 				as poi_typ,
        sb_adresse 				as adresse,
        sb_plz4 				as plz4,
        sb_ort 					as ort,
        sb_telefon				as telefon,
        sb_category 			as category,
        sb_url 					as url,
        sb_domain 				as domain,
        case 
	        when 
	        	distance_meters = 0 
	        then 
	        	distance_meters 
	        else 
	        	null 
	    end 					as distance_meters,  
        has_gastro,     
        branchenfeld,
        geo_point_schwim 		as geo_point,
        topcc_bezeichnung,
    	topcc_poi_id
    from  
        top_cc.gastro_um_schwimmbaeder --top_cc.gastro_um_schwimmbaeder 
    union all  
    -- Step 2: Select nearby gastro places within 150m (gs_id)
    select
        sb_id,         
        gs_id 					as id,   -- Store gastro place ID
        gs_name 				as name,  
        gs_poi_typ 				as poi_typ,
        gs_adresse 				as adresse,
        gs_plz4 				as plz4,
        gs_ort 					as ort,
        gs_telefon				as telefon,
        gs_category 			as category,
        gs_url 					as url,
        gs_domain 				as domain,
        distance_meters,   
        null                    as has_gastro,     
        branchenfeld,
        geo_point_gastro 		as geo_point,
        topcc_bezeichnung,
    	topcc_poi_id
    from
        top_cc.gastro_um_schwimmbaeder --top_cc.gastro_um_schwimmbaeder 
   	where
    	sb_id <> gs_id --or gs_id is null  -- Exclude cases where sb_id = gs_id
) s -- Step 3: Aggregate duplicates and merge poi_typ lists
group by 
    sb_id, 
    id, 
    name, 
    adresse, 
    plz4, 
    ort, 
    telefon,
    category, 
    url, 
    domain, 
    geo_point, 
    --poi_typ,
    topcc_bezeichnung,
    topcc_poi_id,
    branchenfeld
order by  
    sb_id, 
   	distance_meters
 ;



select id, count(*) from top_cc.schwimmbaeder_gastro_topcc group by id having count(*) > 1;


---------------------------------------
-- unique CID : get rid of dublication 
---------------------------------------
drop table if exists top_cc.schwimmbaeder_gastro_topcc_v2;
create table 
	top_cc.schwimmbaeder_gastro_topcc_v2
as
with grouped_data as (
  select 
  	id,  
    name,
    poi_typ,
    adresse,
    plz4,
    ort,
    telefon,
	sb_id, 
	distance_meters,
	has_gastro,  -- If any has_gastro is TRUE, keep true
    branchenfeld,
    category,
    url,
    domain,
    topcc_bezeichnung,
    topcc_poi_id,
    geo_point, 
    ROW_NUMBER() OVER (PARTITION BY id, name, adresse, plz4, ort ORDER BY sb_id, distance_meters) AS rn
  from 
    top_cc.schwimmbaeder_gastro_topcc
  group by 
    sb_id,
    id,
    name,
    adresse,
    telefon,
    plz4,
    ort,
    distance_meters,
    has_gastro,  
    branchenfeld,
    plz4,
    ort,
    category,
    url,
    domain,
    topcc_bezeichnung,
    topcc_poi_id,
    poi_typ,
    geo_point
)
select
	*
from  
	grouped_data
where  
	rn = 1;



/*
 
 select 
	*
from
	top_cc.schwimmbaeder_gastro_topcc
where 
 id in (
		 '''801541267872302207'
		,'''1092625333666337012'
		,'''14743855619359558032'
		,'''7303893014044456942'
		,'''2108790368148172847'
		,'''11343680020006846870'
		,'''8206310730370575849'
 ) 
  
  
  
select 
	id,
	count(*)
from 
	top_cc.schwimmbaeder_gastro_topcc_v1
group by
	id 
having 
	count(*) > 1;

-- add the rest of Takaway to swimming-pool table 
alter table top_cc.takeaway_data_topcc 
add column branchenfeld text;


update top_cc.takeaway_data_topcc
set 
	branchenfeld = 'Take-Away/Imbiss'
	where 
		cid not in (
					select 
						id
					from
						top_cc.schwimmbaeder_gastro_topcc_v1
		)
;

select 
 	cid,
    count(*)
from 
	top_cc.takeaway_data_topcc 
where 
	cid not in (
				select 
					id
				from
					top_cc.schwimmbaeder_gastro_topcc_v1
	)
group by 
	cid 
having 
	count(*) > 1
;

select * from top_cc.takeaway_data_topcc where cid in  ('252976825284367155', '10856534232208765966');


*/

-------------------------
-- create finale table 
-------------------------
drop table if exists top_cc.schwimmbaeder_gastro_all_topcc_v1;
create table top_cc.schwimmbaeder_gastro_all_topcc_v1
as
select 
	'''' || id 				as poi_id,  
    name 					as poi_name, 
  	poi_typ,
    adresse,
    plz4,
    ort,
    telefon,
	'''' || sb_id 			as schwimmbad_id, 
	distance_meters::text,
	has_gastro::text,  
    branchenfeld,
    category,
    url,
    domain,
    topcc_bezeichnung,
    topcc_poi_id,
    geo_point
from 
	top_cc.schwimmbaeder_gastro_topcc_v2
union all
select distinct 
	'''' || cid 			as poi_id,  
    bezeichnung  			as poi_name, 
  	poi_typ,
    adresse,
    plz4,
    ort,
    '''' || telefon 		as telefon,
	null 					as schwimmbad_id, 
	null 					as distance_meters,
	null 					as has_gastro,  
    branchenfeld,
    category_ids as category,
    url,
    domain,
    topcc_bezeichnung,
    topcc_poi_id,
    geo_point_google as geo_point
from
	top_cc.takeaway_data_topcc 
where  
	branchenfeld = 'Take-Away/Imbiss'
;
	
	

select 
	poi_id, 
	branchenfeld, 
	count(*) 
from 
	top_cc.schwimmbaeder_gastro_all_topcc_v1 
group by 
	branchenfeld, poi_id 
having 
	count(*) > 1;	
	
	
select
	*
from 
	top_cc.schwimmbaeder_gastro_all_topcc_v1
where 
 poi_id in (
			'''11343680020006846870',
			'''8206310730370575849',
			'''7303893014044456942',
			'''11842208464658945666',
			'''12725086611292852933',
			'''12725086611292852933',
			'''16073492637687707695',
			'''16073492637687707695',
			'''2878158419780901321'
 )
; 	

select 
	*
from 
	top_cc.schwimmbaeder_gastro_all_topcc_v1;






update top_cc.schwimmbaeder_gastro_all_topcc_v1
set
	poi_typ = SUBSTRING(poi_typ, 1, POSITION(',' IN poi_typ) - 1)
where
	poi_typ LIKE '%,%'
;
	
	
/*	
poi_id >> Alle Gastro- und Schwimmbad-CIDs; sie sind eindeutig.
schwimmbad_id >> Alle Schwimmbad-CIDs, die dupliziert sind gemäss der Anzahl der Gastro-Betriebe in der Nähe (max. 150 m).
distance_meter >> Die Distanz zwischen dem Schwimmbad und dem nächstgelegenen Gastro-Betrieb.
has_gastro >> "True", wenn das Schwimmbad entweder selbst eine Gastro-Einrichtung hat oder sich ein Gastro-Betrieb im Umkreis von 150 m befindet, sonst "False".
branchenfeld >> Beschreibt die poi_id und gibt an, ob sie zu einer der folgenden vier Kategorien gehört: Schwimmbad, Schwimmbad mit Gastro, Gastro in der Nähe eines Schwimmbads oder Imbiss/Take-Away.
category >> Nur für Peter zur Überprüfung.
*/	
	
	
	
	
	
-- case tum testen schwimmbad_id = '''7492408083168432948'	
	
-- table for Simon	
create table top_cc.schwimmbaeder_gastro_all_topcc_sw
as
select 
	poi_id 
	,poi_name 
	,poi_typ 
	,adresse 
	,plz4 
	,ort 
	,telefon 
	,schwimmbad_id 
	,distance_meters 
	,has_gastro 
	,branchenfeld 
	,url 
	,"domain" 
	,topcc_bezeichnung 
	,geo_point 
from 
	top_cc.schwimmbaeder_gastro_all_topcc_v2
where
	has_gastro = 'false' 
;


select * from top_cc.schwimmbaeder_gastro_all_topcc_sw 
where
	has_gastro = 'Ja';


select adresse, strasse, haus_nr from top_cc.schwimmbaeder_gastro_all_topcc_sw;






/*
select REPLACE(poi_id, '''', '') as cleaned_cid
from top_cc.schwimmbaeder_gastro_all_topcc_sw;


select 
	t1.adresse
	,t2.korr_strasse as strasse
	,t2.korr_hausnum as haus_nr
from 
	top_cc.schwimmbaeder_gastro_all_topcc_sw t1
join 
	top_cc.tmp_takeaway_data t2
on
	REPLACE(t1.poi_id, '''', '')  = t2.cid
;


alter table top_cc.schwimmbaeder_gastro_all_topcc_sw
add column 
	strasse text,
add column 
	haus_nr text
;

update top_cc.schwimmbaeder_gastro_all_topcc_sw t1
set 
	strasse = t2.korr_strasse,
	haus_nr = t2.korr_hausnum
from 
	top_cc.tmp_takeaway_data t2
where 
	REPLACE(t1.poi_id, '''', '') = t2.cid
;


select 
	adresse
from 
	top_cc.schwimmbaeder_gastro_all_topcc_sw
where 
	REPLACE(poi_id, '''', '') in (
									select 
										cid 
									from
										top_cc.tmp_schwimmbaeder_data 
	)
 ;

*/



/*
update top_cc.schwimmbaeder_gastro_all_topcc_sw
set
	has_gastro = 'Ja'
where
	has_gastro = 'true' ;

update top_cc.schwimmbaeder_gastro_all_topcc_sw
set
	has_gastro = 'Nein'
where
	has_gastro = 'false' ;



alter table top_cc.gastro_um_schwimmbaeder 
alter column 
	has_gastro type TEXT 
using 
	(case 
		when 
			has_gastro then 'Ja' 
		else 
			'Nein' 
		end
	)
;

*/
/*
delete from top_cc.schwimmbaeder_gastro_all_topcc_v1 
where
	(telefon is null or telefon = '')
    and 
    (url is null or url = '')
;
*/


/*
create table top_cc.schwimmbaeder_gastro_all_topcc_v1_copy
as
select 
	*
from 
	top_cc.schwimmbaeder_gastro_all_topcc_v1
;
*/
/*
update top_cc.schwimmbaeder_gastro_all_topcc_v1
set 
	ort = 'Zug'
where 
	plz4 = '6300'
;	
*/	
	
	
	
	
	
	
	











--///////////////////////////////////////////////////////////////////////////////

--2523
create table top_cc.gastro_um_schwimmbaeder_v1
as
WITH matched_gastro AS (
    -- Get all Gastro places within 150m of a swimming pool
    SELECT 
        s.cid AS sb_id, 
        t.cid AS gs_id,
        s.geo_point_google AS geo_point_schwim,
        t.geo_point_google AS geo_point_gastro,
        ST_Distance(s.geo_point_google, t.geo_point_google) AS distance_meters
    FROM 
        top_cc.schwimmbaeder_topcc AS s
    LEFT JOIN LATERAL (
        SELECT * 
        FROM top_cc.tmp_takeaway_data AS t
        WHERE ST_DWithin(s.geo_point_google, t.geo_point_google, 150)
        AND (
            s.plz4 = t.plz4
            OR s.korr_plz4 = t.plz4
            OR s.plz4 = t.korr_plz4
            OR s.korr_plz4 = t.korr_plz4
        )
    ) AS t ON TRUE
)
SELECT 
    COALESCE(s.cid, g.gs_id) AS id,  -- Use the available ID
    s.cid AS sb_id,  
    g.gs_id AS gs_id,  
    s.hauptkategorie AS sp_hauptkategorie,
    t.hauptkategorie AS gs_hauptkategorie,
    s.bezeichnung AS sb_name,
    t.bezeichnung AS gs_name,
    s.poi_typ AS sb_poi_typ,
    t.poi_typ AS gs_poi_typ,
    s.adresse AS sb_adresse,
    t.adresse AS gs_adresse,
    s.telefon AS sb_telefon,
    t.telefon AS gs_telefon,
    g.distance_meters,
    -- Corrected has_gastro logic: TRUE if at least one gastro place exists
    CASE 
        WHEN g.gs_id IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS has_gastro,
    s.plz4 AS sb_plz4,
    t.korr_plz4 AS gs_plz4,
    s.ort AS sb_ort,
    t.ort AS gs_ort,
    s.category_ids AS sb_category,
    t.category_ids AS gs_category,
    s.url AS sb_url,
    t.url AS gs_url,
    s.domain AS sb_domain,
    t.domain AS gs_domain,
    s.geo_point_google AS geo_point_schwim,
    t.geo_point_google AS geo_point_gastro,
    s.topcc_bezeichnung,
    s.topcc_poi_id
FROM  
    top_cc.schwimmbaeder_topcc AS s
FULL OUTER JOIN matched_gastro AS g ON s.cid = g.sb_id
FULL OUTER JOIN top_cc.takeaway_data_topcc AS t ON g.gs_id = t.cid
ORDER BY 
    has_gastro DESC,
    distance_meters;



select * from top_cc.schwimmbaeder_topcc; --340
select * from top_cc.takeaway_data_topcc; --2204
select * from top_cc.gastro_um_schwimmbaeder_v1; --2523




select gs_id, count(*) from top_cc.gastro_um_schwimmbaeder_v1 group by gs_id having count(*) > 1;







	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	







