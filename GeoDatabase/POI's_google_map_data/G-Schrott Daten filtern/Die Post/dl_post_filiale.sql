-----------------------------------------------
-- Project: Die Post Filiale
-- Datum: 14.02.2025
-- Update: 16.02.2025
-----------------------------------------------
-- Tabelle auf Serverless migrieren
drop table if exists
	geo_afo_prod.imp_gmd_geo_neu;
create table
	google_maps_dev.poi_abgleich_google_dienstleistung_tot
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
				google_maps_dev_abgleich.poi_abgleich_google_dienstleistung_tot
		$POSTGRES$
	) as imp_gmd_geo_neu (
			poi_id varchar(1000) 
			,hauskey numeric 
			,poi_typ_id numeric 
			,poi_typ text 
			,google_poi_typ varchar(1000) 
			,category_ids varchar(1000) 
			,company_group_id numeric 
			,company_group text 
			,company_id numeric 
			,company text 
			,company_unit text 
			,company_brand text 
			,bezeichnung_lang text 
			,bezeichnung_kurz text 
			,adresse text 
			,adress_lang varchar(1000) 
			,plz4 numeric 
			,plz4_orig numeric 
			,ort text 
			,google_strasse varchar(1000) 
			,google_strasse_std varchar(1000) 
			,google_hausnum varchar(1000) 
			,google_plz4 varchar(1000) 
			,google_ort varchar(1000) 
			,gwr_strasse varchar(1000) 
			,gwr_hausnum varchar(1000) 
			,gwr_plz4 int4 
			,gwr_ort varchar(1000) 
			,plz6 varchar(1000) 
			,gemeinde varchar(1000) 
			,gmd_nr varchar(1000) 
			,url varchar(10000) 
			,"domain" varchar(1000) 
			,geo_point_lv95 public.geometry(point, 2056) 
			,quelle varchar(255) 
			,dubletten_nr varchar(1000)
	)
;

select * from google_maps_dev.poi_abgleich_google_dienstleistung_tot; --59890



-- create temp table for test 
drop table if exists geo_afo_tmp.tmp_dienstleistung_test_ta;
create table 
	geo_afo_tmp.tmp_dienstleistung_test_ta
as
select 
	*
from 
	google_maps_dev.poi_abgleich_google_dienstleistung_tot
;



-- correct plz4 6300 in "poi_abgleich_google_dienstleistung_tot"
-- 253 ort not correct to plz4 6330
select 
	plz4,
	ort
from 
	geo_afo_tmp.tmp_dienstleistung_test_ta
where 
	plz4 = 6300
	and 
	ort <> 'Zug'
;



update geo_afo_tmp.tmp_dienstleistung_test_ta
set 
	ort = 'Zug'
where 
	plz4 = 6300
	and 
	ort <> 'Zug'
;

---------------------
-- inspect data set 
---------------------
select 
	* 
from 
	geo_afo_tmp.tmp_dienstleistung_test_ta
where
	company_group_id <> 215
	--and
	--lower(company) like 'post%'
	and
	LOWER(url) like '%001pst%'
	and 
	lower(company) not like '% filiale%'
;


	
select 
	* 
from 
	geo_afo_tmp.tmp_dienstleistung_test_ta
where
	lower(url) like '%places.post.ch/poi%' 
	and  
	company_group_id <> 215
;
	



---------------
-- google data
---------------
-- 2017
drop table if exists tmp_post_filiale;
create temp table tmp_post_filiale
as
select  
	* 
from  
	geo_afo_tmp.tmp_dienstleistung_test_ta
where  
	(
		(
			LOWER(company) like 'post%'
			and   
			LOWER(company) like '%filiale%'
		)
		or  
		(
			LOWER(url) like '%places.post.ch%'
			and  
			LOWER(url) like '%001pst%' --'%001mp24%' --
		)
	)
	and   
	company_group_id <> 215
;


-- In Shop vs Eigenständig Post Shop
select 
	poi_id,
	company,
	adresse,
	adress_lang,
	CASE 
        WHEN lower(adresse) ilike '%'||lower(SPLIT_PART(adress_lang, ',', 1))||'%' THEN NULL
        ELSE SPLIT_PART(adress_lang, ',', 1)
    end as in_shop_post,
    url
from 	
	tmp_post_filiale;
   
--///////////////////////////////
select  
    poi_id,
    company,
    adresse,
    adress_lang,
    case  
        -- Normalize common abbreviations in the extracted part
        when 
        	lower(adresse) ilike '%' || lower(
            regexp_replace(
            regexp_replace(
            regexp_replace(
            regexp_replace(
            regexp_replace(
            regexp_replace(
            regexp_replace(
            regexp_replace(
            regexp_replace(
            regexp_replace(
            regexp_replace(
            regexp_replace(
            regexp_replace(
            regexp_replace(
            regexp_replace(split_part(adress_lang, ',', 1),
            'Av\.', 'Avenue', 'gi'), 
            'str\.', 'strasse', 'gi'), 
            'Bahnhofstr\.', 'Bahnhofstrasse', 'gi'), 
            'Pl\.', 'platz', 'gi'),
            'Imp\.', 'Impasse', 'gi'),
            'Imp\.', 'Impasse', 'gi'),
            'Chem\.', 'Chemin', 'gi'),
            'pl\.', 'platz', 'gi'),
            'u\.', 'Untere', 'gi'),
            'G\.', 'Gasse', 'gi'),
            'Pl\. de la', 'Place de la', 'gi'), 
            'Pl\. des', 'Place des', 'gi'), 
            'Pl\. du', 'Place du', 'gi'), 
            'Rte', 'Route', 'gi'),
            'Rue', 'Route', 'gi')
        ) || '%' 																										then 'Eigenständiger Post-Shop' 
        when 
        	lower(adresse) ilike '%' || lower(regexp_replace(split_part(adress_lang, ',', 1),'Pl\.', 'Place', 'gi')) 	then 'Eigenständiger Post-Shop'
		when 
			lower(adresse) ilike '%'||lower(split_part(adress_lang, ',', 1))||'%' 										then 'Eigenständiger Post-Shop' 
        -- Ensure extracted text follows "Street Name + Number" format (try to avoid false match)
        when 
        	split_part(adress_lang, ',', 1) ~ '^[A-Za-zÀ-ÿ\s-]+ [0-9]+[a-zA-Z]?$' 										then 'Eigenständiger Post-Shop'   
        else 
        	split_part(adress_lang, ',', 1) 
    end as in_shop_post,
    url
from
	tmp_post_filiale
;


--adress_lang like '%Pl%'
--2287259768126565075

-- 13 étoiles Pam Valais, Administration communale, Aletsch Arena, avec, Cancelleria comunale, Coop, Coop Supermarkt, Denner, Denner Express, Denner Partner
-- Denner Partenaire, Dorfladen, Edelweiss Market, Gemeindeverwaltung, Il Consum, Konsum, Landi, L'Epicerie de, Migros, Migrolino, Migros Partenaire, Migros-Supermarkt
--Prima, SPAR, Spar Supermarkt, Supermarché Migros, Tourist Information, VOI, Volg

-- part of title then adress >> Bäckerei, Alimentation, Beck, Boulangerie, Chez, Dorfmarkt , Drogerie, Epicerie, Farmacia, Kiosk 






/*
	and 
	url is not null
	and
	LOWER(company) NOT LIKE '%my post%'
	and 
	LOWER(company) NOT LIKE '%my post service%'
	and 
	LOWER(company) NOT LIKE '%finance%'
	and  
	LOWER(company) NOT LIKE '%postomat%'
	and 
	LOWER(company) NOT LIKE '%briefeinwurf%'
	and 
	LOWER(company) NOT LIKE '%briefkasten%'
	and 
	LOWER(company) NOT LIKE '%hausservice%'
	and 
	LOWER(company) NOT LIKE '%postfachanlage%'
	and 
	LOWER(company) NOT LIKE '%annahmestelle%'
	and   
	LOWER(company) NOT LIKE '%logistik%'
	and   
	LOWER(company) NOT LIKE '%virtuelle%'
	and 
	LOWER(company) NOT LIKE '%self-service%'
	and 
	LOWER(company) NOT LIKE '%self service%'
	and 
	LOWER(company) NOT LIKE '%courrier%'
	and 
	LOWER(company) NOT LIKE '%mobile%'
	and 
	LOWER(company) NOT LIKE '%postplatz%'
	and 
	LOWER(company) NOT LIKE '%post & services%'
;
*/

--url duplicate >> https://places.post.ch/poi/-/-/-/-/001PST_001107203
select 
	url,
	count(*)
from
	tmp_post_filiale
group by
	url
having 
	count(*) > 1
;

select * from tmp_post_filiale;



-- cid eindeutig 
select 
	poi_id,
	count(*)
from
	tmp_post_filiale
group by
	poi_id
having 
	count(*) > 1
;


-- https://www.google.com/maps?cid=2200525963756365614
--company >> Post CH AG, Filiale 24
select 
	company,
	count(*)
from
	tmp_post_filiale
group by
	company
having 
	count(*) > 1
;


select 
	poi_id,
	company,
	url
from 
	tmp_post_filiale
where 
	company like '%Post CH AG,%'
;

-- 10333371539349726058  >> http://places.post.ch/poi/-/-/-/-/001PST_001106688  Seite nicht gefunden
-- 3435542487314219044	 >> http://places.post.ch/poi/-/-/-/-/001PST_001107191	Seite nicht gefunden	


select 
	* 
from 
	tmp_post_filiale
where 
	plz4 = 4123
;

--------------------
-- AFO Data
--------------------
-- 1845
 
select  
	* 
from  
	geo_afo_tmp.tmp_dienstleistung_test_ta
where  
	(
		(
			LOWER(company) like 'post%'
			and   
			LOWER(company_unit) in ('Geschäftskundenstelle', 'Postagentur', 'Post Filiale', 'Poststelle')
		)
		or  
		(
			LOWER(url) like '%places.post.ch%'
			and  
			LOWER(url) like '%001pst%'
			and 
			LOWER(url) like '%filiale%'
		)
	)
	and 
	quelle = 'AFO' --'GOOGLE'
	and
	dubletten_nr is not null 
;


-- mehr breite Suche
-- 2023
select  
	* 
from  
	geo_afo_tmp.tmp_dienstleistung_test_ta
where  
	(
		LOWER(company) like 'post%'
		or  
		LOWER(url) like '%places.post.ch%'
	)
	--and 
	--quelle = 'AFO' --'GOOGLE'
	and
	dubletten_nr is not null 
;





---------------- 
-- 	Data Match
----------------
select  
	*
from  
	geo_afo_tmp.tmp_dienstleistung_test_ta
where  
	(
		(
			LOWER(company) like 'post%'
			and   
			LOWER(company) like '%filiale%'
		)
		or 
		LOWER(company_unit) in ('Geschäftskundenstelle', 'Postagentur', 'Post Filiale', 'Poststelle')
		or  
		(
			LOWER(url) like '%places.post.ch%'
			and  
			LOWER(url) like '%001pst%'
		)
	)
	and 
	--quelle = 'AFO'
	--and
	dubletten_nr is not null 
;


select 
	*
from 
	geo_afo_tmp.tmp_dienstleistung_test_ta
where 
	dubletten_nr in (
						'12869469443383006834'
						,'12968018233291390830'
						,'12968018233291390830'
						,'15653147525232820363'
						,'15653147525232820363'
						,'16773374629587894977'
						,'1804392397840072799'
						,'18279725583131617074'
						,'18337586765119087469'
						,'2482162164650063920'
						,'2650463727731805117'
						,'3791807892236891401'
						,'4380488312317355608'
						,'4384495596542012194'
						,'4506779279941662660'
						,'6374412745786914743'
						,'6460593186540029034'
						,'7683568413312957572'
						,'7795718636313950848'
					)
					and 
					url not like '%bls.ch%'
order by 
	dubletten_nr,
	quelle
;





----------------
-- Cross-Query
----------------
create temp table tmp_cross_query
as
SELECT 
	dubletten_nr::VARCHAR(100),
	poi_id::VARCHAR(100),
	quelle,
	company,
	bezeichnung_lang,
	adresse,
	adress_lang,
	plz4,
	ort
FROM 
	(
		SELECT 
			* 
		FROM 
			geo_afo_tmp.tmp_dienstleistung_test_ta
		WHERE 
			dubletten_nr IN 
			( 
				SELECT 
					dubletten_nr
				FROM 
					geo_afo_tmp.tmp_dienstleistung_test_ta
				WHERE 
					(
						(
							LOWER(company) like 'post%'
							and   
							LOWER(company) like '%filiale%'
						)
						or 
						LOWER(company_unit) in (
												'Geschäftskundenstelle'
												,'Postagentur'
												,'Post Filiale'
												,'Poststelle'
						)
						or  
						(
							LOWER(url) like '%places.post.ch%'
							and  
							LOWER(url) like '%001pst%'
						)
					)
					AND
					dubletten_nr IS NOT NULL
			)
		UNION 
		SELECT 
			* 
		FROM 
			geo_afo_tmp.tmp_dienstleistung_test_ta
		WHERE 
					(
						(
							LOWER(company) like 'post%'
							and   
							LOWER(company) like '%filiale%'
						)
						or 
						LOWER(company_unit) in (
												'Geschäftskundenstelle'
												,'Postagentur'
												,'Post Filiale'
												,'Poststelle'
						)
						or  
						(
							LOWER(url) like '%places.post.ch%'
							and  
							LOWER(url) like '%001pst%'
						)
					)
					AND
					dubletten_nr IS NULL
			) AS un_table
ORDER BY 
	dubletten_nr
	,plz4
	,adresse
	,quelle
;

















-- Filiale 								>> https://places.post.ch/poi/-/-/-/-/001PST_001102551
-- My Post 24 							>> https://places.post.ch/poi/-/-/-/-/001MP24_001214193
-- My Post Service 						>> https://places.post.ch/poi/-/-/-/-/001AG-PICK_001212651
-- PostFinance 							>> https://places.post.ch/poi/-/-/-/-/001PFFIL_001213608
-- Postomat 							>> https://places.post.ch/poi/-/-/-/-/004PSTMAT_00413025328
-- Briefeinwurf 						>> https://places.post.ch/poi/-/-/-/-/003BE_00311545
-- hausservice 							>> https://places.post.ch/poi/-/-/-/-/001HS_001153505
-- postfachanlage 						>> https://places.post.ch/poi/-/-/-/-/001PFST_0012830
-- annahmestelle logistik 				>> https://places.post.ch/poi/-/-/-/-/001AL_001102026
-- geschäftskundenstelle self-service 	>> https://places.post.ch/poi/-/-/-/-/001GKSS_001215006
-- PostAuto 							>> https://places.post.ch/poi/-/-/-/-/001VPA_001215642






--schrott
--15549385637748400590
--5924874182248695424
--15618503925878248159
--13084743397615823212
--13481596652966487457
--4119079652182596187
--1661347898680578613
--14280964133890162981












/*	
select 
	* 
from 
	google_maps_dev_abgleich.poi_abgleich_google_finanzdienstleistung_tot
where  
	lower(company) like '%post%'
	or 
	lower(company) like '%finance%'
;
*/	