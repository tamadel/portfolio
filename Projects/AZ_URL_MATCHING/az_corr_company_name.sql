--====================================
-- Project: AZ URLs rescherche
-- Action: company name correction
-- Date: 07.03.2025
-- update:
--====================================
-- Source Data 
select * from az_testdata.az_url_recherche;
select * from geo_zefix.unternehmen;
select * from az_testdata.az_search_url_monyh_ch_v;
select * from az_testdata.az_search_url_sear_ch_v;
select * from az_testdata.az_search_url_all_org_v;
select * from az_testdata.az_organic_urls; 				-- all organic without filiter  
select * from az_testdata.az_search_url; 				-- all Google organic search - raw data 
select * from az_testdata.az_map_url_gmap_v;

select * from az_testdata.az_search_url;
-- Function to normalize the string
create or replace function normalize_string(input_text TEXT)
returns TEXT as $$
begin 
    -- Remove all characters except alphanumeric and spaces
    return regexp_replace(input_text, '[^a-zA-Z0-9\s]', '', 'g');
end;
$$ language plpgsql immutable;

-----------------------------------------------
-- First: Name correction with uid_no 
-----------------------------------------------
-- 316 uid_no are null
drop table if exists geo_afo_tmp.tmp_az_correct_name;
create table 
	geo_afo_tmp.tmp_az_correct_name
as
select distinct on (bestandid) 
	t1.bestandid
	,t1.uid_no 			as az_uid_no
	,t2.uid_no 			as afo_uid_no
	,t1.title 			as az_name
	,t2.name 			as afo_name
	,t1.zuststrasse 	as strasse
	,t1.zustplz 		as plz4
	,t1.zustort 		as ort
	,t2.chid 
	,t2.status 
from 
	az_testdata.az_url_recherche t1
left join 
	geo_zefix.unternehmen t2
on 
	normalize_string(lower(t1.uid_no)) = normalize_string(lower(t2.uid_no))
order by 
	bestandid, 
         case  
             when status = 'ACTIVE' then 1  -- Prioritize "ACTIVE"
             else 2                         -- Otherwise, keep any other status
         end
;



select * from geo_afo_tmp.tmp_az_correct_name;
-- check duplicates 
select 
	az_uid_no 
	,count(*)
from 
	geo_afo_tmp.tmp_az_correct_name
group by
	 az_uid_no 
having 
	count(*) > 1
;


-- correct the duplicate 
create temp table tmp_duplicate_bestandid
as
select 
	bestandid 
	,status 
from
	geo_afo_tmp.tmp_az_correct_name
where 
	az_uid_no in (
					select 
						az_uid_no 
					from 
						geo_afo_tmp.tmp_az_correct_name
					group by
						 az_uid_no 
					having 
						count(*) > 1
	)
;

select * from tmp_duplicate_bestandid;
	
select distinct on (bestandid) 
		bestandid
		--,status
from
	tmp_duplicate_bestandid
order by 
	bestandid, 
         case  
             when status = 'ACTIVE' then 1  -- Prioritize "ACTIVE"
             else 2                         -- Otherwise, keep any other status
         end
;



select * from geo_afo_tmp.tmp_az_correct_name;


--------------------------------------------------------------
-- the following afo_name will takeover in the final table 
--------------------------------------------------------------
select 
	*
from 
	geo_afo_tmp.tmp_az_correct_name
where 
	lower(az_name) not like '%' || lower(afo_name) || '%'
	and
	az_uid_no is not null 
	and 
	afo_uid_no is not null 
;


--------------------------------------------------------------------
-- there are 17 az_uid_no that not existed in afo Unternehmen layer
--------------------------------------------------------------------
select 
	*
from 
	geo_afo_tmp.tmp_az_correct_name
where 
	afo_uid_no is null 
	and 
	az_uid_no is not null 
;
-- the following "az_uid_no" are not exists in afo 
-- will test manually
/*
CHE-391.093.304 -- UID-Register@BFS: Regionaler Sozialdienst der Gemeinden Burg, Menziken und Reinach

CHE-113.506.025 -- UID-Register@BFS: Amt für Landwirtschaft und Natur (it has to be) CHE-469.411.645 --INFORAMA Rütti https://www.inforama.ch/  : it matches with address
				-- wrong uid_no

CHE-108.531.690 -- UID-Register@BFS: Marcel Roth Inneneinrichtungen -- https://www.pierrefrey.com/en/points-of-sale/marcel-roth-inneneinrichtungen www.pierrefrey.com

CHE-108.117.936 -- az_data: André Rochat Rue du Maupas 27 google_suche: >> André Rochat Rue de Maupas 6 1004 Lausanne.  OR Architecte Rochat André  Rue du Maupas 8, 1004 Lausanne
				-- UID-Register@BFS: André Rochat Rue du Maupas 8, 1004 Lausanne
				
CHE-105.674.525 -- UID-Register@BFS: Hanniball Erwin Oberholzer Zusätzlicher Name: Hanniball's Expresskurier : Dauerhaft geschlossen : inaktiv INACTIVE

CHE-104.034.748 -- UID-Register@BFS: Verband der Hersteller von Bäckerei- & Konditorei- Halbfabrikaten : https://www.vhk.ch/

CHE-279.493.520 -- UID-Register@BFS: Schule Bürglen https://www.schulebuerglen.ch/

CHE-115.479.600 -- UID-Register@BFS: Chr. Graf & R. Häsler & J. Winkler  google name: Praxis für Kieferorthopädie – Liebefeld Köniz https://ortholiebefeld.ch/ 

CHE-479.161.898 -- UID-Register@BFS: Serge Rouvinet Zusätzlicher Name: Avocat

CHE-115.152.091 -- UID-Register@BFS: Feuerschaugemeinde Appenzell, Energie- und Wasserversorgung
				-- 1) https://www.kvu.ch/de/adressen/wasserversorgung?id=252   2) https://www.appenzell.ch/de/service/freizeitanbieter/energie-und-wasserversorgung-appenzell.html
				-- 3) https://www.ai.ch/feuerschaugemeinde
				
CHE-114.891.375 -- UID-Register@BFS: Einwohnergemeinde Wohlen bei Bern  https://www.wohlen-be.ch/de/

CHE-457.548.237 -- UID-Register@BFS: Sozialdienst Region Trachselwald 

CHE-116.243.885 -- UID-Register@BFS: Berufsbildungszentrum Olten  https://bbzolten.so.ch/bbz-olten-ueber-uns/

CHE-116.277.967 -- UID-Register@BFS: GebäudeKlima Schweiz https://gebaeudeklima-schweiz.ch/de/ Eichistrasse 1, 6055 Alpnach Dorf
				-- Adress on Google and AZ data is not correct search with: GebäudeKlima Schweiz, Rötzmattweg, Olten get the the one from UID-Register@BFS

CHE-108.953.720 -- UID-Register@BFS: Gemeindewerke Arth Elektrizitäts- und Wasserwerk

CHE-108.904.331 -- UID-Register@BFS: Kantonsspital Winterthur 
				-- AZ name is not correct "k kiosk"

CHE-107.078.630 -- UID-Register@BFS: Interverband für Rettungswesen IVR-IAS   https://www.144.ch/
 */


-- Note: AZ Data can contain incorrect Name or address or uid no
-- incorrect UID_No : CHE-113.506.025
-- incorrect address: CHE-116.277.967 it could be an old address
-- incorrect name: 	  CHE-108.904.331 



select 
	*
from 
	geo_afo_tmp.tmp_az_correct_name
where
	az_uid_no in (
					'CHE-457.548.237'
					,'CHE-279.493.520'
					,'CHE-391.093.304'
					,'CHE-107.078.630'
					,'CHE-115.479.600'
					,'CHE-105.674.525'
					,'CHE-116.277.967'
					,'CHE-104.034.748'
					,'CHE-479.161.898'
					,'CHE-114.891.375'
					,'CHE-115.152.091'
					,'CHE-113.506.025'
					,'CHE-108.117.936'
					,'CHE-108.904.331'
					,'CHE-108.953.720'
					,'CHE-108.531.690'
					,'CHE-116.243.885'
	)
;


-----------------------------------------------------------------
-- second: using google organic data to correct the company name 
-- for the 316 az_name that has no uid_no 
-----------------------------------------------------------------

select * from az_testdata.az_search_url;


select 
	bestandid 
	,name1 
	,keyword
	,title
	,zuststrasse 
	,zustplz 
	,zustort 
	,rank_absolute 
	,url 
	,type
from 
	az_testdata.az_search_url
where 
	bestandid in (
					select 
						bestandid 
					from 
						geo_afo_tmp.tmp_az_correct_name
					where 
						afo_uid_no is null 
	)
	and 
	rank_absolute <= 3
	and
    url not like '%search.ch%'
    and
    url not like '%moneyhouse.ch%'
    and
    url not like '%local.ch%'
    and
    url not like '%facebook.com%'
    and
    url not like '%zugerzeitung.ch%'
	and 
	url not like '%yellow.local.ch%'
	and 
	type in ('knowledge_graph', 'organic')
order by 
	bestandid,
	rank_absolute 
;
--type = 'knowledge_graph' 





--Moneyhouse 



--Search.ch













