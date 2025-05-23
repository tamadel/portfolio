-------------------------------------------------------------------------------------------
-- Kunde: AFO
-- Thema: Kontrolle POI Liste
-- Datum: 148.06.2024
-- Autor: Tamer Adel
-- DB: geo_database
-- Schema: geo_afo_prod
-------------------------------------------------------------------------------------------

--------------------------------------------------------------------
-- Daten sichten
---------------------------------------------------------------------


/*
SELECT *
FROM google_maps_dev.afo_poi_typ_hotel_gastro
LIMIT 10;


SELECT *
FROM geo_afo_prod.afo_poi_typ_hotel_gastro
LIMIT 10;

SELECT *
FROM geo_afo_prod.google_hotel_gastro
LIMIT 100;


SELECT count(*)
FROM geo_afo_prod.afo_poi_typ_hotel_gastro
LIMIT 10;

SELECT count(*), count(distinct cid)
FROM geo_afo_prod.google_hotel_gastro
LIMIT 10;

SELECT *
FROM geo_afo_prod.google_hotel_gastro
where substring(strasse,1,1) in ('0','1','2','3','4','5','6','7','8','9')
LIMIT 10;

select *
from geo_afo_prod.google_hotel_gastro
order by coalesce(geo_point_lv95,'0') desc
limit 10
*/

CREATE OR REPLACE PROCEDURE 
	geo_afo_prod.sp_abgleich_poi_google
(
)
LANGUAGE 
	plpgsql
AS $$


begin


--------------------------------------------------------------------
-- 1. Daten für Ableich stamdardisieren
---------------------------------------------------------------------

-- Poi-Adressen standardisieren
drop table if exists #strasse;
create table #strasse as
select *,
	public.strasse_extract_strasse(coalesce(adresse,'')) as strasse,
	public.strasse_extract_hnr(coalesce(adresse,'')) as hnr,
	row_number() over (order by poi_id, random()) as id
from geo_afo_prod.afo_poi_typ_hotel_gastro;  -- Namen anpassen Poi Tabelle

update #strasse set
	strasse = substring(strasse,1,regexp_instr(STRASSE, '[0-9][^ ]*')-1)
where regexp_instr(STRASSE, '[0-9][^ ]*')+4>=length(strasse) and hnr<>'' and regexp_instr(STRASSE, '[0-9][^ ]*')>0
;

update #strasse set
	strasse = trim(trim(substring(strasse, regexp_instr(STRASSE, ',[^ ]*')+1,255))||' '||substring(strasse, 1, regexp_instr(STRASSE, ',[^ ]*')-1))
where strasse like '%,%'
;


drop table if exists geo_afo_prod.poi_mch;
create table geo_afo_prod.poi_mch as
select t1.*,
	public.clean_firma(coalesce(company,'')) as firma_std,
	public.clean_firma_ohne_phon(coalesce(company,'')) as firma_std_ohne_phon,
  	public.strassen_sufix(strasse) as str_sufix,
  	public.strassen_prefix(strasse) as str_prefix,
  	public.phon_mch_strasse(strasse) as strasse_std,
  	public.clean_name(public.phon_mch_strasse(strasse),1) as strasse_std1,
  	public.phon_mch_hnr(hnr) as hnr_std,
	coalesce(plz_grob, t1.plz4) as plz_grob
from #strasse t1
	left join geo_afo_prod.plz_grob t2 on t1.plz4=t2.plz;


update geo_afo_prod.poi_mch set
	firma_std_ohne_phon = replace(replace(firma_std_ohne_phon,'’',''),chr(39),'')
;


-- Google-Adressen standardisieren
drop table if exists #strasse;
create table #strasse as
select *,
	public.strasse_extract_strasse(coalesce(strasse,'')) as strasse_ber,
	public.strasse_extract_hnr(coalesce(strasse,'')) as hnr
from geo_afo_prod.google_hotel_gastro; -- Namen anpassen Google Tabelle

update #strasse set
	strasse_ber = substring(strasse,1,regexp_instr(strasse_ber, '[0-9][^ ]*')-1)
where regexp_instr(strasse_ber, '[0-9][^ ]*')+4>=length(strasse_ber) and hnr<>'' and regexp_instr(strasse_ber, '[0-9][^ ]*')>0
;

update #strasse set
	strasse_ber = trim(trim(substring(strasse_ber, regexp_instr(strasse_ber, ',[^ ]*')+1,255))||' '||substring(strasse_ber, 1, regexp_instr(strasse_ber, ',[^ ]*')-1))
where strasse_ber like '%,%'
;

update #strasse set
	plz4 = 0
where length(plz4)<>4                      
;

update #strasse set
	plz4 = trim(substring(address, 1, regexp_instr(address, ' [^ ]*')-1))
from #strasse 
where plz4 = 0 and substring(address,1,1) in ('1','2','3','4','5','6','7','8','9') and length(trim(substring(address, 1, regexp_instr(address, ' [^ ]*')-1)))=4                  
;

drop table if exists geo_afo_prod.google_mch;
create table geo_afo_prod.google_mch as
select t1.*,
	public.clean_firma(coalesce(title,'')) as firma_std,
	public.clean_firma_ohne_phon(coalesce(title,'')) as firma_std_ohne_phon,
  	public.strassen_sufix(strasse_ber) as str_sufix,
  	public.strassen_prefix(strasse_ber) as str_prefix,
  	public.phon_mch_strasse(strasse_ber) as strasse_std,
  	public.clean_name(public.phon_mch_strasse(strasse_ber),1) as strasse_std1,
  	public.phon_mch_hnr(hnr) as hnr_std,
	coalesce(plz_grob, cast(t1.plz4 as int)) as plz_grob
from #strasse t1
	left join geo_afo_prod.plz_grob t2 on cast(t1.plz4 as int)=t2.plz;

update geo_afo_prod.google_mch set
	firma_std_ohne_phon = replace(replace(firma_std_ohne_phon,'’',''),chr(39),'');


--------------------------------------------------------------------
-- 2. Abgleich
---------------------------------------------------------------------

-- Abgleich
drop table if exists geo_afo_prod.abgleich_poi;
create table geo_afo_prod.abgleich_poi as
SELECT a.*, b.cid, 1 as qual
FROM geo_afo_prod.poi_mch AS a LEFT JOIN geo_afo_prod.google_mch AS b
	ON (a.strasse_std=b.strasse_std AND a.strasse_std<>'' AND a.hnr_std=b.hnr_std AND a.plz_grob=b.plz_grob and len(a.plz_grob)>2)
		and public.levenshtein_levenshtein_token_set_ratio(a.firma_std_ohne_phon, b.firma_std_ohne_phon)>85 and a.firma_std_ohne_phon<>''
WHERE b.cid IS NOT null;

insert into geo_afo_prod.abgleich_poi
SELECT a.*, b.cid, 1 as qual
FROM geo_afo_prod.poi_mch AS a LEFT JOIN geo_afo_prod.google_mch AS b
	ON (a.strasse_std=b.strasse_std AND a.strasse_std<>'' AND a.hnr_std=b.hnr_std AND a.plz_grob=b.plz_grob and len(a.plz_grob)>2)
		and public.levenshtein_partial_ratio(a.firma_std_ohne_phon, b.firma_std_ohne_phon)>85 and a.firma_std_ohne_phon<>''
WHERE b.cid IS NOT null;

insert into geo_afo_prod.abgleich_poi
SELECT a.*, b.cid, 3 as qual
FROM geo_afo_prod.poi_mch AS a LEFT JOIN geo_afo_prod.google_mch AS b
	ON (a.strasse_std=b.strasse_std AND a.strasse_std<>'' AND a.hnr_std=b.hnr_std AND a.plz_grob=b.plz_grob and len(a.plz_grob)>2)
		and a.firma_std=b.firma_std and a.firma_std<>''
WHERE b.cid IS NOT null;

insert into geo_afo_prod.abgleich_poi
SELECT a.*, b.cid, 6 as qual
FROM geo_afo_prod.poi_mch AS a LEFT JOIN geo_afo_prod.google_mch AS b
	ON (a.strasse_std=b.strasse_std AND a.strasse_std<>'' AND a.plz_grob=b.plz_grob and len(a.plz_grob)>2) and (a.hnr_std='' or b.hnr_std='')
		and public.levenshtein_levenshtein_token_set_ratio(a.firma_std_ohne_phon, b.firma_std_ohne_phon)>90 and a.firma_std_ohne_phon<>''
WHERE b.cid IS NOT null;

insert into geo_afo_prod.abgleich_poi
SELECT a.*, b.cid, 7 as qual
FROM geo_afo_prod.poi_mch AS a LEFT JOIN geo_afo_prod.google_mch AS b
	ON (a.strasse_std1=b.strasse_std1 AND a.strasse_std1<>'' AND a.hnr_std=b.hnr_std AND a.plz_grob=b.plz_grob and len(a.plz_grob)>2)
		and public.levenshtein_levenshtein_token_set_ratio(a.firma_std_ohne_phon, b.firma_std_ohne_phon)>90 and a.firma_std_ohne_phon<>''
WHERE b.cid IS NOT null;

insert into geo_afo_prod.abgleich_poi
SELECT a.*, b.cid, 8 as qual
FROM geo_afo_prod.poi_mch AS a LEFT JOIN geo_afo_prod.google_mch AS b
	ON (public.levenshtein_levenshtein_token_set_ratio(a.strasse_std, b.strasse_std)>95 and a.strasse_std<>'' AND a.hnr_std=b.hnr_std AND a.plz_grob=b.plz_grob and len(a.plz_grob)>2)
		and public.levenshtein_levenshtein_token_set_ratio(a.firma_std_ohne_phon, b.firma_std_ohne_phon)>90 and a.firma_std_ohne_phon<>''
WHERE b.cid IS NOT null;

insert into geo_afo_prod.abgleich_poi
SELECT a.*, b.cid, 9 as qual
FROM geo_afo_prod.poi_mch AS a LEFT JOIN geo_afo_prod.google_mch AS b
	ON (a.strasse_std=b.strasse_std and a.strasse_std<>'' AND public.levenshtein_levenshtein_token_set_ratio(a.hnr_std, b.hnr_std)>=50 AND a.plz_grob=b.plz_grob and len(a.plz_grob)>2)
		and public.levenshtein_levenshtein_token_set_ratio(a.firma_std_ohne_phon, b.firma_std_ohne_phon)>90 and a.firma_std_ohne_phon<>''
WHERE b.cid IS NOT null;

insert into geo_afo_prod.abgleich_poi
SELECT a.*, b.cid, 10 as qual
FROM geo_afo_prod.poi_mch AS a LEFT JOIN geo_afo_prod.google_mch AS b
	ON (a.strasse_std=b.strasse_std and a.strasse_std<>'' AND a.hnr_std=b.hnr_std AND a.plz_grob=b.plz_grob and len(a.plz_grob)>2)
		and left(a.firma_std_ohne_phon,5)=left(b.firma_std_ohne_phon,5) and a.firma_std_ohne_phon<>''
WHERE b.cid IS NOT null;

insert into geo_afo_prod.abgleich_poi
SELECT a.*, b.cid, 11 as qual
FROM geo_afo_prod.poi_mch AS a LEFT JOIN geo_afo_prod.google_mch AS b
	ON (a.strasse_std=b.strasse_std and a.strasse_std<>'' AND a.hnr_std=b.hnr_std AND a.plz_grob=b.plz_grob and len(a.plz_grob)>2)
		and right(a.firma_std_ohne_phon,7)=right(b.firma_std_ohne_phon,7) and a.firma_std_ohne_phon<>''
WHERE b.cid IS NOT null;

insert into geo_afo_prod.abgleich_poi
SELECT a.*, b.cid, 12 as qual
FROM geo_afo_prod.poi_mch AS a LEFT JOIN geo_afo_prod.google_mch AS b
	ON (a.strasse_std=b.strasse_std and a.strasse_std<>'' AND a.plz_grob=b.plz_grob and len(a.plz_grob)>2)
		and a.firma_std_ohne_phon=b.firma_std_ohne_phon and a.firma_std_ohne_phon<>''
WHERE b.cid IS NOT null;

/*
insert into geo_afo_prod.abgleich_poi
SELECT a.*, b.cid, 14 as qual
FROM geo_afo_prod.poi_mch AS a LEFT JOIN geo_afo_prod.google_mch AS b
	ON (a.strasse_std='' AND a.plz_grob=b.plz_grob and len(a.plz_grob)>2)
		and public.levenshtein_levenshtein_token_set_ratio(a.firma_std_ohne_phon, b.firma_std_ohne_phon)>90 and a.firma_std_ohne_phon<>''
WHERE b.cid IS NOT null;
*/

-- Matching über Koordinaten innerhalb 50m
drop table if exists #base_koord;
create table #base_koord as
SELECT a.*, b.cid, 14 as qual, b.geo_point_lv95 as geo_point_lv95_b, b.firma_std_ohne_phon as firma_std_ohne_phon_b, b.strasse_std as strasse_std_b
FROM geo_afo_prod.poi_mch AS a LEFT JOIN geo_afo_prod.google_mch AS b
	ON public.levenshtein_levenshtein_token_set_ratio(a.firma_std_ohne_phon, b.firma_std_ohne_phon)>90 and a.firma_std_ohne_phon<>'' and a.plz_grob=b.plz_grob
WHERE b.cid IS NOT null;

delete from #base_koord where id in (select id from geo_afo_prod.abgleich_poi);

drop table if exists #mch_koord;
create table #mch_koord as
SELECT *
FROM #base_koord
WHERE ST_DWithin(ST_SetSRID(geo_point_lv95::geometry, 2056), ST_SetSRID(geo_point_lv95_b::geometry, 2056), 30); --ST_DWithin(geo_point_lv95::geometry, geo_point_lv95_b::geometry, 30);


alter table #mch_koord drop column strasse_std_b; 
alter table #mch_koord drop column firma_std_ohne_phon_b;
alter table #mch_koord drop column geo_point_lv95_b;

insert into geo_afo_prod.abgleich_poi
SELECT *
FROM #mch_koord;

-- Matching über Koordinaten innerhalb 50m und PLZ=0
drop table if exists #base_koord;
create table #base_koord as
with poi_mch as (select * from geo_afo_prod.poi_mch where id not in (select id from geo_afo_prod.abgleich_poi))

SELECT a.*, b.cid, 15 as qual, b.geo_point_lv95 as geo_point_lv95_b, b.firma_std_ohne_phon as firma_std_ohne_phon_b, b.strasse_std as strasse_std_b
FROM poi_mch AS a LEFT JOIN (select * from geo_afo_prod.google_mch where plz4=0) AS b
	ON public.levenshtein_levenshtein_token_set_ratio(a.firma_std_ohne_phon, b.firma_std_ohne_phon)>85 and a.firma_std_ohne_phon<>''
WHERE b.cid IS NOT null;

delete from #base_koord where id in (select id from geo_afo_prod.abgleich_poi);

drop table if exists #mch_koord;
create table #mch_koord as
SELECT *
FROM #base_koord
WHERE ST_DWithin(ST_SetSRID(geo_point_lv95::geometry, 2056), ST_SetSRID(geo_point_lv95_b::geometry, 2056), 30); --ST_DWithin(geo_point_lv95::geometry, geo_point_lv95_b::geometry, 30);

alter table #mch_koord drop column strasse_std_b; 
alter table #mch_koord drop column firma_std_ohne_phon_b;
alter table #mch_koord drop column geo_point_lv95_b;

insert into geo_afo_prod.abgleich_poi
SELECT *
FROM #mch_koord;


--------------------------------------------------------------------
-- 3. Zusammenfassung Abgleich
---------------------------------------------------------------------

-- Gastro-Adressen
drop table if exists geo_afo_prod.abgleich_poi_prio;
create table geo_afo_prod.abgleich_poi_prio as
select *, row_number () over (partition by id order by qual, random()) as prio
from geo_afo_prod.abgleich_poi;

delete from geo_afo_prod.abgleich_poi_prio where prio>1;

insert into geo_afo_prod.abgleich_poi_prio
select *
from geo_afo_prod.poi_mch
where id not in (select id from geo_afo_prod.abgleich_poi_prio);


--------------------------------------------------------------------
-- 4. Zusammenfassung Dubletten für Peter
---------------------------------------------------------------------
-- Dubletten File mit AZ Adressen und TopCC Adressen
drop table if exists geo_afo_prod.poi_abgleich_google_tot;
create table geo_afo_prod.poi_abgleich_google_tot as
select 
	cast(t1.poi_id as varchar(255)) as poi_id,
	t1.hauskey,
	t1.poi_typ_id,
	t1.poi_typ,
	cast('' as varchar(255)) as google_poi_typ,
	cast('' as varchar(255)) as category_ids_text,
	t1.company_group_id,
	t1.company_group,
	cast(t1.company_id as int) as company_id,
	--cast('' as varchar(255)) as keyword,
	--cast('' as varchar(255)) as category,
	t1.company,
	t1.company_unit,
	t1.company_brand,
	t1.bezeichnung_lang,
	t1.bezeichnung_kurz,
	t1.adresse,
	cast('' as varchar(255)) as adress_lang,
	t1.plz4,
	cast(t1.plz4 as varchar(255)) as plz4_orig,
	t1.ort,
	cast('' as varchar(255)) as google_strasse,
	cast('' as varchar(255)) as google_strasse_std,
	cast('' as varchar(255)) as google_hausnum,
	cast('' as varchar(255)) as google_plz4,
	cast('' as varchar(255)) as google_ort,
	cast('' as varchar(255)) as gwr_strasse,
	cast('' as varchar(255)) as gwr_hausnum,
	cast( 0 as int) as gwr_plz4,
	cast('' as varchar(255)) as gwr_ort,
	cast('' as varchar(255)) as plz6,
	cast('' as varchar(255)) as gemeinde,
	cast('' as varchar(255)) as gmd_nr,
	cast(t1.url as varchar(10000)) as url,
	cast('' as varchar(255)) as domain,
	t1.geo_point_lv95,
	cast('AFO' as varchar(255)) as quelle,
	cast(t2.cid as varchar(255)) as dubletten_nr 
from geo_afo_prod.poi_mch t1
	join geo_afo_prod.abgleich_poi_prio t2 on t1.id=t2.id;

insert into geo_afo_prod.poi_abgleich_google_tot
select 
	T1.cid, 
	0 as hauskey,
	0 as poi_typ_id,
	'' as poi_typ,
	google_poi_typ,
	category_ids_text as google_category_ids,
	0 as company_group_id,
	'' as company_group,
	0 as company_id,
	--keyword,
	--category,
	title, 
	'' as company_unit,
	'' as company_brand,
	'' as bezeichnung_lang,
	'' as bezeichnung_kurz,
	strasse, 
	address as adress_lang,
	cast(case when length(plz4)<>4 then 0 else cast(plz4 as int) end as int), 
	plz4 as plz4_orig,
	ort,
	google_strasse,
	google_strasse_std,
	google_hausnum,
	google_plz4,
	google_ort,
	gwr_strasse,
	gwr_hausnum,
	gwr_plz4,
	gwr_ort,
	plz6,
	gemeinde,
	gmd_nr,
	url,
	domain,
	geo_point_lv95,
	'GOOGLE' as quelle,
	cast(t2.cid as varchar(255)) as dubletten_nr 
from geo_afo_prod.google_hotel_gastro T1 -- Namen anpassen
	left join (select distinct cid from geo_afo_prod.abgleich_poi_prio) T2 on T1.cid=T2.cid;


end;

$$
;



--select distinct cid from geo_afo_prod.abgleich_poi_prio;
--select distinct cid from geo_afo_prod.google_hotel_gastro;

/*
call geo_afo_prod.sp_abgleich_poi_google();

1. Namen Rohtabellen anpassen
2. Procedure laufen lassen
3. Tabelle geo_afo_prod.poi_abgleich_google_tot exportieren

*/


/* Kontrolle

select *
from geo_afo_prod.poi_abgleich_google_tot
order by random() limit 20

select *
from geo_afo_prod.poi_abgleich_google_tot
where plz4=8404 order by dubletten_nr, adresse

select qual, count(*)
from geo_afo_prod.abgleich_poi_prio
group by qual
order by qual

select quelle, count(*)
from geo_afo_prod.poi_abgleich_google_tot
group by quelle
order by quelle

*/


-- Table for Peter 
drop table if exists google_maps_dev.poi_abgleich_google_tot_csv;
create table google_maps_dev.poi_abgleich_google_tot_csv
as
select
	*
from 
	geo_afo_prod.poi_abgleich_google_tot
order by
	dubletten_nr
;	


update google_maps_dev.poi_abgleich_google_tot_csv
set poi_id = '''' || poi_id,
	dubletten_nr = '''' || dubletten_nr
;


select 
	*
from 
	google_maps_dev.poi_abgleich_google_tot_csv
;




create table google_maps_dev.poi_abgleich_google_tot
as
select
	*
from 
	geo_afo_prod.poi_abgleich_google_tot
;

select 
	*
from 
	google_maps_dev.poi_abgleich_google_tot
order by
	dubletten_nr 
;














