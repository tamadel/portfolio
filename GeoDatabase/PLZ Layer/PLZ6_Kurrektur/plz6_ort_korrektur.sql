--===========================================
-- Project: pass den Ort zu Aktuelle PLZ6 an 
-- Date: 28.02.2025
-- 
--===========================================
--quelle 
select * from geo_afo_prod.imp_plz6_geo_neu_python;
--aktuell
select * from geo_afo_prod.imp_plz6_geo_neu plz;
--Ort
--select * from geo_afo_prod.mv_lay_plz4_aktuell;
select * from geo_afo_prod.qu_geopost_01_new_plz1_hist;


select 
	plz_ort,
	plz6,
	count(*)
from 
	geo_afo_prod.imp_plz6_geo_neu
group by
	plz_ort
	plz6
having 
	count(*) > 1
;


-- plz that has no plzzus = 0
select 
	distinct plz 
	,plz_ort
from 
	geo_afo_prod.imp_plz6_geo_neu
where 
	plz not in (
					select
						distinct plz
					from 
						geo_afo_prod.imp_plz6_geo_neu
					where 
						plzzus = '00'
	)
;



-- there are "plz 6872 with plz_zus 01" and "plz 2714 with plz_zus 02" are deaktivated by "qu_geopost_01_new_plz1_hist" on 2025-02-03
-- this changes still not updated on swisstotp data that's why this 2 plz6 687201 and  271402 has no Ort "null (*) old orts manuell added 
-- "6872  01  Somazzo" and  "2714  02  Le Prédame"  gültig_bis 2025-02-03
select 
	*
from 
	geo_afo_prod.imp_plz6_geo_neu
where 
	plz6 not in (
					select 
						t1.plz6
					from 
						geo_afo_prod.imp_plz6_geo_neu t1
					left join
						geo_afo_prod.qu_geopost_01_new_plz1_hist t2
					on
						t1.plz = t2.postleitzahl 
						and 
						t1.plzzus = t2.plz_zz 
					where 
						extract(year from t2.gueltig_bis) = 9999
	)

	
select 
	postleitzahl
	,plz_zz
	,ortbez18
	,gueltig_bis
from
	geo_afo_prod.qu_geopost_01_new_plz1_hist
where
	postleitzahl in (6872, 2714)
	and 
	plz_zz in ('01', '02')
;
	
	
	
	
-- update plz6 layer to contain ort per plz6 
drop table if exists geo_afo_prod.imp_plz6_geo_01_2025;
create table geo_afo_prod.imp_plz6_geo_01_2025
as	
select 
	t1.*,
	t2.ortbez18 as ort
from 
	geo_afo_prod.imp_plz6_geo_neu t1
left join(
			select 
				*
			from 
				geo_afo_prod.qu_geopost_01_new_plz1_hist
			where 
				extract(year from gueltig_bis) = 9999
	) t2
on
	t1.plz = t2.postleitzahl 
	and 
	t1.plzzus = t2.plz_zz 
;
	


select * from geo_afo_prod.imp_plz6_geo_01_2025 where plz = '5012';


-- verfication 
-- ort is unique per plz4  same per plz6 
select 
	plz
	,plz6
	,ort
	,count(*)
from 
	geo_afo_prod.imp_plz6_geo_01_2025
group by
	plz
	,plz6
	,ort
having 
	count(*) > 1
;













