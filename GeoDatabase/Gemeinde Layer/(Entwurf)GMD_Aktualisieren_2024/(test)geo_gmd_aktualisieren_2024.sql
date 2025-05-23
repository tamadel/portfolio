--==========================
-- gmd Aktualisieren
--==========================
select 
	count(*)
from
	geo_afo.gmd_bfs_tot 
;

create table geo_afo_prod.imp_gmd_geo_neu1
as
select 
	*
from 
	geo_afo_prod.imp_gmd_geo_neu_all
where 
	objektart = 'Gemeindegebiet' 
;

select distinct
	bezirksnum 
from
 geo_afo_prod.imp_gmd_geo_neu_v0
 ;

select 
	bfs_nummer,
	name, 
	count(*)
from 
	geo_afo_prod.imp_gmd_geo_neu 
where
	objektart = 'Gemeindegebiet'
group by
	bfs_nummer,
	name; 

---------------------------------------------
drop table if exists geo_afo_prod.imp_gmd_geo_neu;
create table geo_afo_prod.imp_gmd_geo_neu
as
select
	ogc_fid as gid, -- ich bin noch nicht sicher ob es ogc_fid genau wie gid ist
	bfs_nummer as gmd_nr,
	name as gemeinde,
	kantonsnum as kanton_nr,
	einwohnerz,
	hist_nr,
	herkunft_j,
	gem_flaech,
	geo_pol_lv95 as geo_poly_lv95,
	ST_AsText(ST_Transform(ST_SetSRID(geo_pol_lv95, 2056), 21781)) AS geo_poly_lv03,  
    ST_AsText(ST_Transform(ST_SetSRID(geo_pol_lv95, 2056), 4326)) AS geo_poly_wgs84
from 
	geo_afo_prod.imp_gmd_geo_neu_v0
;



select 
	*
from 
	geo_afo_prod.imp_gmd_geo_neu;

select 
	*
from 
	geo_afo_prod.lay_gmd_geo_hist;


--==================================================
-- Abgleich mit der hist-Tabelle
--==================================================
-- Duplication überprüfung in lay_plz6_geo_hist
select
	gmd_nr, 
	count(*)
from
	geo_afo_prod.lay_gmd_geo_hist
where 
	extract(year from gueltig_bis) = 9999
group by
	gmd_nr
having	
	count(*) > 1;

-- Duplication überprüfung in imp_plz6_geo_neu
select
	gmd_nr, 
	count(*)
from
	geo_afo_prod.imp_gmd_geo_neu
group by
	gmd_nr
having
	count(*) > 1;

-- historische Tabelle vorbreiten
drop table if exists 
				tmp_gmd_hist;
create temp table 
				tmp_gmd_hist
as
select
	*
from 
	geo_afo_prod.lay_gmd_geo_hist
where 
	extract(year from gueltig_bis) = 9999
;


----------------------
--gemeinde wird gelöscht: 
----------------------
-- 2456, 947, 993, 6775, 4042 und 6773
select
	gmd_nr
from
	tmp_lay_gmd_geo_hist
where
	gmd_nr not in(
					select
						gmd_nr
					from
						geo_afo_prod.imp_gmd_geo_neu
	) and
	extract(year from gueltig_bis) = 9999
;
------------------------
--gemeinde wird hinzugefügt:
------------------------
-- 7301, 7101, und 6812
select
	gmd_nr
from
	geo_afo_prod.imp_gmd_geo_neu
where
	gmd_nr not in (
				select
					gmd_nr 
				from 
					tmp_lay_gmd_geo_hist
	)
;

-------------------------------------------
-- Gemeinde, wo sich die Geometrie geändert hat:
-------------------------------------------
-- ST_Equals Funktion
select
    t0.gmd_nr,
    t0.geo_poly_lv95 as old_geo_poly_lv95,
    t1.geo_poly_lv95 as new_geo_poly_lv95
from
    tmp_gmd_hist t0
join
    geo_afo_prod.imp_gmd_geo_neu t1
on
    t0.gmd_nr = t1.gmd_nr 
where 
    not ST_Equals(
        ST_SetSRID(t0.geo_poly_lv95, 2056),   
        ST_SetSRID(t1.geo_poly_lv95, 2056)    
    )
group by
    t0.gmd_nr, 
    t0.geo_poly_lv95, 
    t1.geo_poly_lv95
   ;

  
-- Area Differenz
select
    t0.gmd_nr,
    ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056)) AS area_old,
    ST_Area(ST_SetSRID(t1.geo_poly_lv95, 2056)) AS area_new,
    round(ST_Area(ST_SetSRID(t1.geo_poly_lv95, 2056)) - ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056))) AS area_diff,
    t0.geo_poly_lv95 as old_poly,
    t1.geo_poly_lv95 as new_poly
from
    tmp_gmd_hist t0
join
    geo_afo_prod.imp_gmd_geo_neu t1
on
    t0.gmd_nr = t1.gmd_nr  
where
    ST_Area(ST_SetSRID(t1.geo_poly_lv95, 2056)) != ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056))
order by
	area_diff desc;


select 
	*
from 
	tmp_gmd_hist
where
	gmd_nr = 5112
;


select 
	*
from 
	geo_afo_prod.imp_gmd_geo_neu
where
	gmd_nr = 5112
;
--=========================================
--====== Python Script Test ===============
--=========================================
select 
	*
from
	geo_afo_prod.imp_gmd_gebiet_geo
;

select 
	*
from
	geo_afo_prod.imp_gmd_grenze_geo
;




drop table if exists geo_afo_prod.imp_gmd_geo_neu;
create table geo_afo_prod.imp_gmd_geo_neu
as
select
	--ogc_fid as gid, -- ich bin noch nicht sicher ob es ogc_fid genau wie gid ist
	"BFS_NUMMER" as gmd_nr,
	"NAME" as gemeinde,
	"KANTONSNUM" as kanton_nr,
	"BEZIRKSNUM" as bzr_nr,
	"EINWOHNERZ" as einwohnerz,
	"HIST_NR" as hist_nr,
	"HERKUNFT_J" as herkunft_j,
	"GEM_FLAECH" as gem_flaech,
	 ST_Multi(geometry) as geo_poly_lv95,
	ST_AsText(ST_Transform(ST_SetSRID(ST_Multi(geometry), 2056), 21781)) as geo_poly_lv03,  
    ST_AsText(ST_Transform(ST_SetSRID(ST_Multi(geometry), 2056), 4326)) as geo_poly_wgs84
from 
	geo_afo_prod.imp_gmd_gebiet_geo
where 
	"OBJEKTART" = 'Gemeindegebiet'
;

-------------------------------------------
--GMD, wo sich die Geometrie geändert hat:
-------------------------------------------
select 
	count(*)
from(
		select 
			gmd_nr,
			gemeinde,
			area_overlap/area_old as percentage_overlap,
			old_poly,
			new_poly
		from(
				select 
				    t0.gmd_nr,
				    gueltig_bis,
				    t0.gemeinde,
				    t1.objekt_art,
				    round(ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056))) as  area_old,
				    round(ST_Area(ST_SetSRID(t1.geo_poly_lv95, 2056))) as  area_new,
				    round(ST_Area(ST_Intersection(
				        ST_SetSRID(t0.geo_poly_lv95, 2056),
				        ST_SetSRID(t1.geo_poly_lv95, 2056)
				    ))) as area_overlap,
				    ST_SetSRID(t0.geo_poly_lv95, 2056) as old_poly,
				    ST_SetSRID(t1.geo_poly_lv95, 2056) as new_poly
				from
				    tmp_lay_gmd_geo_hist t0 --geo_afo_prod.mv_lay_plz6_aktuell  t0
				join
				    geo_afo_prod.imp_gmd_geo_neu t1
				on
				    t0.gmd_nr = t1.gmd_nr
			) t
			where 
				(area_overlap / area_old) < 0.995
				and 
				extract(year from gueltig_bis) = 9999
				and 
				objekt_art = 'Gemeindegebiet'
			order by
				    percentage_overlap
	) p 
;

		
		   
--=================================
-- TEST 
--================================		   
  /* select
        "ICC" as icc,
        "BFS_NUMMER" as gmd_nr,
        "NAME" as gemeinde,
        "KANTONSNUM" as kanton_nr,
        "BEZIRKSNUM" as bzr_nr,
        "EINWOHNERZ" as einwohnerz,
        "HIST_NR" as hist_nr,
        "HERKUNFT_J" as herkunft_j,
        "OBJEKTART" as objekt_art,
        "GEM_FLAECH" as gem_flaech,
        ST_Force2D(ST_Multi(geometry)) as geo_poly_lv95,
        ST_AsText(ST_Transform(ST_SetSRID(ST_Force2D(ST_Multi(geometry)), 2056), 21781)) as geo_poly_lv03,  
        ST_AsText(ST_Transform(ST_SetSRID(ST_Force2D(ST_Multi(geometry)), 2056), 4326)) as geo_poly_wgs84
    from 
       geo_afo_prod.imp_gmd_geo_neu_python
    where 
        "ICC" in ('LI', 'CH')
        and
        "OBJEKTART" = 'Gemeindegebiet'
    ;	   
		  
select 
	*
from 
	geo_afo_prod.imp_gmd_geo_neu 
;

select 
	*
from 
	geo_afo_prod.imp_gmd_geo_neu_python
;	
*/
--=================================
-- Finale Query
--=================================
drop table if exists tmp_lay_gmd_geo_hist;
create temp table tmp_lay_gmd_geo_hist
as
select
	*
from 
	geo_afo_prod.lay_gmd_geo_hist
;
   
-- Delete/deactivate old PLz6   
update
	tmp_lay_gmd_geo_hist
set
	gueltig_bis = current_date 
where
	gmd_nr in(
			select
				gmd_nr
			from
				tmp_lay_gmd_geo_hist
			where
				gmd_nr not in(
							select
								gmd_nr 
							from
								geo_afo_prod.imp_gmd_geo_neu
				)
				and 
				gueltig_bis = '9999-12-31'
	)
	and
    gueltig_bis = '9999-12-31'
;
    
-- insert new records     
insert into tmp_lay_gmd_geo_hist (gmd_nr, gueltig_bis)
select  
    neu.gmd_nr, 
    '9999-12-31' AS gueltig_bis
from  
    geo_afo_prod.imp_gmd_geo_neu neu
where  
    not exists (
        select		
        	neu.gmd_nr 
        from
        	tmp_lay_gmd_geo_hist hist
        where
        	hist.gmd_nr = neu.gmd_nr
        	and
        	gueltig_bis = '9999-12-31'
    );
   
-- replace old poly with new poly if perecentage_overlap < 0.995
select 
	count(*)
from(
		update
			tmp_lay_gmd_geo_hist
		set 
			geo_poly_lv95 = u.new_poly,
			geo_poly_lv03 = ST_Transform(u.new_poly, 21781),
		    geo_poly_wgs84 = ST_Transform(u.new_poly, 4326),
		    --geo_point_lv95 = ST_Centroid(u.new_poly),
		    --geo_point_lv03 = ST_Transform(ST_Centroid(geo_poly_lv95), 21781),
		    --geo_point_wgs84 = ST_Transform(ST_Centroid(geo_poly_lv95), 4326), 
			gueltig_von = current_date,
			gueltig_bis = '9999-12-31'
		from(
			select 
				gmd_nr,
				gemeinde,
				area_overlap/area_old as percentage_overlap,
				old_poly,
				new_poly
			from(
				select 
				    t0.gmd_nr,
				    gueltig_bis,
				    t0.gemeinde,
				    t1.objekt_art,
				    round(ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056))) as  area_old,
				    round(ST_Area(ST_SetSRID(t1.geo_poly_lv95, 2056))) as  area_new,
				    round(ST_Area(ST_Intersection(
				        ST_SetSRID(t0.geo_poly_lv95, 2056),
				        ST_SetSRID(t1.geo_poly_lv95, 2056)
				    ))) as area_overlap,
				    ST_SetSRID(t0.geo_poly_lv95, 2056) as old_poly,
				    ST_SetSRID(t1.geo_poly_lv95, 2056) as new_poly
				from
				    tmp_lay_gmd_geo_hist t0 
				join
				    geo_afo_prod.imp_gmd_geo_neu t1
				on
				    t0.gmd_nr = t1.gmd_nr
			) t
			where 
				(area_overlap / area_old) < 0.995
				and 
				extract(year from gueltig_bis) = 9999
				and 
				objekt_art = 'Gemeindegebiet'
			order by
				    percentage_overlap
		) u
		where 
			h.gmd_nr = u.gmd_nr
		returning 
			h.gmd_nr
	) as updated_rows
;



select 
	*
from 
	tmp_lay_gmd_geo_hist;

select 
	*
from 
	geo_afo_prod.imp_gmd_geo_neu_python;


select 
	*
from 
	geo_afo_prod.imp_gmd_geo_neu ;



--TEST for Python 
select 
    gmd_nr,
    gemeinde,
    area_overlap/area_old as percentage_overlap,
    old_poly,
    new_poly
from(
    select 
        t0.gmd_nr,
        gueltig_bis,
        t0.gemeinde,
        t1.objekt_art,
        round(ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056))) as  area_old,
        round(ST_Area(ST_SetSRID(t1.geo_poly_lv95, 2056))) as  area_new,
        round(ST_Area(ST_Intersection(
            ST_SetSRID(t0.geo_poly_lv95, 2056),
            ST_SetSRID(t1.geo_poly_lv95, 2056)
        ))) as area_overlap,
        ST_SetSRID(t0.geo_poly_lv95, 2056) as old_poly,
        ST_SetSRID(t1.geo_poly_lv95, 2056) as new_poly
    from
    	geo_afo_prod.test_lay_gmd_geo_hist t0 
    join
        geo_afo_prod.imp_gmd_geo_neu t1
    on
        t0.gmd_nr = t1.gmd_nr
    where 
    	extract(year from gueltig_bis) = 9999
) t
where 
    (area_overlap / area_old) < 0.995
;

