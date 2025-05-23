--=============================
-- PLZ6 aktualisieren
--============================
select 
	*
from 
	geo_afo_prod.imp_plz6_geo_neu_v0;
	
select 
	*
from 
	geo_afo_prod.lay_plz6_geo_hist
;


select 
	*
from 
	geo_afo_prod.imp_plz_geo
; 

select 
	*
from 
	geo_afo_prod.imp_plz6_geo_neu_python
;

drop table if exists geo_afo_prod.imp_plz6_geo_neu;
create table geo_afo_prod.imp_plz6_geo_neu
as
select
	"ZIP_ID" as plz_id,
	"ZIP4"::numeric as plz,
	"ADDITIONAL" as zusatzziffer,
	concat("ZIP4", "ADDITIONAL")::numeric as plz6,
	"MODIFIED" as modified,
	"SHAPE_AREA" as shape_area,
	"SHAPE_LEN" as shape_len,
	geometry as geo_poly_lv95,
	ST_AsText(ST_Transform(ST_SetSRID(geometry, 2056), 21781)) AS geo_poly_lv03,  
    ST_AsText(ST_Transform(ST_SetSRID(geometry, 2056), 4326)) AS geo_poly_wgs84
from 
	geo_afo_prod.imp_plz_geo 
	--geo_afo_prod.imp_plz6_geo_neu_v0
;

select 
	*
from 
	geo_afo_prod.imp_plz6_geo_neu;

select 
	*
from
	geo_afo_prod.lay_plz6_geo_hist;
--==================================================
-- Abgleich mit der hist-Tabelle
--==================================================
-- Duplication überprüfung in lay_plz6_geo_hist
select
	plz6, count(*)
from
	geo_afo_prod.lay_plz6_geo_hist
where 
	extract(year from gueltig_bis) = 9999
group by
	plz6
having	
	count(*) > 1;

-- Duplication überprüfung in imp_plz6_geo_neu
select
	plz6, 
	count(*)
from
	geo_afo_prod.imp_plz6_geo_neu
group by
	plz6
having
	count(*) > 1;

---------------------------------
-- historische Tabelle vorbreiten
---------------------------------
drop table if exists 
				tmp_plz6_hist;
create temp table 
				tmp_plz6_hist
as
select
	*
from 
	geo_afo_prod.lay_plz6_geo_hist
where 
	extract(year from gueltig_bis) = 9999
;


----------------------
--PLZ6 wird gelöscht: 
----------------------
-- 677701 und 678002
select
	*
from
	tmp_lay_plz6_geo_hist
where
	plz6 not in(
				select
					plz6
				from
					geo_afo_prod.imp_plz6_geo_neu_python	
		)
	and
	extract(year from gueltig_bis) = 9999
;
------------------------
--PLZ6 wird hinzugefügt:
------------------------
-- 396305
select
	count(*)
from
	geo_afo_prod.imp_plz6_geo_neu
where
	plz6 not in (
				select
					plz6 
				from 
					tmp_lay_plz6_geo_hist
				where 
					extract(year from gueltig_bis) = 9999
	);
















-------------------------------------------
--PLZ6, wo sich die Geometrie geändert hat:
-------------------------------------------
select 
	plz6,
	--ort,
	area_overlap/area_old as percentage_overlap,
	old_poly,
	new_poly
from(
		select 
		    t0.plz6,
		    gueltig_bis,
		    --t0.ort,
		    round(ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056))) as  area_old,
		    round(ST_Area(ST_SetSRID(t1.geo_poly_lv95, 2056))) as  area_new,
		    round(ST_Area(ST_Intersection(
		        ST_SetSRID(t0.geo_poly_lv95, 2056),
		        ST_SetSRID(t1.geo_poly_lv95, 2056)
		    ))) as area_overlap,
		    ST_SetSRID(t0.geo_poly_lv95, 2056) as old_poly,
		    ST_SetSRID(t1.geo_poly_lv95, 2056) as new_poly
		from
		    tmp_lay_plz6_geo_hist t0 --geo_afo_prod.mv_lay_plz6_aktuell  t0
		join
		    geo_afo_prod.imp_plz6_geo_neu_python  t1
		on
		    t0.plz6 = t1.plz6
	) t
	where 
		(area_overlap / area_old) < 0.995
		and 
		gueltig_bis = '9999-12-31'
	order by
		    percentage_overlap;

--=================================
-- Finale Query
--=================================
drop table if exists tmp_lay_plz6_geo_hist;
create temp table tmp_lay_plz6_geo_hist
as
select
	*
from 
	geo_afo_prod.lay_plz6_geo_hist
;
   
-- Delete/deactivate old PLz6   
update
	tmp_lay_plz6_geo_hist
set
	gueltig_bis = current_date --NOW()
where
	plz6 in(
			select
				plz6
			from
				tmp_lay_plz6_geo_hist
			where
				plz6 not in(
							select
								plz6 
							from
								geo_afo_prod.imp_plz6_geo_neu_python
				)
	)
	and
    gueltig_bis = '9999-12-31'
;
    
-- insert new records     
insert into tmp_lay_plz6_geo_hist (plz6, gueltig_bis)
select  
    neu.plz6, 
    '9999-12-31' AS gueltig_bis
from  
    geo_afo_prod.imp_plz6_geo_neu_python neu
where  
    not exists (
        select		
        	neu.plz6 
        from
        	tmp_lay_plz6_geo_hist hist
        where
        	hist.plz6 = neu.plz6
    );
   
-- replace old poly with new poly if perecentage_overlap < 0.995
update
	tmp_lay_plz6_geo_hist h
set 
	geo_poly_lv95 = u.new_poly,
	geo_poly_lv03 = ST_Transform(u.new_poly, 21781),
    geo_poly_wgs84 = ST_Transform(u.new_poly, 4326),
    geo_point_lv95 = ST_Centroid(u.new_poly),
    geo_point_lv03 = ST_Transform(ST_Centroid(geo_poly_lv95), 21781),
    geo_point_wgs84 = ST_Transform(ST_Centroid(geo_poly_lv95), 4326), 
	gueltig_von = current_date-- now()
from(
	select 
		plz6,
		--ort,
		area_overlap/area_old as percentage_overlap,
		old_poly,
		new_poly
	from(
			select 
			    t0.plz6,
			    --t0.ort,
			    round(ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056))) as  area_old,
			    round(ST_Area(ST_SetSRID(t1.geo_poly_lv95, 2056))) as  area_new,
			    round(ST_Area(ST_Intersection(
			        ST_SetSRID(t0.geo_poly_lv95, 2056),
			        ST_SetSRID(t1.geo_poly_lv95, 2056)
			    ))) as area_overlap,
			    ST_SetSRID(t0.geo_poly_lv95, 2056) as old_poly,
			    ST_SetSRID(t1.geo_poly_lv95, 2056) as new_poly
			from
			    geo_afo_prod.mv_lay_plz6_aktuell  t0
			join
			    geo_afo_prod.imp_plz6_geo_neu_python t1
			on
			    t0.plz6 = t1.plz6
		) t
		where 
			(area_overlap / area_old) < 0.995
		order by
			    percentage_overlap
) u
where 
	h.plz6 = u.plz6
;
--====================================================================================





select
	h.plz6, 
	h.gueltig_bis,
	h.geo_poly_lv95 AS old_polygon, 
	n.geo_poly_lv95 AS new_polygon
from
	tmp_lay_plz6_geo_hist h
join
	geo_afo_prod.imp_plz6_geo_neu n 
on
	h.plz6 = n.plz6
where
	not ST_Equals(
        ST_SetSRID(h.geo_poly_lv95, 2056), 
        ST_SetSRID(n.geo_poly_lv95, 2056)
    );

		   
		   
		   
		   
		   
select 
	*
from 
	tmp_lay_plz6_geo_hist;
		   
		   
--Test for Python script --
select
    "ZIP_ID" as plz_id,
    "ZIP4"::numeric as plz,
    "ADDITIONAL" as zusatzziffer,
    concat("ZIP4", "ADDITIONAL")::numeric as plz6,
    "MODIFIED" as modified,
    "SHAPE_AREA" as shape_area,
    "SHAPE_LEN" as shape_len,
    ST_Multi(geometry) as geo_poly_lv95,
    ST_AsText(ST_Transform(ST_SetSRID(ST_Multi(geometry), 2056), 21781)) AS geo_poly_lv03,  
    ST_AsText(ST_Transform(ST_SetSRID(ST_Multi(geometry), 2056), 4326)) AS geo_poly_wgs84
from 
   geo_afo_prod.imp_plz_geo
where 
    ST_GeometryType(geometry) = 'ST_Polygon'
;
		   
		   
		   
		   

--////////////////////////////////////////////////////////////////////////////////////////////////////
--Test for python script
--///////////////////////////////////////////////////////////////////////////////////////////////////
select 
    count(*)
from(
        select 
            plz6,
            gueltig_bis,
            area_overlap/area_old as percentage_overlap,
            old_poly,
            new_poly
        from(
                select 
                    t0.plz6,
                    gueltig_bis,
                    round(ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056))) as  area_old,
                    round(ST_Area(ST_SetSRID(t1.geo_poly_lv95, 2056))) as  area_new,
                    round(ST_Area(ST_Intersection(
                        ST_SetSRID(t0.geo_poly_lv95, 2056),
                        ST_SetSRID(t1.geo_poly_lv95, 2056)
                    ))) as area_overlap,
                    ST_SetSRID(t0.geo_poly_lv95, 2056) as old_poly,
                    ST_SetSRID(t1.geo_poly_lv95, 2056) as new_poly
                from
                   geo_afo_prod.test_lay_plz6_geo_hist t0 
                join
                    geo_afo_prod.imp_plz6_geo_neu_python t1
                on
                    t0.plz6 = t1.plz6
                where 
                	extract(year from gueltig_bis) = 9999
            ) t
            where 
                (area_overlap / area_old) < 0.995
                and 
                extract(year from gueltig_bis) = 9999
            order by
                    percentage_overlap
    ) p 
;


drop table if exists 
    geo_afo_prod.test_lay_plz6_geo_hist  
;
create table 
    geo_afo_prod.test_lay_plz6_geo_hist  
as
select
    *
from 
    geo_afo_prod.lay_plz6_geo_hist
;

select 
	plz6,
	count(*)
from 
	geo_afo_prod.test_lay_plz6_geo_hist
where 
	extract(year from gueltig_bis) = 9999
group by
	plz6
having 
	count(*) > 1
	;

select 
	count(*)
from 
	geo_afo_prod.imp_plz6_geo_neu_python;

	

 UPDATE
    geo_afo_prod.test_lay_plz6_geo_hist
SET 
    geo_poly_lv95 = u.new_poly,
    geo_poly_lv03 = ST_Transform(u.new_poly, 21781),
    geo_poly_wgs84 = ST_Transform(u.new_poly, 4326),
    geo_point_lv95 = ST_Centroid(u.new_poly),
    geo_point_lv03 = ST_Transform(ST_Centroid(geo_poly_lv95), 21781),
    geo_point_wgs84 = ST_Transform(ST_Centroid(geo_poly_lv95), 4326), 
    gueltig_von = CURRENT_DATE --NOW()
FROM (
    SELECT 
        plz6,
        area_overlap / area_old AS percentage_overlap,
        old_poly,
        new_poly
    FROM (
        SELECT 
            t0.plz6,
            gueltig_bis,
            ROUND(ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056))) AS area_old,
            ROUND(ST_Area(ST_SetSRID(t1.geo_poly_lv95, 2056))) AS area_new,
            ROUND(ST_Area(ST_Intersection(
                ST_SetSRID(t0.geo_poly_lv95, 2056),
                ST_SetSRID(t1.geo_poly_lv95, 2056)
            ))) AS area_overlap,
            ST_SetSRID(t0.geo_poly_lv95, 2056) AS old_poly,
            ST_SetSRID(t1.geo_poly_lv95, 2056) AS new_poly
        FROM
            geo_afo_prod.test_lay_plz6_geo_hist t0
        JOIN
            geo_afo_prod.imp_plz6_geo_neu_python t1
        ON
            t0.plz6 = t1.plz6
        WHERE
            extract(year from gueltig_bis) = 9999
    ) t
    WHERE 
        (area_overlap / area_old) < 0.995
        and
        extract(year from gueltig_bis) = 9999
    ORDER BY
        percentage_overlap
) u
;	   

UPDATE
    geo_afo_prod.test_lay_plz6_geo_hist h
SET 
    geo_poly_lv95 = u.new_poly,
    geo_poly_lv03 = ST_Transform(u.new_poly, 21781),
    geo_poly_wgs84 = ST_Transform(u.new_poly, 4326),
    geo_point_lv95 = ST_Centroid(u.new_poly),
    geo_point_lv03 = ST_Transform(ST_Centroid(u.new_poly), 21781),
    geo_point_wgs84 = ST_Transform(ST_Centroid(u.new_poly), 4326), 
    gueltig_von = CURRENT_DATE
select 
	*
FROM (
    SELECT 
        t0.plz6,
        ROUND(ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056))) AS area_old,
        ROUND(ST_Area(ST_SetSRID(t1.geo_poly_lv95, 2056))) AS area_new,
        ROUND(ST_Area(ST_Intersection(
            ST_SetSRID(t0.geo_poly_lv95, 2056),
            ST_SetSRID(t1.geo_poly_lv95, 2056)
        ))) AS area_overlap,
        ST_SetSRID(t0.geo_poly_lv95, 2056) AS old_poly,
        ST_SetSRID(t1.geo_poly_lv95, 2056) AS new_poly
    FROM
        geo_afo_prod.test_lay_plz6_geo_hist t0
    JOIN
        geo_afo_prod.imp_plz6_geo_neu_python t1
    ON
        t0.plz6 = t1.plz6
    WHERE
        extract(year from t0.gueltig_bis) = 9999
) u
WHERE 
    (u.area_overlap / u.area_old) < 0.995
    AND h.plz6 = u.plz6
;

select
	count(*)
from(
		UPDATE
		    test_lay_plz6_geo_hist h
		SET
		    geo_poly_lv95 = u.new_poly,
		    geo_poly_lv03 = ST_Transform(u.new_poly, 21781),
		    geo_poly_wgs84 = ST_Transform(u.new_poly, 4326),
		    geo_point_lv95 = ST_Centroid(u.new_poly),
		    geo_point_lv03 = ST_Transform(ST_Centroid(u.new_poly), 21781),
		    geo_point_wgs84 = ST_Transform(ST_Centroid(u.new_poly), 4326), 
		    gueltig_von = CURRENT_DATE
		FROM (
		    SELECT 
		        t0.plz6,
		        ROUND(ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056))) AS area_old,
		        ROUND(ST_Area(ST_SetSRID(t1.geo_poly_lv95, 2056))) AS area_new,
		        ROUND(ST_Area(ST_Intersection(
		            ST_SetSRID(t0.geo_poly_lv95, 2056),
		            ST_SetSRID(t1.geo_poly_lv95, 2056)
		        ))) AS area_overlap,
		        ST_SetSRID(t1.geo_poly_lv95, 2056) AS new_poly
		    FROM
		        geo_afo_prod.test_lay_plz6_geo_hist t0
		    JOIN
		        geo_afo_prod.imp_plz6_geo_neu_python t1
		    ON
		        t0.plz6 = t1.plz6
		    WHERE
		        extract(year from t0.gueltig_bis) = 9999
		        AND (ST_Area(ST_Intersection(ST_SetSRID(t0.geo_poly_lv95, 2056), ST_SetSRID(t1.geo_poly_lv95, 2056))) / ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056))) < 0.995
		) u
		WHERE 
		    h.plz6 = u.plz6
		    AND extract(year from h.gueltig_bis) = 9999
	) f
;








		   
--////////////////////////////////////Experiment////////////////////////////////////////////// 		   
/*
 -- ST_Equals Funktion
 select
    t0.plz6,
    ST_SetSRID(t0.geo_poly_lv95, 2056) as old_geo_poly_lv95,
    ST_SetSRID(t1.geo_poly_lv95, 2056) as new_geo_poly_lv95
from
    tmp_plz6_hist t0
join
    geo_afo_prod.imp_plz6_geo_neu t1
on
    t0.plz6 = t1.plz6 
where 
    not ST_Equals(
        ST_SetSRID(t0.geo_poly_lv95, 2056),   -- Set SRID to 2056 for historical geometry
        ST_SetSRID(t1.geo_poly_lv95, 2056)    -- Set SRID to 2056 for new geometry
    )
group by
    t0.plz6, 
    t0.geo_poly_lv95, 
    t1.geo_poly_lv95
   ;

  
-- Area Differenz
select
    t0.plz6,
    ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056)) AS area_old,
    ST_Area(ST_SetSRID(t1.geo_poly_lv95, 2056)) AS area_new,
    ST_Area(ST_SnapToGrid(ST_SetSRID(t1.geo_poly_lv95, 2056), 5)) - ST_Area(ST_SnapToGrid(ST_SetSRID(t0.geo_poly_lv95, 2056), 5)) AS area_diff,
    ST_SetSRID(t0.geo_poly_lv95, 2056) as old_poly,
    ST_SetSRID(t1.geo_poly_lv95, 2056) as new_poly
from
    tmp_plz6_hist t0
join
    geo_afo_prod.imp_plz6_geo_neu t1
on
    t0.plz6 = t1.plz6  -- Explicit type casting for plz6
where
    ST_Area(ST_SnapToGrid(ST_SetSRID(t1.geo_poly_lv95, 2056), 25)) != ST_Area(ST_SnapToGrid(ST_SetSRID(t0.geo_poly_lv95, 2056), 25))
    --and
    --round(ST_Area(ST_SetSRID(t1.geo_poly_lv95, 2056)) - ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056))) between -10 and 10
order by
	area_diff desc
;



--
select 
    t0.plz6,
    t0.ort,
    ST_Area(t0.old_poly) as  area_old,
    ST_Area(t1.new_poly) as  area_new,
    ST_Area(t1.new_snap) - ST_Area(t0.old_snap) as  area_diff,
    t0.old_snap,
    t1.new_snap,
    t0.old_poly,
    t1.new_poly
from 
    (
        select 
            plz6,
            ort,
            ST_SetSRID(geo_poly_lv95, 2056) as  old_poly,
            ST_SnapToGrid(ST_SetSRID(geo_poly_lv95, 2056), 0.1) as  old_snap
        from 
            geo_afo_prod.mv_lay_plz6_aktuell 
    ) as t0
join 
    (
        select 
            plz6,
            ST_SetSRID(geo_poly_lv95, 2056) as  new_poly,
            ST_SnapToGrid(ST_SetSRID(geo_poly_lv95, 2056), 0.1) as  new_snap
        from 
            geo_afo_prod.imp_plz6_geo_neu
    ) as  t1
on 
    t0.plz6 = t1.plz6
where 
    ST_Area(t1.new_poly) != ST_Area(t0.old_snap)
order by 
    area_diff desc;

 
  
  
--/////////////////////////////////////////////////////////////////////

select 
    t0.plz6,
    t0.ort,
    ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056)) as  area_old,
    ST_Area(ST_SetSRID(t1.geo_poly_lv95, 2056)) as  area_new,
    ST_Area(ST_Intersection(
        ST_SetSRID(t0.geo_poly_lv95, 2056),
        ST_SetSRID(t1.geo_poly_lv95, 2056)
    )) AS area_overlap,
    ST_SetSRID(t0.geo_poly_lv95, 2056) as  old_poly,
    ST_SetSRID(t1.geo_poly_lv95, 2056) as  new_poly,
    ST_Buffer(ST_SetSRID(t0.geo_poly_lv95, 2056), 1) as  old_poly_buffered,
    ST_Buffer(ST_SetSRID(t1.geo_poly_lv95, 2056), 1) as  new_poly_buffered,
    case 
        when ST_Equals(ST_SetSRID(t0.geo_poly_lv95, 2056), ST_SetSRID(t1.geo_poly_lv95, 2056)) then 'Exactly Match'
        when ST_Equals(
                ST_SnapToGrid(ST_SetSRID(t0.geo_poly_lv95, 2056), 0.1),
                ST_SnapToGrid(ST_SetSRID(t1.geo_poly_lv95, 2056), 0.1)
             ) then 'Nearly Match'
        else 'No Match'
    end as match_status
from 
    geo_afo_prod.mv_lay_plz6_aktuell  t0
join
    geo_afo_prod.imp_plz6_geo_neu t1
on
    t0.plz6 = t1.plz6
order by
    area_overlap desc;
 */   
   
   /*
select
    t0.plz6,
    ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056)) AS area_old,
    ST_Area(ST_SetSRID(t1.geo_poly_lv95, 2056)) AS area_new,
    ST_Area(ST_Intersection(
        ST_Buffer(ST_SetSRID(t0.geo_poly_lv95, 2056), 1),
        ST_Buffer(ST_SetSRID(t1.geo_poly_lv95, 2056), 1)
    )) as area_overlap,
    ST_SetSRID(t0.geo_poly_lv95, 2056) as old_poly,
    ST_SetSRID(t1.geo_poly_lv95, 2056) as new_poly,
    ST_Buffer(ST_SetSRID(t0.geo_poly_lv95, 2056), 1) AS old_poly_buffered,
    ST_Buffer(ST_SetSRID(t1.geo_poly_lv95, 2056), 1) AS new_poly_buffered,
    case
        when ST_Equals(
                ST_SnapToGrid(ST_SetSRID(t0.geo_poly_lv95, 2056), 1),
                ST_SetSRID(t1.geo_poly_lv95, 2056)
             ) then 'Nearly Match'
        else 'No Match'
    END AS match_status
FROM
    tmp_plz6_hist t0
JOIN
    geo_afo_prod.imp_plz6_geo_neu t1
ON
    t0.plz6 = t1.plz6
ORDER BY
    area_overlap desc;
   

SELECT
    t0.plz6,
    ST_Area(ST_SetSRID(t0.geo_poly_lv95, 2056)) AS area_old,
    ST_Area(ST_SetSRID(t1.geo_poly_lv95, 2056)) AS area_new,
    ST_Area(ST_Simplify(ST_SetSRID(t0.geo_poly_lv95, 2056), 5)) AS simplified_area_old,  -- Using 5 meters tolerance
    ST_Area(ST_Simplify(ST_SetSRID(t1.geo_poly_lv95, 2056), 5)) AS simplified_area_new,  -- Using 5 meters tolerance
    ST_Intersection(
        ST_Buffer(ST_MakeValid(ST_Simplify(ST_SetSRID(t0.geo_poly_lv95, 2056), 5)), 0.01),
        ST_Buffer(ST_MakeValid(ST_Simplify(ST_SetSRID(t1.geo_poly_lv95, 2056), 5)), 0.01)
    ) AS intersection_poly,
    CASE
        WHEN ST_Intersects(
                ST_Buffer(ST_MakeValid(ST_Simplify(ST_SetSRID(t0.geo_poly_lv95, 2056), 5)), 0.01),
                ST_Buffer(ST_MakeValid(ST_Simplify(ST_SetSRID(t1.geo_poly_lv95, 2056), 5)), 0.01)
             ) THEN 'Nearly Match'
        ELSE 'No Match'
    END AS match_status
FROM
    tmp_plz6_hist t0
JOIN
    geo_afo_prod.imp_plz6_geo_neu t1
ON
    t0.plz6 = t1.plz6
ORDER BY
    simplified_area_old DESC;



   
   
   
   
   
     -- ST_Buffer(ST_SetSRID(t0.geo_poly_lv95, 2056), 1) as old_poly_buffered,
		   -- ST_Buffer(ST_SetSRID(t1.geo_poly_lv95, 2056), 1) as new_poly_buffered,
		    --case 
		      --  when ST_Equals(ST_SetSRID(t0.geo_poly_lv95, 2056), ST_SetSRID(t1.geo_poly_lv95, 2056)) then 'Exactly Match'
		       -- when ST_DWithin(ST_SetSRID(t0.geo_poly_lv95, 2056), ST_SetSRID(t1.geo_poly_lv95, 2056), 0.5) then 'Nearly Match'
		        --else 'No Match'
		    --end as match_status
   
   
   
   
   
   

  


