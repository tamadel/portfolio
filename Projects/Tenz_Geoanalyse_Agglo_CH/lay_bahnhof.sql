
DROP table if exists
	tenz.lay_bahnhof;

create table
	tenz.lay_bahnhof
as
SELECT 
	*
FROM 
	dblink(
		'geo_database',
		$POSTGRES$
			select
				poi_id
				,poi_typ 
				,company_group 
				,company
				,ort
				,geo_point_lv95 
			from
				geo_afo_prod.mv_lay_poi_aktuell 
			where
				poi_typ = 'Bahn'
		$POSTGRES$
	) AS mv_lay_poi_aktuell (
		poi_id int
		,poi_typ text
		,company_group text
		,company text
		,ort text
		,geo_point_lv95 public.geometry(point, 2056)
	)
;

select 
	*
from
	tenz.lay_bahnhof
;




