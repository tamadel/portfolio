
DROP table if exists
	tenz.lay_str_freq;

create table
	tenz.lay_str_freq
as
SELECT 
	*
FROM 
	dblink(
		'geo_database',
		$POSTGRES$
			select
				gid 
				,str_id 
				,dtv_alle 
				,case
					when dtv_alle = 0 then 0
					else
						ntile(4) over (
							order by
								dtv_alle
						)
				end as dtv_alle_grp
				,geo_line_lv95
			from
				geo_afo_prod.mv_lay_str_freq_aktuell 
		$POSTGRES$
	) AS mv_lay_str_freq_aktuell (
		gid int
		,str_id int
		,dtv_alle float
		,dtv_alle_grp int
		,geo_line_lv95 public.geometry(multilinestring, 2056)
	)
;

select 
	*
from
	tenz.lay_str_freq
;
