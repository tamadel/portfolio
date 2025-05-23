
drop table if exists
	google_maps_dev.google_map_items_gesundheit_all;	

create table
	google_maps_dev.google_map_items_gesundheit_all
as 
select
	*
	,ROW_NUMBER() OVER (PARTITION BY cid ORDER BY rank_absolute::INTEGER, random()) AS prio
from (
	select
		*
	from
		google_maps_dev.google_map_items_gesundheit_pi
	union
	select
		*
	from
		google_maps_dev.google_map_items_gesundheit_pii
	union
	select
		*
	from
		google_maps_dev.google_map_items_gesundheit_piii 
) t0
;

delete from
	google_maps_dev.google_map_items_gesundheit_all
where
	prio > 1
;


select 
	cid
	,count(*)
from
	google_maps_dev.google_map_items_gesundheit_all
group by
	cid
having
	count(*) > 1
;

	