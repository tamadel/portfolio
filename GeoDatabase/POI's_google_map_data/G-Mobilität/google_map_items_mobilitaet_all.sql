
drop table if exists
	google_maps_dev.google_map_items_mobilitaet_all;	

create table
	google_maps_dev.google_map_items_mobilitaet_all
as 
select
	*
	,ROW_NUMBER() OVER (PARTITION BY cid ORDER BY rank_absolute::INTEGER, random()) AS prio
from (
	select
		*
	from
		google_maps_dev.google_map_items_mobilität_pi
	union
	select
		*
	from
		google_maps_dev.google_map_items_mobilität_pii
	union
	select
		*
	from
		google_maps_dev.google_map_items_mobilität_piii 
	union
	select
		*
	from
		google_maps_dev.google_map_items_mobilität_piv 
) t0
;

delete from
	google_maps_dev.google_map_items_mobilitaet_all
where
	prio > 1
;


select 
	cid
	,count(*)
from
	google_maps_dev.google_map_items_mobilitaet_all
group by
	cid
having
	count(*) > 1
;

select 
	count(*)
from
	google_maps_dev.google_map_items_mobilitaet_all
;
