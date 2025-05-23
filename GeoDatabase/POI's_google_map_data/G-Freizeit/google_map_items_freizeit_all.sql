
drop table if exists
	google_maps_dev.google_map_items_freizeit_all;	

create table
	google_maps_dev.google_map_items_freizeit_all
as 
select
	*
	,ROW_NUMBER() OVER (PARTITION BY cid ORDER BY rank_absolute::INTEGER, random()) AS prio
from (
	select
		*
	from
		google_maps_dev.google_map_items_freizeit_pi
	union
	select
		*
	from
		google_maps_dev.google_map_items_freizeit_pii
	union
	select
		*
	from
		google_maps_dev.google_map_items_freizeit_piii 
) t0
;

delete from
	google_maps_dev.google_map_items_freizeit_all
where
	prio > 1
;


select 
	cid
	,count(*)
from
	google_maps_dev.google_map_items_freizeit_all
group by
	cid
having
	count(*) > 1
;

select 
	count(*)
from
	google_maps_dev.google_map_items_freizeit_all
;
