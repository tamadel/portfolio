-- google_maps_dev.google_map_metadata definition

-- Drop table

-- DROP TABLE google_maps_dev.google_map_metadata;

drop table if exists
	google_maps_dev.google_map_metadata
;

CREATE TABLE google_maps_dev.google_map_metadata (
	plz4 int8 NULL,
	keyword text NULL,
	"depth" text NULL,
	"time" text NULL,
	"cost" float8 NULL,
	id text NULL,
	datetime timestamp NULL,
	post_created_ts timestamp NULL,
	n_result int4 NULL,
	exact_match integer NULL,
	item_type text null
);

insert into
	google_maps_dev.google_map_metadata 
(
	plz4,
	keyword,
	"depth",
	"time",
	"cost",
	id,
	post_created_ts
)
select
	plz4,
	keyword ,
	"depth" ,
	time,
	"cost" ,
	id,
	post_created_ts 
from
	tmp_google_map_metadata
;

select 
	*
from 
	tmp_google_map_metadata
;

CREATE temp TABLE
	tmp_google_map_metadata
AS
SELECT
	*
FROM
	google_maps_dev.google_map_metadata
;

in



ALTER TABLE google_maps_dev_test.google_map_items SET SCHEMA google_maps_dev;
ALTER TABLE google_maps_dev_test.google_map_metadata SET SCHEMA google_maps_dev;
ALTER TABLE google_maps_dev_test.google_map_results SET SCHEMA google_maps_dev;

