--=================================
-- Project: layer and attr. update
-- Date:
-- update:
--=================================
-- Gmd Data

--geo_afo_prod.meta_attribute_hist
--geo_afo_prod.lay_gmd_attr_hist

-- anz_attr
select 
	distinct attr_id
	,attr_name 
	,attr_desc 
from
	geo_afo_prod.meta_attribute_hist
where 
	extract(year from gueltig_bis) = '9999'
;