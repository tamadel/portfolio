--====================================
-- Category view with business data
--====================================

select  
	*
from  
	google_maps_dev.google_map_category_hierarchy
;

select
	*
from 
	google_maps_dev.categories_business_data 
;


select 
	*
from 
	geo_afo_prod.meta_poi_category_new_hist
;


-- join categories business data with our category hierarchy
drop table if exists geo_afo_prod.meta_poi_categories_business_data;
create table geo_afo_prod.meta_poi_categories_business_data  
as
select
	category_name as google_kategorie
	,category_en 
	,category_de 
	,hauptkategorie_neu as hauptkategorie
	,kategorie_neu as kategorie
	,poi_typ_neu as poi_typ
	,kategorie_alt
	,poi_typ_alt
from (
		select
			t1.*
			,t0.*
		from
			google_maps_dev.categories_business_data t0
		left join
			google_maps_dev.google_map_category_hierarchy t1
		on
			t0.category_name = t1.category_en 
	) gb
;


select 
	*
from 
	geo_afo_prod.meta_poi_categories_business_data_aktuell  
where 
	afo_kategorie_en is not null 
;

select 
	count(*)
from
	geo_afo_prod.mv_lay_plz4_aktuell 
;



------------------------------------------------------------
-- categories_business_data_listings_afo_poi_typ ph_changes
-- changes by Peter
------------------------------------------------------------


select distinct
	hauptkategorie_neu 
	,kategorie_neu 
	,poi_typ_neu 
from 
	geo_afo_prod.meta_poi_categories_business_data_aktuell 
where 
	hauptkategorie_neu = 'Gesundheit'
;


select
	* 
from
	geo_afo_prod.meta_poi_categories_business_data_aktuell  
where
	hauptkategorie_neu = 'Hotel & Gastronomie'
	--category_id like '%_restaurant%'
;


update  
    geo_afo_prod.meta_poi_categories_business_data_aktuell
set  
    hauptkategorie_neu = case  
                            when hauptkategorie_neu is null  
                            then 'Hotel & Gastronomie'
                            else hauptkategorie_neu 
                         end, 
    kategorie_neu = case 
                        when kategorie_neu is null  
                        then 'Restaurant'
                        else kategorie_neu 
                    end,
    poi_typ_neu = case 
                    when poi_typ_neu is null 
                    then 'Restaurant'
                    else poi_typ_neu 
                  end,
    kategorie_alt = case 
                    when kategorie_alt is null 
                    then 'Verpflegung'
                    else kategorie_alt 
                   end,
    poi_typ_alt = case 
                    when poi_typ_alt is null 
                    then 'Restaurant'
                    else poi_typ_alt 
                  end 
where  
    category_id like '%_restaurant%'
   ;

	
			
  
select 
	*
from
	geo_afo_prod.meta_poi_categories_business_data_aktuell	
where 
	category_id in (
					'bistro'
					,'coffee_shop'
					,'espresso_bar'
					,'breakfast_restaurant'
					,'childrens_cafe'
	)
;






select 
	*
from
	geo_afo_prod.meta_poi_categories_business_data_aktuell	
where 
	category_id like '%service restaurant%'
;
	
	
	

select 
	*
from 
	geo_afo_prod.meta_poi_google_maps_category
where 
	--hauptkategorie_neu = 'Gesundheit'
	kategorie_neu like '%Self service restaurant%'
;



select  
	*
from  
	google_maps_dev.google_map_category_hierarchy
where 
	kategorie_neu like '%Self service restaurant%'
;




