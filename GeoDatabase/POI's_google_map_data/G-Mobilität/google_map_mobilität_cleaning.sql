--===========================
-- First sample for Mobilität
-- 04.11.2024
--===========================

create temp table tmp_items_mobilität_agg
as
select --62,940
	*
from 
	google_maps_dev.google_map_items_mobilität
union all
select --75,333
	*
from 
	google_maps_dev.google_map_items_mobilität_pii
union all
select -- 14,498
	*
from 
	google_maps_dev.google_map_items_mobilität_piii
union all
select --58,217
	*
from 
	google_maps_dev.google_map_items_mobilität_piv	
;


--==========================
-- Creat agg Mobilität Table
-- unique "cid"
--==========================
-- get rid of dublication that happend because of same results with differents keyword search 

drop table if exists google_maps_dev_test.google_map_items_mobilität_agg;
create table google_maps_dev_test.google_map_items_mobilität_agg
as
select
    t.*
from (
    select
        m.*
        ,ROW_NUMBER() over (partition by m.cid order by m.rank_absolute, random()) as row_num
    from
        tmp_items_mobilität_agg  m
) t
where 
    t.row_num = 1
;



-- test duplication 
select
	cid
	,count(*)
from 
	google_maps_dev_test.google_map_items_mobilität_agg
group by 
	cid
having 
	count(*) > 1
;


select 
	*
from 
	google_maps_dev_test.google_map_items_mobilität_agg
;

-- Update: categories business data 
select 
	*
from
	geo_afo_prod.meta_poi_categories_business_data_aktuell
where
	--hauptkategorie_neu = 'Mobilität'
	category_id in ('car_detailing_service', 'car_manufacturer', 'car_factory', 'department_of_motor_vehicles', 
					 'auto_wrecker', 'auto_radiator_repair_service', 'used_tire_shop', 'taxi_service', 
					 'car_battery_store', 'truck_repair_shop', 'tuning_automobile', 'vehicle_wrapping_service', 
					 'towing_service', 'rv_repair_shop', 'auto_upholsterer', 'auto_market', 'motor_scooter_repair_shop', 
					 'motorcycle_rental_agency', 'car_inspection_station', 'auto_broker', 'driveshaft_shop', 
					 'race_car_dealer', 'self_service_car_wash', 'transmission_shop', 'auto_parts_manufacturer', 
					 'auto_parts_market', 'drivers_license_training_school', 'tesla_showroom', 'auto_auction', 
					 'car_leasing_service', 'engine_rebuilding_service', 'rv_detailing_service', 'mechanic', 
					 'racing_car_parts_store', 'motorcycle_repair_shop', 'motorcycle_shop', 'car_stereo_store', 
					 'auto_machine_shop', 'valet_parking_service', 'automotive', 'parking_lot_for_motorcycle', 
					 'taxis', 'trailer_repair_shop', 'trailer_manufacturer', 'trailer_dealer', 'trailer_rental_service', 
					 'motorcycle_parts_store', 'auto_accessories_wholesaler', 'electric_vehicle_charging_station_contractor', 
					 'showroom', 'tune_up_supplier', 'car_service', 'limousine_service', 'race_car_dealer', 
					 'bus_company', 'bus_tour_agency', 'taxi_stand', 'atv_dealer', 'forklift_dealer', 
					 'motoring_club', 'utility_trailer_dealer'
	)
;

update geo_afo_prod.meta_poi_categories_business_data_aktuell
set 
	hauptkategorie_neu = 'Mobilität'
where 
	category_id in ('car_detailing_service', 'car_manufacturer', 'car_factory', 'department_of_motor_vehicles', 
					 'auto_wrecker', 'auto_radiator_repair_service', 'used_tire_shop', 'taxi_service', 
					 'car_battery_store', 'truck_repair_shop', 'tuning_automobile', 'vehicle_wrapping_service', 
					 'towing_service', 'rv_repair_shop', 'auto_upholsterer', 'auto_market', 'motor_scooter_repair_shop', 
					 'motorcycle_rental_agency', 'car_inspection_station', 'auto_broker', 'driveshaft_shop', 
					 'race_car_dealer', 'self_service_car_wash', 'transmission_shop', 'auto_parts_manufacturer', 
					 'auto_parts_market', 'drivers_license_training_school', 'tesla_showroom', 'auto_auction', 
					 'car_leasing_service', 'engine_rebuilding_service', 'rv_detailing_service', 'mechanic', 
					 'racing_car_parts_store', 'motorcycle_repair_shop', 'motorcycle_shop', 'car_stereo_store', 
					 'auto_machine_shop', 'valet_parking_service', 'automotive', 'parking_lot_for_motorcycle', 
					 'taxis', 'trailer_repair_shop', 'trailer_manufacturer', 'trailer_dealer', 'trailer_rental_service', 
					 'motorcycle_parts_store', 'auto_accessories_wholesaler', 'electric_vehicle_charging_station_contractor', 
					 'showroom', 'tune_up_supplier', 'car_service', 'limousine_service', 'race_car_dealer', 
					 'bus_company', 'bus_tour_agency', 'taxi_stand', 'atv_dealer', 'forklift_dealer', 
					 'motoring_club', 'utility_trailer_dealer','shipyard', 'boat_builder', 'boating_instructor', 'boat_club',
					 'boat_accessories_supplier', 'bus_and_coach_company', 'bus_charter', 'car_battery_store', 'truck_accessories_store',
					 'electric_motor_store', 'bicycle_wholesale', 'bicycle_repair_shop', 'driving_school', 'wheel_store', 'gas_company',
					 'motorcycle_shop', 'used_auto_parts_store', 'transit_station', 'heating_oil_supplier', 'machine_workshop', 'motorcycle_driving_school',
					 'scooter_repair_shop', 'motorsports_store', 'diesel_engine_repair_service', 'electric_motor_repair_shop', 'sailing_school', 'shipping_service',
					 'tractor_repair_shop', 'transportation_service', 'shipping_company', 'bicycle_store', 'aquatic_center', 'mobile_home_dealer', 'auto_tune_up_service',
					 'railway_services', 'transportation_escort_service', 'parking_lot_for_bicycles', 'vehicle_shipping_agent', 'electric_bicycle_store', 'truck_topper_supplier', 
					 'mobility_equipment_supplier', 'trailer_supply_store', 'bus_ticket_agency', 'trucking_company', 'minibus_taxi_service', 'machining_manufacturer', 'bike_wash',
					 'atv_rental_service', 'auto_glass_shop', 'auto_bodywork_mechanic', 'truck_topper_supplier', 'railroad_equipment_supplier', 'mobility_equipment_supplier'
	)
;	



--filiter with all elements in category_ids
drop table if exists google_maps_dev_test.google_map_mobilität_agg;
create table google_maps_dev_test.google_map_mobilität_agg
as
select 
	*
from 
	google_maps_dev_test.google_map_items_mobilität_agg t0
where exists (
    select 
    	*
    from 
    	geo_afo_prod.meta_poi_categories_business_data_aktuell as t1
    where
    	t0.category_ids @> to_jsonb(t1.category_id)::jsonb
    	and 
    	t1.hauptkategorie_neu = 'Mobilität'
)
 ;



-- filiter with the first elemnt only in category_ids
drop table if exists google_maps_dev_test.google_map_mobilität_agg_v1;
create table google_maps_dev_test.google_map_mobilität_agg_v1
as
select 
	*
from 
	google_maps_dev_test.google_map_items_mobilität_agg t0
where exists (
    select 
    	1
    from 
    	geo_afo_prod.meta_poi_categories_business_data_aktuell as t1
    where
    	t0.category_ids->> 0 = t1.category_id
    	and 
    	t1.hauptkategorie_neu = 'Mobilität'
)
;


select -- filiter with all elements 
	--distinct category_ids->> 0
	*
from 
	google_maps_dev_test.google_map_mobilität_agg
where 
	cid not in ( -- filiter with the first element
				select 
					cid
				from
					google_maps_dev_test.google_map_mobilität_agg_v1
	)
;


select 
	*
from
	geo_afo_prod.meta_poi_categories_business_data_aktuell
where
	hauptkategorie_neu = 'Mobilität'
;



-- TEST
select 
	*
from 
	google_maps_dev_test.google_map_mobilität_agg
where 
	cid in (
			select  
			    t0.cid
			    --t0.title,
			    --t0.category_ids,
			    --t1.category_id as mobility_category_id,
			    --t1.category_de as mobility_category_name
			from  
			    google_maps_dev_test.google_map_mobilität_agg  t0
			join   
			    geo_afo_prod.meta_poi_categories_business_data_aktuell  t1
			on  
			    t0.category_ids @> to_jsonb(t1.category_id::text)::jsonb
			    and
			   	t1.hauptkategorie_neu = 'Mobilität'
	)
;







select 
	*
from 
	geo_afo_prod.meta_poi_categories_business_data_aktuell
;


















--==============================================================================
-- Category_id: anfalten
--==============================================================================
drop table if exists tmp_mobilität_category_ids;
create temp table tmp_mobilität_category_ids
as
select 
	cid,
	split_part(keyword,' ',1) as keyword,
	category,
	category_ids,
	jsonb_array_elements_text(category_ids::jsonb) as category_id
from 
	 google_maps_dev_test.google_map_mobilität_agg
where 
	jsonb_typeof(category_ids::jsonb) = 'array'
;
	

--Mobilität Category_ids matrix Tabelle
select  
    a.category_id as category_id_1,
    b.category_id as category_id_2,
    COUNT(*) as pair_count
from  
    tmp_mobilität_category_ids AS a
join  
    tmp_mobilität_category_ids AS b
on  
    a.cid = b.cid and a.category_id < b.category_id
--where 
	--a.category_id = 'restaurant'
group by  
    a.category_id, b.category_id
order by  
    pair_count desc
;


select 
	--cid
	count(*)
	,category_id
	--,count(category_ids)
	--,category_ids
	--,count(cid)
from
	tmp_mobilität_category_ids
group by
	category_id
order by 
	count(*) desc
--having 
	--ount(category_ids) > 5
;
	
--=====================================================================================================	
	

select
	cid
	,count(*)
from 
	google_maps_dev_test.google_map_mobilität_agg
group by 
	cid
having 
	count(*) > 1
;


select 
	distinct poi_typ
from 
	google_maps_dev_test.google_map_mobilität_agg
;



select distinct 
	category 
	--,category_ids
from 
	google_maps_dev_test.google_map_mobilität_agg
where 
	category not in(
					select 
						distinct poi_typ
					from 
						google_maps_dev_test.google_map_mobilität_agg
	)
;



--===================================================================
-- Cleaning the table >> get rid of irrelevant category
--===================================================================
-- Part(1) -- get rid of "Spenglerei" mapped to "Dachdecker"
select  
    *
from  
    google_maps_dev_test.google_map_mobilität_agg
where  
    --category like '%Spenglerei%'
    --and  
    '["convenience_store"]'::jsonb @> category_ids
;

delete from google_maps_dev_test.google_map_mobilität_agg
where 
	category like '%Spenglerei%'
    and  
    '["Dachdecker"]'::jsonb @> additional_categories
;



select  
    *
from  
    google_maps_dev_test.google_map_mobilität_agg
where  
    category like '%Dachdecker%'
;

delete from google_maps_dev_test.google_map_mobilität_agg
where 
	category like '%Dachdecker%'
;




-- irrelevant category
select 
	keyword
	,title
	,category
	,category_ids
from 
	google_maps_dev_test.google_map_mobilität_agg
where
	category in (
				    'Agenzia delle dogane',
				    'Allround-Handwerker',
				    'Amtlich zugelassener Buch- und Rechnungsprüfer',
				    'Apartmenthaus',
				    'Architekt',
				    'Arzt',
				    'Auktionshaus',
				    'Aussichtsplattform',
				    'Bar',
				    'Bastelgeschäft',
				    'Baufachhandel',
				    'Bedachungsfachhandel',
				    'Bed and Breakfast',
				    'Behörde',
				    'Bekleidungsgeschäft',
				    'Beleuchtungsgeschäft',
				    'Berater',
				    'Bistro',
				    'Blumenmarkt',
				    'Buchhaltungsdienst',
				    'Büromöbelgeschäft',
				    'Business-Networking-Unternehmen',
				    'Business Park',
				    'Campingladen',
				    'Catering',
				    'Chalet',
				    'Chemiewerk',
				    'Cheminéebauer',
				    'Coaching',
				    'Computer',
				    'Computergeschäft',
				    'Computerservice',
				    'Dachdecker',
				    'Digitaldruckerei',
				    'E-Commerce-Dienst',
				    'Elektriker',
				    'Energieanbieter',
				    'Fachgeschäft für Fahnen',
				    'Fachgeschäft für Glasartikel',
				    'Ferienhaus',
				    'Ferienhausvermietung',
				    'Ferienwohnung',
				    'Finanzberater',
				    'Fotogeschäft',
				    'Fußpflege',
				    'Garten',
				    'Gartenbauer',
				    'Gartencenter',
				    'Grafikdesigner',
				    'Graveur',
				    'Gürtelgeschäft',
				    'Hausmeisterservice',
				    'Hausreinigungsdienst',
				    'Hausverwaltungsunternehmen',
				    'Herrenmodengeschäft',
				    'Hochzeitsservice',
				    'Imbiss',
				    'Immobilienbüro',
				    'Immobilienmakler',
				    'Jalousiengeschäft',
				    --'Karosserieteileanbieter',
				    'Käseladen',
				    'Kosmetiker',
				    'Kosmetikgeschäft',
				    'Kosmetikstudio',
				    'Künstler',
				    'Lageranbieter',
				    'Landschaftsgestalter',
				    'Lebensmittelanbieter',
				    'Lebensmittelhändler',
				    'Marketingbüro',
				    --'Markt für Autoteile',
				    'Möbeltischler',
				    'Mobiltelefongeschäft',
				    'Papeterie',
				    'Pub',
				    'Restaurant',
				    'Schlüsseldienst',
				    'Schuhmacher',
				    'Schweizerisch',
				    'Softwareunternehmen',
				    'Supermarkt',
				    'Tabakladen',
				    'Thailändisch',
				    'Touristen Informationszentrum',
				    'Unternehmen für Isolationsarbeiten',
				    'Unternehmensberater',
				    'Veranstaltungsraum',
				    'Verleih von Sportausrüstung',
				    --'Versicherungsagentur',
				    'Wechselstube',
				    'Werbeagentur',
				    --'Werkzeuggeschäft',
				    'Zimmermann'
	);


-- Keyword that retreave irrelevant data ("Tankstelle",  )
-- category_id filiter that bringt irrlevant data ("gas_station", "free_parking_lot", "public_parking_space", "truck_stop", "border_crossing_station", "parking_lot", "parking_garage", )


   
   
   
   
   
   
   
	


