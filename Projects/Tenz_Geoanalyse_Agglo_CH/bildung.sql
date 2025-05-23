
select 
	ort
	,bezeichnung 
	,category_en_ids 
	,category_de_ids 
	,url 
	,domain
from
	google_maps_dev.google_map_bildung_v1
where 
	korr_ort = 'Basel'
	--korr_plz4 = '6330'
	--'4123' --Allschwil 42
	and
	category_en_ids not in (
							'[kindergarten]'
							,'[university]'
							,'[high_school]'
							,'[preschool]'
							,'[bilingual_school | international_school | preschool | primary_school | school]'
							,'[gymnasium_school | higher_secondary_school | school | school_administrator]'
							,'[gymnasium_school]'
							,'[gymnasium_school | high_school | kindergarten | primary_school | private_school | school]'
							,'[primary_school]'
							,'[kindergarten | preschool]'
							,'[gymnasium_school | private_school | school]'
							,'[college]'
							,'[nursery_school]'
							,'[research_institute | university]'
							,'[university | university_department]'
							,'[day_care_center]' -- [Kindertagesstätte]
							,'[child_care_agency]' --[Kinderbetreuungseinrichtung]
							,'[babysitter | child_care_agency | day_care_center]'
							,'[day_care_center | kindergarten]'
							,'[child_care_agency | day_care_center | kindergarten | preschool]'
							,'[day_care_center | kindergarten | nursing_home | tutoring_service]'
							,'[coworking_space | university]'
							,'[adult_education_school | school]'
							,'[high_school | international_school | primary_school | senior_high_school]'
							,'[child_care_agency | day_care_center]'
							,'[after_school_program]'
							,'[public_university | university]'
							,'[after_school_program | child_care_agency | day_care_center]'
							,'[after_school_program | child_care_agency | day_care_center | kindergarten]'
							,'[after_school_program | child_care_agency | day_care_center | international_school | kindergarten | preschool]'
							,'[college | school]'
							,'[college | school | university]'
							,'[college | university]'
							,'[educational_institution | high_school]'
							,'[education_center | gymnasium_school | international_school | preparatory_school]'
							,'[elementary_school | high_school | international_school | nursery_school | private_college]'
							,'[english_language_school | german_language_school | gymnasium_school | language_school | tutoring_service]'
							,'[gymnasium_school | high_school]'
							,'[gymnasium_school | high_school | private_school]'
							,'[higher_education | university]'
							,'[high_school | language_school]'
							,'[high_school | primary_school]'
							,'[high_school | school]'
							,'[international_school | primary_school]'
							,'[kindergarten | language_school | music_school | primary_school | public_school]'
							,'[kindergarten | language_school | music_school | public_school | school]'
							,'[kindergarten | primary_school]'
							,'[kindergarten | private_school | school]'
							,'[learning_center | university]'
							,'[school | university]'
							,'[university | university_library]'
							,'[studying_center | university]'
							,'[research_institute | technical_school | university]'
							,'[research_institute | university | university_department]'
							,'[research_foundation | university]'
							)
	;
	
	
-- [adult_education_school] >> bringt viel schrott

-- >> Zum Testen 
-- [adult_education_school | private_school]
-- [adult_education_school | private_university]
-- [adult_education_school | technical_school]
-- [adult_education_school | training_center]
-- [adult_education_school | tutoring_service]
-- [adult_education_school | university]
-- [adult_education_school | vocational_secondary_school]
-- [adult_education_school | vocational_training_school]
-- [adult_entertainment_store | high_school] | [Sekundarschule | Sexshop]  >> Komisch "Sekundarschule Rüegsauschachen" https://www.schulen-ruegsau.ch/index.php?id=418&L=0
-- [driving_school | kindergarten | language_school | music_school | public_school | school] >> Komisch http://www.schule-baden.ch/ger/Kindergarten-Primar/Ruetihof
-- 



-- >> muss eliminieren
-- [adult_education_school | driving_school] 
-- [adult_education_school | drivers_license_training_school | motorcycle_driving_school]	
-- [adult_education_school | marketing_consultant]	
-- [adult_education_school | martial_arts_school | massage_therapist]	
-- [adult_education_school | massage_spa | wellness_center]  aber 	[adult_education_school | massage_school | massage_therapist] >> muss haben	
-- [adult_education_school | massage_therapist]
-- domin mit ".de"
						
						
select 
	ort
	,bezeichnung 
	,category_en_ids 
	,category_de_ids 
	,url 
	,domain
from
	google_maps_dev.google_map_bildung_v1
where 
	--korr_plz4 = '6330'
	--'4123' --Allschwil 42
	--and
	category_en_ids like '%[adult_education_school%'						
;
						
						
						
						
						
						
--//////////////////////////////////////////////////////////////////////////	
/*
	'[university]'
	,'[public_university | university]'
	,'[college | university]'
	)
;

--,'[after_school_program | bilingual_school | english_language_school | french_language_school | language_school | primary_school | private_school | private_tutor | students_support_association | tutoring_service]'
--,'[adult_education_school | gymnasium_school | trade_school]'
--,'[adult_education_school | education_center]'
	
	
*/	