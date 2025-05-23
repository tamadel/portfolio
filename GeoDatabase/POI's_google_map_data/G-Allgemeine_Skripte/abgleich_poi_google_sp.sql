-------------------------------------------------------------------------------------------
-- Kunde: AFO
-- Thema: Kontrolle POI Liste
-- Datum: 148.06.2024
-- Autor: Tamer Adel 
-- DB: geo_database
-- Schema: geo_afo_prod
-------------------------------------------------------------------------------------------
--<Hauptkategorie Name>
--------------------------------------------------------------------
-- Daten sichten
---------------------------------------------------------------------

-- Testing

CALL google_maps_dev_abgleich.sp_abgleich_poi_google_sp('google_maps_dev_abgleich.afo_poi_typ_food','google_maps_dev_abgleich.google_abgleich_food','google_maps_dev_abgleich.food_test_sp')

SELECT 
	* 
FROM 
	google_maps_dev_abgleich.food_test_sp;


-----------------
-- ANSCHAUEN DER 2 STARTTABELLEN ZUM VERGLEICH
-----------------

SELECT * FROM google_maps_dev_abgleich.google_abgleich_food;


SELECT 
	* 
FROM 
	google_maps_dev_abgleich.food_test_sp 
WHERE 
	lower(company) LIKE '%coop%'
AND 
	dubletten_nr IS NULL
;

------ Coop Testingground

DROP TABLE IF EXISTS 
	google_maps_dev_abgleich.coop_test_sp;

CREATE TABLE
	google_maps_dev_abgleich.coop_test_sp 
AS
SELECT 
	*,
	'Coop'	
FROM 
	google_maps_dev_abgleich.food_test_sp 
WHERE 
	lower(company) LIKE '%coop%'
	AND 
	lower(company) NOT LIKE '%pronto%'
	AND 
	lower(company) LIKE 'coop%'
	AND 
	(lower(company) LIKE 'coop %' OR lower(company) = 'coop')
	AND 
	lower(company) NOT LIKE '%city%'
	AND
	quelle = 'GOOGLE'
	AND 
	(
		url IS NOT NULL 
		OR 
		dubletten_nr IS NOT NULL
	)
	AND 
	Lower(company) NOT LIKE '%kiosk%'
	AND 
	lower(company) NOT LIKE '%restaurant%'
;

INSERT INTO 
	google_maps_dev_abgleich.coop_test_sp 
SELECT
	*,
	'Coop'
FROM 
	google_maps_dev_abgleich.food_test_sp 
WHERE 
	lower(company) LIKE '%coop%'
	AND 
	lower(company) NOT LIKE '%pronto%'
	AND 
	lower(company) LIKE 'coop%'
	AND 
	(lower(company) LIKE 'coop %' OR lower(company) = 'coop')
	AND 
	quelle = 'AFO'
	AND 
	lower(company) NOT LIKE '%city%'
	AND 
	lower(company) NOT LIKE '%hc%'
;


SELECT 
	* 
FROM 
	google_maps_dev_abgleich.coop_test_sp
WHERE 
	dubletten_nr IS NULL
;



--------------------------------------
-- Testing der Matching Algorithmen
--------------------------------------

-- Removed tokens

CREATE TABLE public.test_removed_tokens (
    token TEXT PRIMARY KEY -- Each token should be unique
);

INSERT INTO public.test_removed_tokens (token) VALUES
('de'),
('fr'),
('standorte'),
('standort'),
('magasins'),
('supermarche')
;

-- Funktion zum URL Matching

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Function to compute similarity scores between two URLs
CREATE OR REPLACE FUNCTION url_similarity(url1 TEXT, url2 TEXT)
RETURNS INT AS $$
DECLARE
    domain1 TEXT;
    domain2 TEXT;
    tokens1 TEXT[];
    tokens2 TEXT[];
    filtered_tokens1 TEXT[] := '{}';
    filtered_tokens2 TEXT[] := '{}';
    remove_tokens TEXT[];
    highest_similarity FLOAT;
    token1 TEXT;
    token2 TEXT;
    main_url_similarity FLOAT;
    main_score INT := 0;
    token_score INT := 0;
	total_score INT := 0;
BEGIN

	url1 := REGEXP_REPLACE(url1, '^https?://', '');
	url2 := REGEXP_REPLACE(url2, '^https?://', '');

    -- Extract main domains (remove www and top-level domain)
    domain1 := regexp_replace(regexp_replace(regexp_replace(url1, 'https?://', ''), '^www\.', ''), '/.*$', '');
    domain2 := regexp_replace(regexp_replace(regexp_replace(url2, 'https?://', ''), '^www\.', ''), '/.*$', '');

    -- Compute similarity for main domain only
    main_url_similarity := similarity(domain1, domain2);

    -- Set main score based on similarity threshold
    IF main_url_similarity > 0.5 THEN
        main_score := 1;
    END IF;

    -- Tokenize by splitting only the part after the domain and before the query parameters
    tokens1 := regexp_split_to_array(split_part(url1, '?', 1), '/');
    tokens2 := regexp_split_to_array(split_part(url2, '?', 1), '/');

    -- Remove tokens before and including the first slash (domain part)
    filtered_tokens1 := tokens1[2:array_length(tokens1, 1)];
    filtered_tokens2 := tokens2[2:array_length(tokens2, 1)];

    -- Fetch tokens to be removed from schema.removed_tokens
    SELECT array_agg(token) INTO remove_tokens FROM public.test_removed_tokens;

    -- Filter tokens by excluding remove tokens
    filtered_tokens1 := ARRAY(SELECT token FROM unnest(filtered_tokens1) token WHERE token <> '' AND NOT (token = ANY(remove_tokens)));
    filtered_tokens2 := ARRAY(SELECT token FROM unnest(filtered_tokens2) token WHERE token <> '' AND NOT (token = ANY(remove_tokens)));

    -- Compare each token in filtered_tokens1 with filtered_tokens2
    FOREACH token1 IN ARRAY filtered_tokens1 LOOP
        highest_similarity := 0;
        FOREACH token2 IN ARRAY filtered_tokens2 LOOP
            highest_similarity := GREATEST(highest_similarity, similarity(token1, token2));
            RAISE NOTICE 'Token1: %, Token2: %, Similarity: %', token1, token2, highest_similarity;
        END LOOP;
        IF highest_similarity > 0.7 THEN
            token_score := 1;
            EXIT; -- Early exit as we only need one match to set token_score
        END IF;
    END LOOP;

    -- Calculate total score
    total_score := main_score + token_score;

    RETURN total_score;
END;
$$ LANGUAGE plpgsql;

-- Example usage:
-- SELECT * FROM url_similarity('https://www.coop.ch/fr/magasins/coop-supermarche-lutry-la-corniche/5072_POS',
--                             'https://www.coop.ch/de/standorte/coop-supermarche-lutry-la-corniche/5072_POS?lat=46.5058405&long=6.6884589',                             0.5);

SELECT 
	* 
FROM 
	google_maps_dev_abgleich.coop_test_sp
WHERE 
	dubletten_nr IS NULL
;

-- Ansatz mit Dubletten Nr NULL und ort aber gleich und URL gleich (für Brands)

DROP TABLE IF EXISTS 
	public.test_new_matches;

CREATE TABLE 
	public.test_new_matches
AS
SELECT 
	a.poi_id, 
	b.poi_id AS cid
FROM 
	( 
		SELECT 
			* 
		FROM 
			google_maps_dev_abgleich.coop_test_sp
		WHERE 
			dubletten_nr IS NULL 
			AND 
			quelle = 'AFO'
	) a 
LEFT JOIN 
	( 
		SELECT 
			* 
		FROM 
			google_maps_dev_abgleich.coop_test_sp
		WHERE 
			dubletten_nr IS NULL 
			AND 
			quelle = 'GOOGLE'
	) b 
ON 
	a.ort = b.ort AND public.url_similarity(a.url,b.url) = 2;


SELECT * FROM public.test_new_matches;

-- ID hinzufügen

SELECT 
	* 
FROM 
	google_maps_dev_abgleich.coop_test_sp
WHERE 
	poi_id IN ('846','122477567923872878');


SELECT public.url_similarity('https://www.coop.ch/fr/magasins/coop-superzzzzzzmarche-lufeewferwfefewfsefetry-la-corniche/507ewfwe2_POS','https://www.coop.ch/de/standorte/coop-supermarche-lutry-la-corniche/5072_POS?lat=46.5058405&long=6.6884589');
	
-- Testing der URL Similarity Funktion

SELECT public.url_similarity('https://www.lola.ch/fdgs/de/heidelberg','https://www.lolas.ch/stanfgfscshhdorte/dedddddssd/regensdcdcscor');











----------------------------------------
-- Modifizierte Stored Procedure
----------------------------------------





CREATE OR REPLACE PROCEDURE 
	google_maps_dev_abgleich.sp_abgleich_poi_google_sp
(
	afo_poi_input_table text,
	google_poi_input_table text,
	abgleich_output_table text
)
LANGUAGE 
	plpgsql
AS $$

	declare 
		cmd varchar(1000);

	begin

		--------------------------------------------------------------------
		-- afo_poi_input_table in tmp-Tabelle abfüllen
		---------------------------------------------------------------------
		cmd := '
			drop table if exists
				tmp_afo_poi_input;
	
			create temp table
				tmp_afo_poi_input
			as
			select
				*
			from
				' || afo_poi_input_table || '
			;
		'
		;
		execute cmd;

		--------------------------------------------------------------------
		-- google_poi_input_table in tmp-Tabelle abfüllen
		---------------------------------------------------------------------
		cmd := '
			drop table if exists
				tmp_google_poi_input;
	
			create temp table
				tmp_google_poi_input
			as
			select
				*
			from
				' || google_poi_input_table || '
			;
		'
		;
		execute cmd;

		--------------------------------------------------------------------
		-- 1. Daten für Ableich stamdardisieren
		---------------------------------------------------------------------
		
		-- Poi-Adressen standardisieren
		drop table if exists 
			tmp_strasse;
		
		create temp table 
			tmp_strasse
		as
		select *,
			public.strasse_extract_strasse(coalesce(adresse,'')) as strasse,
			public.strasse_extract_hnr(coalesce(adresse,'')) as hnr,
			row_number() over (order by poi_id, random()) as id
		from 
			tmp_afo_poi_input
		;
		
		update 
			tmp_strasse 
		set
			strasse = substring(strasse,1,regexp_instr(STRASSE, '[0-9][^ ]*')-1)
		where 
			regexp_instr(STRASSE, '[0-9][^ ]*')+4>=length(strasse) 
			and 
			hnr<>'' 
			and 
			regexp_instr(STRASSE, '[0-9][^ ]*')>0
		;
		
		update 
			tmp_strasse
		set
			strasse = trim(trim(substring(strasse, regexp_instr(STRASSE, ',[^ ]*')+1,255))||' '||substring(strasse, 1, regexp_instr(STRASSE, ',[^ ]*')-1))
		where 
			strasse like '%,%'
		;
		
		
		drop table if exists 
			tmp_poi_mch;
		
		create temp table 
			tmp_poi_mch
		as
		select 
			t1.*,
			public.clean_firma(coalesce(company,'')) as firma_std,
			public.clean_firma_ohne_phon(coalesce(company,'')) as firma_std_ohne_phon,
		  	public.strassen_sufix(strasse) as str_sufix,
		  	public.strassen_prefix(strasse) as str_prefix,
		  	public.phon_mch_strasse(strasse) as strasse_std,
		  	public.clean_name(public.phon_mch_strasse(strasse),1) as strasse_std1,
		  	public.phon_mch_hnr(hnr) as hnr_std,
			coalesce(t2.plz_grob, t1.plz4) as plz_grob
		from 
			tmp_strasse t1
		left join (
			select
				postleitzahl 			as plz
				,min(gplz)				as plz_grob
			from
				geo_afo_prod.qu_geopost_01_new_plz1_hist 
			where
				extract(year from gueltig_bis) = 9999
			group by
				plz
		) t2 
		on 
			t1.plz4 = t2.plz
		;
		
		update 
			tmp_poi_mch
		set
			firma_std_ohne_phon = replace(replace(firma_std_ohne_phon,'’',''),chr(39),'')
		;
		
		-- Google-Adressen standardisieren
		drop table if exists 
			tmp_strasse;
		
		create temp table 
			tmp_strasse 
		as
		select 
			*,
			public.strasse_extract_strasse(coalesce(strasse,'')) as strasse_ber,
			public.strasse_extract_hnr(coalesce(strasse,'')) as hnr
		from 
			tmp_google_poi_input
		;
		
		update 
			tmp_strasse
		set
			strasse_ber = substring(strasse,1,regexp_instr(strasse_ber, '[0-9][^ ]*')-1)
		where 
			regexp_instr(strasse_ber, '[0-9][^ ]*')+4>=length(strasse_ber) 
			and 
			hnr<>'' 
			and 
			regexp_instr(strasse_ber, '[0-9][^ ]*')>0
		;
		
		update 
			tmp_strasse 
		set
			strasse_ber = trim(trim(substring(strasse_ber, regexp_instr(strasse_ber, ',[^ ]*')+1,255))||' '||substring(strasse_ber, 1, regexp_instr(strasse_ber, ',[^ ]*')-1))
		where 
			strasse_ber like '%,%'
		;
		
		update 
			tmp_strasse 
		set
			plz4 = 0
		where 
			length(plz4::INT::TEXT) <> 4                      
		;
		
		update 
			tmp_strasse 
		set
			plz4 = trim(substring(address, 1, regexp_instr(address, ' [^ ]*')-1))::int
		where 
			plz4 = 0 
			and 
			substring(address,1,1) in ('1','2','3','4','5','6','7','8','9') 
			and 
			length(trim(substring(address, 1, regexp_instr(address, ' [^ ]*')-1))) = 4                  
		;
		
		drop table if exists 
			tmp_google_mch;
		
		create temp table 
			tmp_google_mch 
		as
		select 
			t1.*,
			public.clean_firma(coalesce(title,'')) as firma_std,
			public.clean_firma_ohne_phon(coalesce(title,'')) as firma_std_ohne_phon,
		  	public.strassen_sufix(coalesce(strasse_ber,'')) as str_sufix,
		  	public.strassen_prefix(coalesce(strasse_ber,'')) as str_prefix,
		  	public.phon_mch_strasse(coalesce(strasse_ber,'')) as strasse_std,
		  	case 
		  		when length(trim(coalesce(strasse_ber,''))) > 1 then public.clean_name(public.phon_mch_strasse(coalesce(strasse_ber,'')),1)
		  	end as strasse_std1,
		  	public.phon_mch_hnr(coalesce(hnr,'')) as hnr_std,
			coalesce(t2.plz_grob, t1.plz4) as plz_grob
		from 
			tmp_strasse t1
		left join (
			select
				postleitzahl 			as plz
				,min(gplz)				as plz_grob
			from
				geo_afo_prod.qu_geopost_01_new_plz1_hist 
			where
				extract(year from gueltig_bis) = 9999
			group by
				plz
		) t2 
		on 
			t1.plz4 = t2.plz
		;
		
		update 
			tmp_google_mch 
		set
			firma_std_ohne_phon = replace(replace(firma_std_ohne_phon,'’',''),chr(39),'')
		;
		
		
		--------------------------------------------------------------------
		-- 2. Abgleich
		---------------------------------------------------------------------
		
		-- Abgleich
		drop table if exists 
			tmp_abgleich_poi;
		
		create temp table 
			tmp_abgleich_poi
		as
		SELECT 
			a.*, 
			b.cid, 
			1 as qual
		FROM 
			tmp_poi_mch AS a 
		LEFT JOIN 
			tmp_google_mch AS b
		ON 
			(
				a.strasse_std = b.strasse_std 
				AND 
				a.strasse_std <> '' 
				AND 
				a.hnr_std = b.hnr_std 
				AND 
				a.plz_grob = b.plz_grob 
				and 
				length(a.plz_grob::text) > 2
			)
			and
			--public.token_set_similarity(a.firma_std_ohne_phon, b.firma_std_ohne_phon) > 0.85
			-- similarity(a.firma_std_ohne_phon,b.firma_std_ohne_phon) > 0.3
			public.fuzzy_token_set_ratio(a.firma_std_ohne_phon,b.firma_std_ohne_phon)>85
			and 
			a.firma_std_ohne_phon <> ''
		WHERE 
			b.cid IS NOT null
		;
		
		insert into 
			tmp_abgleich_poi
		SELECT 
			a.*, 
			b.cid, 
			1 as qual
		FROM 
			tmp_poi_mch AS a 
		LEFT JOIN 
			tmp_google_mch AS b
		ON 
			(
				a.strasse_std = b.strasse_std 
				AND 
				a.strasse_std <> '' 
				AND 
				a.hnr_std = b.hnr_std 
				AND 
				a.plz_grob = b.plz_grob 
				and 
				length(a.plz_grob::text) > 2
				and 
				--public.partial_ratio(a.firma_std_ohne_phon, b.firma_std_ohne_phon) > 0.85
				-- similarity(a.firma_std_ohne_phon,b.firma_std_ohne_phon) > 0.3 
				public.fuzzy_partial_ratio(a.firma_std_ohne_phon, b.firma_std_ohne_phon)>85
				and 
				a.firma_std_ohne_phon <> ''
			)
		WHERE 
			b.cid IS NOT null
		;
		
		insert into 
			tmp_abgleich_poi
		SELECT 
			a.*, 
			b.cid, 
			3 as qual
		FROM 
			tmp_poi_mch AS a 
		LEFT JOIN 
			tmp_google_mch AS b
		ON 
			(
				a.strasse_std = b.strasse_std 
				AND 
				a.strasse_std <> '' 
				AND 
				a.hnr_std = b.hnr_std 
				AND 
				a.plz_grob = b.plz_grob 
				and 
				length(a.plz_grob::text) > 2
			)
			and 
			a.firma_std = b.firma_std 
			and 
			a.firma_std <> ''
		WHERE 
			b.cid IS NOT null
		;
		
		insert into 
			tmp_abgleich_poi
		SELECT 
			a.*, 
			b.cid, 
			6 as qual
		FROM 
			tmp_poi_mch AS a 
		LEFT JOIN 
			tmp_google_mch AS b
		ON 
			(
				a.strasse_std = b.strasse_std 
				AND 
				a.strasse_std <> '' 
				AND 
				a.plz_grob = b.plz_grob 
				and 
				length(a.plz_grob::text) > 2
			) 
			and 
			(
				a.hnr_std = '' 
				or 
				b.hnr_std = ''
			)
			and 
			--public.token_set_similarity(a.firma_std_ohne_phon, b.firma_std_ohne_phon) > 0.90
			--similarity(a.firma_std_ohne_phon,b.firma_std_ohne_phon) > 0.3
			public.fuzzy_token_set_ratio(a.firma_std_ohne_phon,b.firma_std_ohne_phon)>90
			and 
			a.firma_std_ohne_phon <> ''
		WHERE 
			b.cid IS NOT null
		;
		
		insert into 
			tmp_abgleich_poi
		SELECT 
			a.*, 
			b.cid, 
			7 as qual
		FROM 
			tmp_poi_mch AS a 
		LEFT JOIN 
			tmp_google_mch AS b
		ON 
			(
				a.strasse_std1 = b.strasse_std1 
				AND 
				a.strasse_std1 <> '' 
				AND 
				a.hnr_std = b.hnr_std 
				AND 
				a.plz_grob = b.plz_grob 
				and 
				length(a.plz_grob::text) > 2
			)
			and 
			-- public.token_set_similarity(a.firma_std_ohne_phon, b.firma_std_ohne_phon) > 0.90
 			-- similarity(a.firma_std_ohne_phon,b.firma_std_ohne_phon) > 0.3
			public.fuzzy_token_set_ratio(a.firma_std_ohne_phon,b.firma_std_ohne_phon)>90
			and 
			a.firma_std_ohne_phon <> ''
		WHERE 
			b.cid IS NOT null
		;
		
		insert into 
			tmp_abgleich_poi
		SELECT 
			a.*, 
			b.cid, 
			8 as qual
		FROM 
			tmp_poi_mch AS a 
		LEFT JOIN 
			tmp_google_mch AS b
		ON 
			(
				--public.token_set_similarity(a.strasse_std, b.strasse_std) > 0.95
				--similarity(a.strasse_std,b.strasse_std) > 0.3
				public.fuzzy_token_set_ratio(a.strasse_std, b.strasse_std)>95
				and 
				a.strasse_std <> '' 
				AND 
				a.hnr_std = b.hnr_std 
				AND 
				a.plz_grob = b.plz_grob 
				and 
				length(a.plz_grob::text) > 2
			)
			and 
			--public.token_set_similarity(a.firma_std_ohne_phon, b.firma_std_ohne_phon) > 0.90
			--similarity(a.firma_std_ohne_phon,b.firma_std_ohne_phon) > 0.3 
			public.fuzzy_token_set_ratio(a.firma_std_ohne_phon,b.firma_std_ohne_phon)>90
			and 
			a.firma_std_ohne_phon <> ''
		WHERE 
			b.cid IS NOT null
		;
		
		insert into 
			tmp_abgleich_poi
		SELECT 
			a.*, 
			b.cid, 
			9 as qual
		FROM 
			tmp_poi_mch AS a 
		LEFT JOIN 
			tmp_google_mch AS b
		ON 
			(
				a.strasse_std = b.strasse_std 
				and 
				a.strasse_std <> '' 
				AND 
				-- public.token_set_similarity(a.hnr_std, b.hnr_std) >= 0.50
				-- similarity(a.hnr_std,b.hnr_std) > 0.3
				public.fuzzy_token_set_ratio(a.hnr_std, b.hnr_std)>=50
				AND 
				a.plz_grob = b.plz_grob 
				and 
				length(a.plz_grob::text) > 2
			)
			and 
			--public.token_set_similarity(a.firma_std_ohne_phon, b.firma_std_ohne_phon) > 0.90
			--similarity(a.firma_std_ohne_phon,b.firma_std_ohne_phon) > 0.3 
			public.fuzzy_token_set_ratio(a.firma_std_ohne_phon,b.firma_std_ohne_phon)>90
			and 
			a.firma_std_ohne_phon <> ''
		WHERE 
			b.cid IS NOT null
		;
		
		insert into 
			tmp_abgleich_poi
		SELECT 
			a.*, 
			b.cid, 
			10 as qual
		FROM 
			tmp_poi_mch AS a 
		LEFT JOIN 
			tmp_google_mch AS b
		ON 
			(
				a.strasse_std = b.strasse_std 
				and 
				a.strasse_std <> '' 
				AND 
				a.hnr_std = b.hnr_std 
				AND 
				a.plz_grob = b.plz_grob 
				and 
				length(a.plz_grob::text) > 2
			)
			and 
				left(a.firma_std_ohne_phon,5) = left(b.firma_std_ohne_phon,5) 
				and 
				a.firma_std_ohne_phon <> ''
		WHERE 
			b.cid IS NOT null
		;
		
		insert into 
			tmp_abgleich_poi
		SELECT 
			a.*, 
			b.cid, 
			11 as qual
		FROM 
			tmp_poi_mch AS a 
		LEFT JOIN 
			tmp_google_mch AS b
		ON 
			(
				a.strasse_std = b.strasse_std 
				and 
				a.strasse_std <> '' 
				AND 
				a.hnr_std = b.hnr_std 
				AND 
				a.plz_grob = b.plz_grob 
				and 
				length(a.plz_grob::text) > 2
			)
			and 
			right(a.firma_std_ohne_phon,7) = right(b.firma_std_ohne_phon,7) 
			and 
			a.firma_std_ohne_phon <> ''
		WHERE 
			b.cid IS NOT null
		;
		
		insert into 
			tmp_abgleich_poi
		SELECT 
			a.*, 
			b.cid, 12 as qual
		FROM 
			tmp_poi_mch AS a 
		LEFT JOIN 
			tmp_google_mch AS b
		ON 
			(
				a.strasse_std = b.strasse_std 
				and 
				a.strasse_std <> '' 
				AND 
				a.plz_grob = b.plz_grob 
				and 
				length(a.plz_grob::text) > 2
			)
			and 
			a.firma_std_ohne_phon = b.firma_std_ohne_phon 
			and 
			a.firma_std_ohne_phon <> ''
		WHERE 
			b.cid IS NOT null
		;
		
		/*
		insert into geo_afo_prod.abgleich_poi
		SELECT a.*, b.cid, 14 as qual
		FROM geo_afo_prod.poi_mch AS a LEFT JOIN geo_afo_prod.google_mch AS b
			ON (a.strasse_std='' AND a.plz_grob=b.plz_grob and len(a.plz_grob)>2)
				and public.levenshtein_levenshtein_token_set_ratio(a.firma_std_ohne_phon, b.firma_std_ohne_phon)>90 and a.firma_std_ohne_phon<>''
		WHERE b.cid IS NOT null;
		*/
		
		-- Matching über Koordinaten innerhalb 50m
		drop table if exists 
			tmp_base_koord;
		
		create temp table 
			tmp_base_koord 
		as
		SELECT 
			a.*, 
			b.cid, 
			14 as qual, 
			b.geo_point_lv95 as geo_point_lv95_b, 
			b.firma_std_ohne_phon as firma_std_ohne_phon_b, 
			b.strasse_std as strasse_std_b
		FROM 
			tmp_poi_mch AS a 
		LEFT JOIN 
			tmp_google_mch AS b
		ON 
			--public.token_set_similarity(a.firma_std_ohne_phon, b.firma_std_ohne_phon) > 0.90
			--similarity(a.firma_std_ohne_phon,b.firma_std_ohne_phon) > 0.3 
			public.fuzzy_token_set_ratio(a.firma_std_ohne_phon,b.firma_std_ohne_phon)>90
			and 
			a.firma_std_ohne_phon <> '' 
			and 
			a.plz_grob = b.plz_grob
		WHERE 
			b.cid IS NOT null
		;
		
		delete from 
			tmp_base_koord 
		where 
			id in (
				select 
					id 
				from 
				tmp_abgleich_poi
			)
		;
		
		drop table if exists 
			tmp_mch_koord;
		
		create temp table 
			tmp_mch_koord 
		as
		SELECT 
			*
		FROM 
			tmp_base_koord
		WHERE 
			ST_DWithin(ST_SetSRID(geo_point_lv95::geometry, 2056), ST_SetSRID(geo_point_lv95_b::geometry, 2056), 30)
		; --ST_DWithin(geo_point_lv95::geometry, geo_point_lv95_b::geometry, 30);
		
		
		alter table 
			tmp_mch_koord 
		drop column if exists
			strasse_std_b
		; 
		alter table 
			tmp_mch_koord 
		drop column if exists
			firma_std_ohne_phon_b
		; 
		alter table 
			tmp_mch_koord 
		drop column if exists
			geo_point_lv95_b
		; 
		
		insert into 
			tmp_abgleich_poi
		SELECT 
			*
		FROM 
			tmp_mch_koord
		;
		
		-- Matching über Koordinaten innerhalb 50m und PLZ=0
		drop table if exists 
			tmp_base_koord;
		
		create table 
			tmp_base_koord 
		as
		with 
			poi_mch as (
				select 
					* 
				from 
					tmp_poi_mch 
				where 
					id not in (
						select 
							id 
						from 
							tmp_abgleich_poi
					)
			)
		SELECT 
			a.*, 
			b.cid, 
			15 as qual, 
			b.geo_point_lv95 as geo_point_lv95_b, 
			b.firma_std_ohne_phon as firma_std_ohne_phon_b, 
			b.strasse_std as strasse_std_b
		FROM 
			poi_mch AS a 
		LEFT JOIN (
			select 
				* 
			from 
				tmp_google_mch 
			where 
				plz4 = 0
		) AS b
		ON 
			--public.token_set_similarity(a.firma_std_ohne_phon, b.firma_std_ohne_phon) > 0.85
			--similarity(a.firma_std_ohne_phon,b.firma_std_ohne_phon) > 0.3 
			public.fuzzy_token_set_ratio(a.firma_std_ohne_phon,b.firma_std_ohne_phon)>85
			and 
			a.firma_std_ohne_phon <> ''
		WHERE 
			b.cid IS NOT null
		;
		
		delete from 
			tmp_base_koord 
		where 
			id in (	
				select 
					id 
				from 
					tmp_abgleich_poi
			)
		;
		
		drop table if exists 
			tmp_mch_koord;
		
		create table 
			tmp_mch_koord 
		as
		SELECT 
			*
		FROM 
			tmp_base_koord
		WHERE 
			ST_DWithin(ST_SetSRID(geo_point_lv95::geometry, 2056), ST_SetSRID(geo_point_lv95_b::geometry, 2056), 30)
		; --ST_DWithin(geo_point_lv95::geometry, geo_point_lv95_b::geometry, 30);
		
		alter table 
			tmp_mch_koord 
		drop column if exists
			strasse_std_b
		; 
		alter table 
			tmp_mch_koord 
		drop column if exists 
			firma_std_ohne_phon_b
		; 
		alter table 
			tmp_mch_koord 
		drop column if exists 
			geo_point_lv95_b
		; 
		
		insert into 
			tmp_abgleich_poi
		SELECT 
			*
		FROM 
			tmp_mch_koord
		;
		
		
		--------------------------------------------------------------------
		-- 3. Zusammenfassung Abgleich
		---------------------------------------------------------------------
		
		-- Gastro-Adressen
		drop table if exists 
			tmp_abgleich_poi_prio;
		
		create temp table 
			tmp_abgleich_poi_prio 
		as
		select 
			*, 
			row_number () over (
				partition by 
					id 
				order by 
					qual, 
					random()
			) as prio
		from 
			tmp_abgleich_poi
		;
		
		delete from 
			tmp_abgleich_poi_prio 
		where 
			prio > 1
		;
		
		insert into 
			tmp_abgleich_poi_prio
		select 
			*
		from 
			tmp_poi_mch
		where 
			id not in (
				select 
					id 
				from 
					tmp_abgleich_poi_prio
			)
		;
		
		
		--------------------------------------------------------------------
		-- 4. Zusammenfassung Dubletten für Peter
		---------------------------------------------------------------------
		-- Dubletten File mit AZ Adressen und TopCC Adressen
		drop table if exists 
			tmp_poi_abgleich_google_tot;
		
		create temp table 
			tmp_poi_abgleich_google_tot 
		as
		select 
			cast(t1.poi_id as varchar(1000)) as poi_id,
			t1.hauskey,
			t1.poi_typ_id,
			t1.poi_typ,
			cast('' as varchar(1000)) as google_poi_typ,
			cast('' as varchar(1000)) as category_ids,
			t1.company_group_id,
			t1.company_group,
			--cast(t1.company_id as int) as company_id,
			t1.company_id,
			--cast('' as varchar(255)) as keyword,
			--cast('' as varchar(255)) as category,
			t1.company,
			t1.company_unit,
			t1.company_brand,
			t1.bezeichnung_lang,
			t1.bezeichnung_kurz,
			t1.adresse,
			cast('' as varchar(1000)) as adress_lang,
			t1.plz4,
			--cast(t1.plz4 as varchar(1000)) as plz4_orig,
			t1.plz4 as plz4_orig,
			t1.ort,
			cast('' as varchar(1000)) as google_strasse,
			cast('' as varchar(1000)) as google_strasse_std,
			cast('' as varchar(1000)) as google_hausnum,
			cast('' as varchar(1000)) as google_plz4,
			cast('' as varchar(1000)) as google_ort,
			cast('' as varchar(1000)) as gwr_strasse,
			cast('' as varchar(1000)) as gwr_hausnum,
			cast( 0 as int) as gwr_plz4,
			cast('' as varchar(1000)) as gwr_ort,
			cast('' as varchar(1000)) as plz6,
			cast('' as varchar(1000)) as gemeinde,
			cast('' as varchar(1000)) as gmd_nr,
			cast(t1.url as varchar(10000)) as url,
			cast('' as varchar(1000)) as domain,
			t1.geo_point_lv95,
			cast('AFO' as varchar(255)) as quelle,
			cast(t2.cid as varchar(1000)) as dubletten_nr 
		from 
			tmp_poi_mch t1
		join 
			tmp_abgleich_poi_prio t2
		on 
			t1.id = t2.id
		;
		
		insert into 
			tmp_poi_abgleich_google_tot
		select 
			T1.cid, 
			0 as hauskey,
			0 as poi_typ_id,
			'' as poi_typ,
			google_poi_typ,
			category_ids as google_category_ids,
			0 as company_group_id,
			'' as company_group,
			0 as company_id,
			--keyword,
			--category,
			title, 
			'' as company_unit,
			'' as company_brand,
			'' as bezeichnung_lang,
			'' as bezeichnung_kurz,
			strasse, 
			address as adress_lang,
			cast(
				case 
					when length(plz4::text) <> 4 then 0 
					else cast(plz4 as int) 
				end as int
			), 
			plz4 as plz4_orig,
			ort,
			google_strasse,
			google_strasse_std,
			google_hausnum,
			google_plz4,
			google_ort,
			gwr_strasse,
			gwr_hausnum,
			gwr_plz4,
			gwr_ort,
			plz6,
			gemeinde,
			gmd_nr,
			url,
			domain,
			geo_point_lv95,
			'GOOGLE' as quelle,
			cast(t2.cid as varchar(1000)) as dubletten_nr 
		from 
			tmp_google_poi_input T1
		left join (
			select distinct 
				cid 
			from 
				tmp_abgleich_poi_prio
		) T2 
		on 
			T1.cid = T2.cid
		;
		
		
		--------------------------------------------------------------------
		-- tmp_poi_abgleich_google_tot in <abgleich_output_table> schreiben
		---------------------------------------------------------------------
		cmd := '
			drop table if exists
				' || abgleich_output_table || ';

			create table
				' || abgleich_output_table || '
			as
			select
				*
			from
				tmp_poi_abgleich_google_tot
			;
		'
		;
		execute cmd;

	end;
$$
;













------------------------------------------------------
-- LEVENSHTEIN TESTS
------------------------------------------------------

-- Creation of functions:

-- Levenshtein Weighted distance

CREATE OR REPLACE FUNCTION weighted_levenshtein_distance(s1 TEXT, s2 TEXT)
RETURNS INTEGER AS $$
DECLARE
    len1 INTEGER := LENGTH(s1);
    len2 INTEGER := LENGTH(s2);
    dp INTEGER[][];
    i INTEGER;
    j INTEGER;
BEGIN
    -- Initialize dp array for (len1 + 1) x (len2 + 1) with zeroes
    dp := ARRAY(SELECT ARRAY_FILL(0, ARRAY[len2 + 1]) FROM generate_series(1, len1 + 1));

    -- Base cases: cost for deleting all characters in s1 and inserting all in s2
    FOR i IN 0..len1 LOOP
        dp[i + 1][1] := i;
    END LOOP;
    FOR j IN 0..len2 LOOP
        dp[1][j + 1] := j;
    END LOOP;

    -- Fill the dp table
    FOR i IN 1..len1 LOOP
        FOR j IN 1..len2 LOOP
            IF SUBSTRING(s1 FROM i FOR 1) = SUBSTRING(s2 FROM j FOR 1) THEN
                dp[i + 1][j + 1] := dp[i][j];  -- No cost for matching characters
            ELSE
                dp[i + 1][j + 1] := LEAST(
                    dp[i][j + 1] + 1,  -- Delete
                    dp[i + 1][j] + 1,  -- Insert
                    dp[i][j] + 2       -- Substitution (weighted as 2)
                );
            END IF;
        END LOOP;
    END LOOP;

    -- Return the final distance from the last cell
    RETURN dp[len1 + 1][len2 + 1];
END;
$$ LANGUAGE plpgsql;

-- Ratio function by fuzzywuzzy newly interpreted

CREATE OR REPLACE FUNCTION fuzzy_ratio(s1 TEXT, s2 TEXT)
RETURNS INTEGER AS $$
DECLARE
    levenshtein_distance INTEGER;
    total_length INTEGER;
BEGIN
    -- Calculate the weighted Levenshtein distance
    levenshtein_distance := weighted_levenshtein(s1, s2);
    
    -- Calculate total length of both strings
    total_length := LENGTH(s1) + LENGTH(s2);
    
    -- Handle edge case where both strings are empty
    IF total_length = 0 THEN
        RETURN 100;
    END IF;

    -- Return rounded integer fuzz ratio
    RETURN ROUND(100 * (1 - CAST(levenshtein_distance AS FLOAT) / total_length));
END;
$$ LANGUAGE plpgsql;

SELECT fuzzy_ratio('Szene','Szemen');

-- Partial Ratio function by fuzzywuzzy newly interpreted
-- Nicht gleich wie fuzzywuzzy, aber meiner Meinung nach korrekter für unsere Zwecke, die Quote kann auch runtergesetzt werden damit es mehr Matches gibt


CREATE OR REPLACE FUNCTION fuzzy_partial_ratio(s1 TEXT, s2 TEXT)
RETURNS INTEGER AS $$
DECLARE
    shorter TEXT;
    longer TEXT;
    best_match_score INTEGER := 0;
    current_score INTEGER;
    i INTEGER;
BEGIN
    -- Determine the shorter and longer strings
    IF LENGTH(s1) > LENGTH(s2) THEN
        shorter := s2;
        longer := s1;
    ELSE
        shorter := s1;
        longer := s2;
    END IF;

    -- Loop through substrings of the longer string that are the length of the shorter string
    FOR i IN 1..(LENGTH(longer) - LENGTH(shorter) + 1) LOOP
        -- Get the substring of the longer string
        current_score := fuzzy_ratio(shorter, SUBSTRING(longer FROM i FOR LENGTH(shorter)));

        -- Update the best match score if the current score is higher
        IF current_score > best_match_score THEN
            best_match_score := current_score;
        END IF;
    END LOOP;

    -- Return the best match score
    RETURN best_match_score;
END;
$$ LANGUAGE plpgsql;

SELECT public.fuzzy_partial_ratio('Szene','Szenen');

-- Token Sort Ratio nach Fuzzywuzzy

CREATE OR REPLACE FUNCTION fuzzy_token_sort_ratio(s1 TEXT, s2 TEXT)
RETURNS INTEGER AS $$
DECLARE
    cleaned_s1 TEXT;
    cleaned_s2 TEXT;
    sorted_s1 TEXT;
    sorted_s2 TEXT;
BEGIN
    -- Clean the strings: lowercase, trim spaces, and remove non-alphanumeric characters (except spaces)
    cleaned_s1 := REGEXP_REPLACE(LOWER(TRIM(s1)), '[^a-z0-9\s]', '', 'g');
    cleaned_s2 := REGEXP_REPLACE(LOWER(TRIM(s2)), '[^a-z0-9\s]', '', 'g');

    -- Split into words, sort alphabetically, and join back into a string
    sorted_s1 := ARRAY_TO_STRING(ARRAY(SELECT UNNEST(REGEXP_SPLIT_TO_ARRAY(cleaned_s1, '\s+')) ORDER BY 1), ' ');
    sorted_s2 := ARRAY_TO_STRING(ARRAY(SELECT UNNEST(REGEXP_SPLIT_TO_ARRAY(cleaned_s2, '\s+')) ORDER BY 1), ' ');

    -- Compute the partial ratio of the sorted strings
    RETURN fuzzy_ratio(sorted_s1, sorted_s2);
END;
$$ LANGUAGE plpgsql;

SELECT fuzzy_token_sort_ratio('Szenen im Film','Im verändert Szenen Film in');

-- Token Set Ratio nach Fuzzywuzzy

CREATE OR REPLACE FUNCTION fuzzy_token_set_ratio(s1 TEXT, s2 TEXT)
RETURNS INTEGER AS $$
DECLARE
    cleaned_s1 TEXT;
    cleaned_s2 TEXT;
    tokens1 TEXT[];
    tokens2 TEXT[];
    intersection TEXT[];
    diff1to2 TEXT[];
    diff2to1 TEXT[];
    sorted_sect TEXT;
    sorted_1to2 TEXT;
    sorted_2to1 TEXT;
    combined_1to2 TEXT;
    combined_2to1 TEXT;
    ratio1 INTEGER;
    ratio2 INTEGER;
    ratio3 INTEGER;
    ratio_func REFCURSOR;
BEGIN
    -- Clean the strings: lowercase, trim spaces, and remove non-alphanumeric characters (except spaces)
    cleaned_s1 := REGEXP_REPLACE(LOWER(TRIM(s1)), '[^a-z0-9\s]', '', 'g');
    cleaned_s2 := REGEXP_REPLACE(LOWER(TRIM(s2)), '[^a-z0-9\s]', '', 'g');

    -- Extract words and convert to ordered unique word sets
    tokens1 := REGEXP_SPLIT_TO_ARRAY(cleaned_s1, '\s+');
    tokens2 := REGEXP_SPLIT_TO_ARRAY(cleaned_s2, '\s+');

    -- Find common and unique words
	intersection := ARRAY(SELECT DISTINCT UNNEST(tokens1) INTERSECT SELECT DISTINCT UNNEST(tokens2));
    diff1to2 := ARRAY(SELECT DISTINCT UNNEST(tokens1) EXCEPT SELECT DISTINCT UNNEST(tokens2));
    diff2to1 := ARRAY(SELECT DISTINCT UNNEST(tokens2) EXCEPT SELECT DISTINCT UNNEST(tokens1));

    -- Sort the arrays of intersection and differences
    sorted_sect := ARRAY_TO_STRING(ARRAY(SELECT UNNEST(intersection) ORDER BY 1), ' ');
    sorted_1to2 := ARRAY_TO_STRING(ARRAY(SELECT UNNEST(diff1to2) ORDER BY 1), ' ');
    sorted_2to1 := ARRAY_TO_STRING(ARRAY(SELECT UNNEST(diff2to1) ORDER BY 1), ' ');

    -- Combine sorted tokens
    combined_1to2 := sorted_sect || ' ' || sorted_1to2;
    combined_2to1 := sorted_sect || ' ' || sorted_2to1;

    -- Combine sorted tokens
    sorted_sect := TRIM(sorted_sect);
    combined_1to2 := TRIM(combined_1to2);
    combined_2to1 := TRIM(combined_2to1);

    -- Compute the ratios for the combinations
    ratio1 := fuzzy_ratio(sorted_sect, combined_1to2);
    ratio2 := fuzzy_ratio(sorted_sect, combined_2to1);
    ratio3 := fuzzy_ratio(combined_1to2, combined_2to1);

    -- Return the highest score
    RETURN GREATEST(ratio1, ratio2, ratio3);
END;
$$ LANGUAGE plpgsql;


SELECT public.fuzzy_token_set_ratio('abc Szemen im Film','120 Im verändert Szenen Film in *""');




------------------------------------
-- Testing Ground
------------------------------------

SELECT public.fuzzy_ratio('Hallodfrsf', 'gdggdgHalro');

SELECT public.fuzzy_partial_ratio('Hallodfrsf', 'gdggdgHalro');

SELECT public.fuzzy_token_sort_ratio('gdgg dgHalr o','Hallo df sf sf sf');

SELECT public.fuzzy_token_set_ratio('gdgg dgHalr o','Hallo df sf sf sf');


