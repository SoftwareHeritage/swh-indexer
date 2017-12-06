create sequence origin_metadata_translation_id_seq
	start with 1
	increment by 1
	no maxvalue
	no minvalue
	cache 1;

select setval('fossology_license_id_seq', 833, true);
