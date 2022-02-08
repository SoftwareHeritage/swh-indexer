-- SWH Indexer DB schema upgrade
-- from_version: 114
-- to_version: 115
-- description: Remove temporary table use in reading api

insert into dbversion(version, release, description)
values(115, now(), 'Work In Progress');

drop function swh_mktemp_content_mimetype_missing();
drop function swh_content_mimetype_missing();

drop function swh_content_mimetype_get();
drop type content_mimetype_signature;

drop function swh_mktemp_content_language_missing();
drop function swh_content_language_missing();

drop function swh_content_language_get();
drop type content_language_signature;

drop function swh_mktemp_content_ctags_missing();
drop function swh_content_ctags_missing();

drop function swh_content_ctags_get();
--drop type content_ctags_signature;  -- still used in swh_content_ctags_search

drop function swh_content_fossology_license_get();
drop type content_fossology_license_signature;

drop function swh_mktemp_content_metadata_missing();
drop function swh_content_metadata_missing();

drop function swh_content_metadata_get();
drop type content_metadata_signature;

drop function swh_mktemp_revision_metadata_missing();
drop function swh_revision_metadata_missing();

drop function swh_revision_metadata_get();
drop type revision_metadata_signature;

drop function swh_mktemp_bytea();
