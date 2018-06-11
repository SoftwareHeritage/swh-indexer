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
