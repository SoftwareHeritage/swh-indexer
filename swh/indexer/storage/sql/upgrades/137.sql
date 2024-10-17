-- SWH Indexer DB schema upgrade
-- from_version: 136
-- to_version: 137
-- description: Drop content_language and content_ctags tables and related functions

drop function if exists swh_content_language_add;
drop function if exists swh_mktemp_content_language();
drop function if exists swh_mktemp_content_ctags();
drop function if exists swh_content_ctags_add();
drop function if exists swh_content_ctags_search;

drop type if exists content_ctags_signature;

drop table if exists content_language;
drop table if exists content_ctags;

drop type if exists languages;
drop type if exists ctags_languages;

