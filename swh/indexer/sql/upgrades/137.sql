-- SWH Indexer DB schema upgrade
-- from_version: 136
-- to_version: 137
-- description: Drop content_language and content_ctags tables and related functions

insert into dbversion(version, release, description)
      values(137, now(), 'Work In Progress');

drop function swh_content_language_add;
drop function swh_mktemp_content_language();
drop function swh_mktemp_content_ctags();
drop function swh_content_ctags_add();
drop function swh_content_ctags_search;

drop index content_language_pkey;

drop table content_language;
drop table content_ctags;

drop type languages;
drop type ctags_languages;
drop type content_ctags_signature;

