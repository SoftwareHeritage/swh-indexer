-- SWH Indexer DB schema upgrade
-- from_version: 128
-- to_version: 129
-- description:

insert into dbversion(version, release, description)
values(129, now(), 'Work In Progress');

create or replace function swh_mktemp(tblname regclass)
    returns void
    language plpgsql
as $$
begin
    execute format('
	create temporary table if not exists tmp_%1$I
	    (like %1$I including defaults)
	    on commit delete rows;
      alter table tmp_%1$I drop column if exists object_id;
	', tblname);
    return;
end
$$;

-- create a temporary table for content_mimetype tmp_content_mimetype,
create or replace function swh_mktemp_content_mimetype()
    returns void
    language sql
as $$
  create temporary table if not exists tmp_content_mimetype (
    like content_mimetype including defaults
  ) on commit delete rows;
$$;

-- create a temporary table for retrieving content_language
create or replace function swh_mktemp_content_language()
    returns void
    language sql
as $$
  create temporary table if not exists tmp_content_language (
    like content_language including defaults
  ) on commit delete rows;
$$;

comment on function swh_mktemp_content_language() is 'Helper table to add content language';


-- create a temporary table for content_ctags tmp_content_ctags,
create or replace function swh_mktemp_content_ctags()
    returns void
    language sql
as $$
  create temporary table if not exists tmp_content_ctags (
    like content_ctags including defaults
  ) on commit delete rows;
$$;

comment on function swh_mktemp_content_ctags() is 'Helper table to add content ctags';

-- create a temporary table for content_fossology_license tmp_content_fossology_license,
create or replace function swh_mktemp_content_fossology_license()
    returns void
    language sql
as $$
  create temporary table if not exists tmp_content_fossology_license (
    id                       sha1,
    license                  text,
    indexer_configuration_id integer
  ) on commit delete rows;
$$;

comment on function swh_mktemp_content_fossology_license() is 'Helper table to add content license';


-- create a temporary table for retrieving content_metadata
create or replace function swh_mktemp_content_metadata()
    returns void
    language sql
as $$
  create temporary table if not exists tmp_content_metadata (
    like content_metadata including defaults
  ) on commit delete rows;
$$;

comment on function swh_mktemp_content_metadata() is 'Helper table to add content metadata';


-- create a temporary table for retrieving revision_intrinsic_metadata
create or replace function swh_mktemp_revision_intrinsic_metadata()
    returns void
    language sql
as $$
  create temporary table if not exists tmp_revision_intrinsic_metadata (
    like revision_intrinsic_metadata including defaults
  ) on commit delete rows;
$$;

comment on function swh_mktemp_revision_intrinsic_metadata() is 'Helper table to add revision intrinsic metadata';

-- create a temporary table for retrieving origin_intrinsic_metadata
create or replace function swh_mktemp_origin_intrinsic_metadata()
    returns void
    language sql
as $$
  create temporary table if not exists tmp_origin_intrinsic_metadata (
    like origin_intrinsic_metadata including defaults
  ) on commit delete rows;
$$;

comment on function swh_mktemp_origin_intrinsic_metadata() is 'Helper table to add origin intrinsic metadata';

create or replace function swh_mktemp_indexer_configuration()
    returns void
    language sql
as $$
    create temporary table if not exists tmp_indexer_configuration (
      like indexer_configuration including defaults
    ) on commit delete rows;
    alter table tmp_indexer_configuration drop column if exists id;
$$;
