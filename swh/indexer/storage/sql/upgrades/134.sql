-- SWH Indexer DB schema upgrade
-- from_version: 133
-- to_version: 134
-- description: replace revision_intrinsic_metadata with directory_intrinsic_metadata
--   and origin_intrinsic_metadata.from_revision with origin_intrinsic_metadata.from_directory
--   This migration works by dropping both tables and reindexing from scratch.

insert into dbversion(version, release, description)
      values(134, now(), 'Work In Progress');

drop table origin_intrinsic_metadata;
drop table revision_intrinsic_metadata;
drop function swh_revision_intrinsic_metadata_add;
drop function swh_mktemp_revision_intrinsic_metadata;


create table directory_intrinsic_metadata(
  id                       sha1_git   not null,
  metadata                 jsonb      not null,
  indexer_configuration_id bigint     not null,
  mappings                 text array not null
);

comment on table directory_intrinsic_metadata is 'metadata semantically detected and translated in a directory';
comment on column directory_intrinsic_metadata.id is 'sha1_git of directory';
comment on column directory_intrinsic_metadata.metadata is 'result of detection and translation with defined format';
comment on column directory_intrinsic_metadata.indexer_configuration_id is 'tool used for detection';
comment on column directory_intrinsic_metadata.mappings is 'type of metadata files used to obtain this metadata (eg. pkg-info, npm)';

create table origin_intrinsic_metadata(
  id                        text       not null,  -- origin url
  metadata                  jsonb,
  indexer_configuration_id  bigint     not null,
  from_directory             sha1_git   not null,
  metadata_tsvector         tsvector,
  mappings                  text array not null
);

comment on table origin_intrinsic_metadata is 'keeps intrinsic metadata for an origin';
comment on column origin_intrinsic_metadata.id is 'url of the origin';
comment on column origin_intrinsic_metadata.metadata is 'metadata extracted from a directory';
comment on column origin_intrinsic_metadata.indexer_configuration_id is 'tool used to generate this metadata';
comment on column origin_intrinsic_metadata.from_directory is 'sha1 of the directory this metadata was copied from.';
comment on column origin_intrinsic_metadata.mappings is 'type of metadata files used to obtain this metadata (eg. pkg-info, npm)';

-- add tmp_directory_intrinsic_metadata entries to directory_intrinsic_metadata,
-- overwriting duplicates.
--
-- If filtering duplicates is in order, the call to
-- swh_directory_intrinsic_metadata_missing must take place before calling this
-- function.
--
-- operates in bulk: 0. swh_mktemp(content_language), 1. COPY to
-- tmp_directory_intrinsic_metadata, 2. call this function
create or replace function swh_directory_intrinsic_metadata_add()
    returns bigint
    language plpgsql
as $$
declare
  res bigint;
begin
    insert into directory_intrinsic_metadata (id, metadata, mappings, indexer_configuration_id)
    select id, metadata, mappings, indexer_configuration_id
    from tmp_directory_intrinsic_metadata tcm
    on conflict(id, indexer_configuration_id)
    do update set
        metadata = excluded.metadata,
        mappings = excluded.mappings;

    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_directory_intrinsic_metadata_add() IS 'Add new directory intrinsic metadata';

-- create a temporary table for retrieving directory_intrinsic_metadata
create or replace function swh_mktemp_directory_intrinsic_metadata()
    returns void
    language sql
as $$
  create temporary table if not exists tmp_directory_intrinsic_metadata (
    like directory_intrinsic_metadata including defaults
  ) on commit delete rows;
$$;

comment on function swh_mktemp_directory_intrinsic_metadata() is 'Helper table to add directory intrinsic metadata';

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

-- add tmp_origin_intrinsic_metadata entries to origin_intrinsic_metadata,
-- overwriting duplicates.
--
-- If filtering duplicates is in order, the call to
-- swh_origin_intrinsic_metadata_missing must take place before calling this
-- function.
--
-- operates in bulk: 0. swh_mktemp(content_language), 1. COPY to
-- tmp_origin_intrinsic_metadata, 2. call this function
create or replace function swh_origin_intrinsic_metadata_add()
    returns bigint
    language plpgsql
as $$
declare
  res bigint;
begin
    perform swh_origin_intrinsic_metadata_compute_tsvector();

    insert into origin_intrinsic_metadata (id, metadata, indexer_configuration_id, from_directory, metadata_tsvector, mappings)
    select id, metadata, indexer_configuration_id, from_directory,
           metadata_tsvector, mappings
    from tmp_origin_intrinsic_metadata
    on conflict(id, indexer_configuration_id)
    do update set
        metadata = excluded.metadata,
        metadata_tsvector = excluded.metadata_tsvector,
        mappings = excluded.mappings,
        from_directory = excluded.from_directory;

    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_origin_intrinsic_metadata_add() IS 'Add new origin intrinsic metadata';



-- directory_intrinsic_metadata
create unique index directory_intrinsic_metadata_pkey on directory_intrinsic_metadata(id, indexer_configuration_id);
alter table directory_intrinsic_metadata add primary key using index directory_intrinsic_metadata_pkey;

alter table directory_intrinsic_metadata add constraint directory_intrinsic_metadata_indexer_configuration_id_fkey foreign key (indexer_configuration_id) references indexer_configuration(id) not valid;
alter table directory_intrinsic_metadata validate constraint directory_intrinsic_metadata_indexer_configuration_id_fkey;

-- origin_intrinsic_metadata
create unique index origin_intrinsic_metadata_pkey on origin_intrinsic_metadata(id, indexer_configuration_id);
alter table origin_intrinsic_metadata add primary key using index origin_intrinsic_metadata_pkey;

alter table origin_intrinsic_metadata add constraint origin_intrinsic_metadata_indexer_configuration_id_fkey foreign key (indexer_configuration_id) references indexer_configuration(id) not valid;
alter table origin_intrinsic_metadata validate constraint origin_intrinsic_metadata_indexer_configuration_id_fkey;

create index origin_intrinsic_metadata_fulltext_idx on origin_intrinsic_metadata using gin (metadata_tsvector);
create index origin_intrinsic_metadata_mappings_idx on origin_intrinsic_metadata using gin (mappings);
