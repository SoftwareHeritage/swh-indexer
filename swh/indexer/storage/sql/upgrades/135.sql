-- SWH Indexer DB schema upgrade
-- from_version: 134
-- to_version: 135
-- description: Add support for origin_extrinsic_metadata

insert into dbversion(version, release, description)
      values(135, now(), 'Work In Progress');

create table origin_extrinsic_metadata(
  id                        text       not null,  -- origin url
  metadata                  jsonb,
  indexer_configuration_id  bigint     not null,
  from_remd_id              sha1_git   not null,
  metadata_tsvector         tsvector,
  mappings                  text array not null
);

comment on table origin_extrinsic_metadata is 'keeps extrinsic metadata for an origin';
comment on column origin_extrinsic_metadata.id is 'url of the origin';
comment on column origin_extrinsic_metadata.metadata is 'metadata extracted from a directory';
comment on column origin_extrinsic_metadata.indexer_configuration_id is 'tool used to generate this metadata';
comment on column origin_extrinsic_metadata.from_remd_id is 'sha1 of the directory this metadata was copied from.';
comment on column origin_extrinsic_metadata.mappings is 'type of metadata files used to obtain this metadata (eg. github, gitlab)';

-- create a temporary table for retrieving origin_extrinsic_metadata
create or replace function swh_mktemp_origin_extrinsic_metadata()
    returns void
    language sql
as $$
  create temporary table if not exists tmp_origin_extrinsic_metadata (
    like origin_extrinsic_metadata including defaults
  ) on commit delete rows;
$$;

comment on function swh_mktemp_origin_extrinsic_metadata() is 'Helper table to add origin extrinsic metadata';

create or replace function swh_mktemp_indexer_configuration()
    returns void
    language sql
as $$
    create temporary table if not exists tmp_indexer_configuration (
      like indexer_configuration including defaults
    ) on commit delete rows;
    alter table tmp_indexer_configuration drop column if exists id;
$$;

-- add tmp_origin_extrinsic_metadata entries to origin_extrinsic_metadata,
-- overwriting duplicates.
--
-- If filtering duplicates is in order, the call to
-- swh_origin_extrinsic_metadata_missing must take place before calling this
-- function.
--
-- operates in bulk: 0. swh_mktemp(content_language), 1. COPY to
-- tmp_origin_extrinsic_metadata, 2. call this function
create or replace function swh_origin_extrinsic_metadata_add()
    returns bigint
    language plpgsql
as $$
declare
  res bigint;
begin
    perform swh_origin_extrinsic_metadata_compute_tsvector();

    insert into origin_extrinsic_metadata (id, metadata, indexer_configuration_id, from_remd_id, metadata_tsvector, mappings)
    select id, metadata, indexer_configuration_id, from_remd_id,
           metadata_tsvector, mappings
    from tmp_origin_extrinsic_metadata
    on conflict(id, indexer_configuration_id)
    do update set
        metadata = excluded.metadata,
        metadata_tsvector = excluded.metadata_tsvector,
        mappings = excluded.mappings,
        from_remd_id = excluded.from_remd_id;

    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_origin_extrinsic_metadata_add() IS 'Add new origin extrinsic metadata';


-- Compute the metadata_tsvector column in tmp_origin_extrinsic_metadata.
--
-- It uses the "pg_catalog.simple" dictionary, as it has no stopword,
-- so it should be suitable for proper names and non-English text.
create or replace function swh_origin_extrinsic_metadata_compute_tsvector()
    returns void
    language plpgsql
as $$
begin
    update tmp_origin_extrinsic_metadata
        set metadata_tsvector = to_tsvector('pg_catalog.simple', metadata);
end
$$;

-- origin_extrinsic_metadata
create unique index origin_extrinsic_metadata_pkey on origin_extrinsic_metadata(id, indexer_configuration_id);
alter table origin_extrinsic_metadata add primary key using index origin_extrinsic_metadata_pkey;

alter table origin_extrinsic_metadata add constraint origin_extrinsic_metadata_indexer_configuration_id_fkey foreign key (indexer_configuration_id) references indexer_configuration(id) not valid;
alter table origin_extrinsic_metadata validate constraint origin_extrinsic_metadata_indexer_configuration_id_fkey;

create index origin_extrinsic_metadata_fulltext_idx on origin_extrinsic_metadata using gin (metadata_tsvector);
create index origin_extrinsic_metadata_mappings_idx on origin_extrinsic_metadata using gin (mappings);
