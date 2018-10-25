-- SWH Indexer DB schema upgrade
-- from_version: 115
-- to_version: 116
-- description:

insert into dbversion(version, release, description)
values(116, now(), 'Work In Progress');

drop table origin_metadata_translation;

create table origin_intrinsic_metadata(
  origin_id                 bigserial  not null,
  metadata                  jsonb,
  indexer_configuration_id  bigint     not null,
  from_revision             sha1_git   not null
);

comment on table origin_intrinsic_metadata is 'keeps intrinsic metadata for an origin';
comment on column origin_intrinsic_metadata.origin_id is 'the entry id in origin';
comment on column origin_intrinsic_metadata.metadata is 'metadata extracted from a revision';
comment on column origin_intrinsic_metadata.indexer_configuration_id is 'tool used to generate this metadata';
comment on column origin_intrinsic_metadata.from_revision is 'sha1 of the revision this metadata was copied from.';

-- create a temporary table for retrieving origin_intrinsic_metadata
create or replace function swh_mktemp_origin_intrinsic_metadata()
    returns void
    language sql
as $$
  create temporary table tmp_origin_intrinsic_metadata (
    like origin_intrinsic_metadata including defaults
  ) on commit drop;
$$;

comment on function swh_mktemp_origin_intrinsic_metadata() is 'Helper table to add origin intrinsic metadata';


-- add tmp_origin_intrinsic_metadata entries to origin_intrinsic_metadata,
-- overwriting duplicates if conflict_update is true, skipping duplicates
-- otherwise.
--
-- If filtering duplicates is in order, the call to
-- swh_origin_intrinsic_metadata_missing must take place before calling this
-- function.
--
-- operates in bulk: 0. swh_mktemp(content_language), 1. COPY to
-- tmp_origin_intrinsic_metadata, 2. call this function
create or replace function swh_origin_intrinsic_metadata_add(
        conflict_update boolean)
    returns void
    language plpgsql
as $$
begin
    if conflict_update then
      insert into origin_intrinsic_metadata (origin_id, metadata, indexer_configuration_id, from_revision)
      select origin_id, metadata, indexer_configuration_id, from_revision
      from tmp_origin_intrinsic_metadata
            on conflict(origin_id, indexer_configuration_id)
                do update set metadata = excluded.metadata;

    else
        insert into origin_intrinsic_metadata (origin_id, metadata, indexer_configuration_id, from_revision)
        select origin_id, metadata, indexer_configuration_id, from_revision
      from tmp_origin_intrinsic_metadata
            on conflict(origin_id, indexer_configuration_id)
            do nothing;
    end if;
    return;
end
$$;

comment on function swh_origin_intrinsic_metadata_add(boolean) IS 'Add new origin intrinsic metadata';


-- origin_intrinsic_metadata
create unique index origin_intrinsic_metadata_pkey on origin_intrinsic_metadata(origin_id, indexer_configuration_id);
alter table origin_intrinsic_metadata add primary key using index origin_intrinsic_metadata_pkey;

alter table origin_intrinsic_metadata add constraint origin_intrinsic_metadata_indexer_configuration_id_fkey foreign key (indexer_configuration_id) references indexer_configuration(id) not valid;
alter table origin_intrinsic_metadata validate constraint origin_intrinsic_metadata_indexer_configuration_id_fkey;
alter table origin_intrinsic_metadata add constraint origin_intrinsic_metadata_revision_metadata_fkey foreign key (from_revision, indexer_configuration_id) references revision_metadata(id, indexer_configuration_id) not valid;
alter table origin_intrinsic_metadata validate constraint origin_intrinsic_metadata_revision_metadata_fkey;
