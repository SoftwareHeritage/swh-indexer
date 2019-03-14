-- SWH Indexer DB schema upgrade
-- from_version: 122
-- to_version: 123
-- description: fix heterogeneity of names in metadata tables

insert into dbversion(version, release, description)
values(123, now(), 'Work In Progress');

create or replace function swh_content_metadata_add(conflict_update boolean)
    returns void
    language plpgsql
as $$
begin
    if conflict_update then
      insert into content_metadata (id, metadata, indexer_configuration_id)
      select id, metadata, indexer_configuration_id
      from tmp_content_metadata tcm
            on conflict(id, indexer_configuration_id)
                do update set metadata = excluded.metadata;

    else
        insert into content_metadata (id, metadata, indexer_configuration_id)
        select id, metadata, indexer_configuration_id
      from tmp_content_metadata tcm
            on conflict(id, indexer_configuration_id)
            do nothing;
    end if;
    return;
end
$$;

alter function swh_revision_metadata_add rename to swh_revision_intrinsic_metadata_add;
create or replace function swh_revision_intrinsic_metadata_add(conflict_update boolean)
    returns void
    language plpgsql
as $$
begin
    if conflict_update then
      insert into revision_intrinsic_metadata (id, metadata, mappings, indexer_configuration_id)
      select id, metadata, mappings, indexer_configuration_id
    	from tmp_revision_intrinsic_metadata tcm
            on conflict(id, indexer_configuration_id)
                do update set
                    metadata = excluded.metadata,
                    mappings = excluded.mappings;

    else
        insert into revision_intrinsic_metadata (id, metadata, mappings, indexer_configuration_id)
        select id, metadata, mappings, indexer_configuration_id
    	from tmp_revision_intrinsic_metadata tcm
            on conflict(id, indexer_configuration_id)
            do nothing;
    end if;
    return;
end
$$;

alter function swh_mktemp_revision_metadata rename to swh_mktemp_revision_intrinsic_metadata;
create or replace function swh_mktemp_revision_intrinsic_metadata()
    returns void
    language sql
as $$
  create temporary table tmp_revision_intrinsic_metadata (
    like revision_intrinsic_metadata including defaults
  ) on commit drop;
$$;

create or replace function swh_origin_intrinsic_metadata_add(
        conflict_update boolean)
    returns void
    language plpgsql
as $$
begin
    perform swh_origin_intrinsic_metadata_compute_tsvector();
    if conflict_update then
      insert into origin_intrinsic_metadata (id, metadata, indexer_configuration_id, from_revision, metadata_tsvector, mappings)
      select id, metadata, indexer_configuration_id, from_revision,
             metadata_tsvector, mappings
      from tmp_origin_intrinsic_metadata
            on conflict(id, indexer_configuration_id)
                do update set
                    metadata = excluded.metadata,
                    mappings = excluded.mappings;

    else
        insert into origin_intrinsic_metadata (id, metadata, indexer_configuration_id, from_revision, metadata_tsvector, mappings)
        select id, metadata, indexer_configuration_id, from_revision,
               metadata_tsvector, mappings
      from tmp_origin_intrinsic_metadata
            on conflict(id, indexer_configuration_id)
            do nothing;
    end if;
    return;
end
$$;

alter index revision_metadata_pkey rename to revision_intrinsic_metadata_pkey;

alter table revision_metadata rename column translated_metadata to metadata;
alter table content_metadata rename column translated_metadata to metadata;
alter table origin_intrinsic_metadata rename column origin_id to id;

alter table revision_metadata rename to revision_intrinsic_metadata;
