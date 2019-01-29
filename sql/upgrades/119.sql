-- SWH Indexer DB schema upgrade
-- from_version: 118
-- to_version: 119
-- description: metadata tables: add 'mappings' column

insert into dbversion(version, release, description)
values(119, now(), 'Work In Progress');

alter table revision_metadata
  add column mappings text array not null default '{}';
alter table revision_metadata
  alter column mappings
    drop default;

alter table origin_intrinsic_metadata
  add column mappings text array not null default '{}';
alter table origin_intrinsic_metadata
  alter column mappings
    drop default;


create or replace function swh_revision_metadata_add(conflict_update boolean)
    returns void
    language plpgsql
as $$
begin
    if conflict_update then
      insert into revision_metadata (id, translated_metadata, mappings, indexer_configuration_id)
      select id, translated_metadata, mappings, indexer_configuration_id
    	from tmp_revision_metadata tcm
            on conflict(id, indexer_configuration_id)
                do update set translated_metadata = excluded.translated_metadata;

    else
        insert into revision_metadata (id, translated_metadata, mappings, indexer_configuration_id)
        select id, translated_metadata, mappings, indexer_configuration_id
    	from tmp_revision_metadata tcm
            on conflict(id, indexer_configuration_id)
            do nothing;
    end if;
    return;
end
$$;


create or replace function swh_origin_intrinsic_metadata_add(
        conflict_update boolean)
    returns void
    language plpgsql
as $$
begin
    perform swh_origin_intrinsic_metadata_compute_tsvector();
    if conflict_update then
      insert into origin_intrinsic_metadata (origin_id, metadata, indexer_configuration_id, from_revision, metadata_tsvector, mappings)
      select origin_id, metadata, indexer_configuration_id, from_revision,
             metadata_tsvector, mappings
    	from tmp_origin_intrinsic_metadata
            on conflict(origin_id, indexer_configuration_id)
                do update set metadata = excluded.metadata;

    else
        insert into origin_intrinsic_metadata (origin_id, metadata, indexer_configuration_id, from_revision, metadata_tsvector, mappings)
        select origin_id, metadata, indexer_configuration_id, from_revision,
               metadata_tsvector, mappings
    	from tmp_origin_intrinsic_metadata
            on conflict(origin_id, indexer_configuration_id)
            do nothing;
    end if;
    return;
end
$$;


-- Compute the metadata_tsvector column in tmp_origin_intrinsic_metadata.
--
-- It uses the "pg_catalog.simple" dictionary, as it has no stopword,
-- so it should be suitable for proper names and non-English text.
create or replace function swh_origin_intrinsic_metadata_compute_tsvector()
    returns void
    language plpgsql
as $$
begin
    update tmp_origin_intrinsic_metadata
        set metadata_tsvector = to_tsvector('pg_catalog.simple', metadata);
end
$$;
