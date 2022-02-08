-- SWH Indexer DB schema upgrade
-- from_version: 119
-- to_version: 120
-- description: fix updates of the 'mappings' column in metadata tables

insert into dbversion(version, release, description)
values(120, now(), 'Work In Progress');

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
                do update set
                    translated_metadata = excluded.translated_metadata,
                    mappings = excluded.mappings;

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
                do update set
                    metadata = excluded.metadata,
                    mappings = excluded.mappings;

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
