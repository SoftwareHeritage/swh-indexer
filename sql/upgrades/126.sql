-- SWH Indexer DB schema upgrade
-- from_version: 125
-- to_version: 126
-- description: Make swh_origin_intrinsic_metadata_add update all fields

insert into dbversion(version, release, description)
values(126, now(), 'Work In Progress');


create or replace function swh_origin_intrinsic_metadata_add(
        conflict_update boolean)
    returns void
    language plpgsql
as $$
begin
    perform swh_origin_intrinsic_metadata_compute_tsvector();
    if conflict_update then
      insert into origin_intrinsic_metadata (id, origin_url, metadata, indexer_configuration_id, from_revision, metadata_tsvector, mappings)
      select id, origin_url, metadata, indexer_configuration_id, from_revision,
             metadata_tsvector, mappings
    	from tmp_origin_intrinsic_metadata
            on conflict(id, indexer_configuration_id)
                do update set
                    metadata = excluded.metadata,
                    metadata_tsvector = excluded.metadata_tsvector,
                    mappings = excluded.mappings,
                    origin_url = excluded.origin_url,
                    from_revision = excluded.from_revision;

    else
        insert into origin_intrinsic_metadata (id, origin_url, metadata, indexer_configuration_id, from_revision, metadata_tsvector, mappings)
        select id, origin_url, metadata, indexer_configuration_id, from_revision,
               metadata_tsvector, mappings
    	from tmp_origin_intrinsic_metadata
            on conflict(id, indexer_configuration_id)
            do nothing;
    end if;
    return;
end
$$;

comment on function swh_origin_intrinsic_metadata_add(boolean) IS 'Add new origin intrinsic metadata';
