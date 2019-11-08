-- SWH Indexer DB schema upgrade
-- from_version: 126
-- to_version: 127
-- description: Remove swh_origin_intrinsic_metadata_add origin_url field and
--              replace id by the former content of origin_url

insert into dbversion(version, release, description)
values(127, now(), 'Work In Progress');

-- replace id column by origin_url
alter table origin_intrinsic_metadata
      drop constraint origin_intrinsic_metadata_indexer_configuration_id_fkey;
alter table origin_intrinsic_metadata
      drop constraint origin_intrinsic_metadata_pkey;
alter table origin_intrinsic_metadata
      drop column id;
alter table origin_intrinsic_metadata
      rename column origin_url to id;
comment on column origin_intrinsic_metadata.id is 'url of the origin';

-- replace functions that operate on this table
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
                    metadata_tsvector = excluded.metadata_tsvector,
                    mappings = excluded.mappings,
                    from_revision = excluded.from_revision;

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
comment on function swh_origin_intrinsic_metadata_add(boolean) IS 'Add new origin intrinsic metadata';

-- recreate indexes/constraints on this table
create unique index origin_intrinsic_metadata_pkey
       on origin_intrinsic_metadata(id, indexer_configuration_id);
alter table origin_intrinsic_metadata
      add primary key using index origin_intrinsic_metadata_pkey;

alter table origin_intrinsic_metadata
      add constraint origin_intrinsic_metadata_indexer_configuration_id_fkey foreign key (indexer_configuration_id) references indexer_configuration(id) not valid;
alter table origin_intrinsic_metadata
      validate constraint origin_intrinsic_metadata_indexer_configuration_id_fkey;
