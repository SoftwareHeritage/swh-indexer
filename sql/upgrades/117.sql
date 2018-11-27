-- SWH Indexer DB schema upgrade
-- from_version: 116
-- to_version: 117
-- description: Add fulltext search index for origin intrinsic metadata

insert into dbversion(version, release, description)
values(117, now(), 'Work In Progress');

alter table origin_intrinsic_metadata add column metadata_tsvector tsvector;
update origin_intrinsic_metadata set metadata_tsvector = to_tsvector('pg_catalog.simple', metadata);
create index origin_intrinsic_metadata_fulltext_idx on origin_intrinsic_metadata using gin (metadata_tsvector);
