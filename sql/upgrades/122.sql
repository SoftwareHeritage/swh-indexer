-- SWH Indexer DB schema upgrade
-- from_version: 121
-- to_version: 122
-- description: add index to search origin_intrinsic_metadata for mappings.

insert into dbversion(version, release, description)
values(122, now(), 'Work In Progress');

create index origin_intrinsic_metadata_mappings_idx on origin_intrinsic_metadata using gin (mappings);
