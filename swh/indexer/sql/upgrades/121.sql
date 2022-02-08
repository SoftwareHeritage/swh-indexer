-- SWH Indexer DB schema upgrade
-- from_version: 120
-- to_version: 121
-- description: add comment to the 'mappings' column

insert into dbversion(version, release, description)
values(121, now(), 'Work In Progress');

comment on column revision_metadata.mappings is 'type of metadata files used to obtain this metadata (eg. pkg-info, npm)';
comment on column origin_intrinsic_metadata.mappings is 'type of metadata files used to obtain this metadata (eg. pkg-info, npm)';
