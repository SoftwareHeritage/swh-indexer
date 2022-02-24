-- SWH Indexer DB schema upgrade
-- from_version: 123
-- to_version: 124
-- description: drop constraint that origin_intrinsic_metadata references an existing revision_intrinsic_metadata.

insert into dbversion(version, release, description)
values(124, now(), 'Work In Progress');

alter table origin_intrinsic_metadata drop constraint origin_intrinsic_metadata_revision_metadata_fkey;
