-- SWH Indexer DB schema upgrade
-- from_version: 124
-- to_version: 125
-- description: Add 'origin_url' column to origin_intrinsic_metadata.

insert into dbversion(version, release, description)
values(125, now(), 'Work In Progress');

alter origin_intrinsic_metadata
    add column origin_url type text;

