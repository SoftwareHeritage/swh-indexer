-- SWH Indexer DB schema upgrade
-- from_version: 118
-- to_version: 119
-- description: metadata tables: add 'mappings' column

insert into dbversion(version, release, description)
values(119, now(), 'Work In Progress');

alter table revision_metadata
  add column mappings text array not null default {};
alter table revision_metadata
  alter column mappings
    drop default;

alter table origin_intrinsic_metadata
  add column mappings text array not null default {};
alter table origin_intrinsic_metadata
  alter column mappings
    drop default;
