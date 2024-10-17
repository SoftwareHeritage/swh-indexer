-- SWH Indexer DB schema upgrade
-- from_version: 127
-- to_version: 128
-- description: Add index on content_mimetype table to improve read queries

insert into dbversion(version, release, description)
values(128, now(), 'Work In Progress');

create index on content_mimetype(id) where mimetype like 'text/%';
