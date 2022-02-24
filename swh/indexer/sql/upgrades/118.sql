-- SWH Indexer DB schema upgrade
-- from_version: 117
-- to_version: 118
-- description: content_mimetype: Migrate bytes column to text

insert into dbversion(version, release, description)
values(118, now(), 'Work In Progress');

alter table content_mimetype
  alter column mimetype set data type text
    using convert_from(mimetype, 'utf-8'),
  alter column encoding set data type text
    using convert_from(encoding, 'utf-8');
