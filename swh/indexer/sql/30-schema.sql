---
--- Software Heritage Indexers Data Model
---

-- drop schema if exists swh cascade;
-- create schema swh;
-- set search_path to swh;

create table dbversion
(
  version     int primary key,
  release     timestamptz,
  description text
);

insert into dbversion(version, release, description)
      values(133, now(), 'Work In Progress');
-- Computing metadata on sha1's contents

-- a SHA1 checksum (not necessarily originating from Git)
create domain sha1 as bytea check (length(value) = 20);

-- a Git object ID, i.e., a SHA1 checksum
create domain sha1_git as bytea check (length(value) = 20);

create table indexer_configuration (
  id serial not null,
  tool_name text not null,
  tool_version text not null,
  tool_configuration jsonb
);

comment on table indexer_configuration is 'Indexer''s configuration version';
comment on column indexer_configuration.id is 'Tool identifier';
comment on column indexer_configuration.tool_version is 'Tool name';
comment on column indexer_configuration.tool_version is 'Tool version';
comment on column indexer_configuration.tool_configuration is 'Tool configuration: command line, flags, etc...';

-- Properties (mimetype, encoding, etc...)
create table content_mimetype (
  id sha1 not null,
  mimetype text not null,
  encoding text not null,
  indexer_configuration_id bigint not null
);

comment on table content_mimetype is 'Metadata associated to a raw content';
comment on column content_mimetype.mimetype is 'Raw content Mimetype';
comment on column content_mimetype.encoding is 'Raw content encoding';
comment on column content_mimetype.indexer_configuration_id is 'Tool used to compute the information';

-- Language metadata
create table content_language (
  id sha1 not null,
  lang languages not null,
  indexer_configuration_id bigint not null
);

comment on table content_language is 'Language information on a raw content';
comment on column content_language.lang is 'Language information';
comment on column content_language.indexer_configuration_id is 'Tool used to compute the information';

-- ctags information per content
create table content_ctags (
  id sha1 not null,
  name text not null,
  kind text not null,
  line bigint not null,
  lang ctags_languages not null,
  indexer_configuration_id bigint not null
);

comment on table content_ctags is 'Ctags information on a raw content';
comment on column content_ctags.id is 'Content identifier';
comment on column content_ctags.name is 'Symbol name';
comment on column content_ctags.kind is 'Symbol kind (function, class, variable, const...)';
comment on column content_ctags.line is 'Symbol line';
comment on column content_ctags.lang is 'Language information for that content';
comment on column content_ctags.indexer_configuration_id is 'Tool used to compute the information';

create table fossology_license(
  id smallserial,
  name text not null
);

comment on table fossology_license is 'Possible license recognized by license indexer';
comment on column fossology_license.id is 'License identifier';
comment on column fossology_license.name is 'License name';

create table content_fossology_license (
  id sha1 not null,
  license_id smallserial not null,
  indexer_configuration_id bigint not null
);

comment on table content_fossology_license is 'license associated to a raw content';
comment on column content_fossology_license.id is 'Raw content identifier';
comment on column content_fossology_license.license_id is 'One of the content''s license identifier';
comment on column content_fossology_license.indexer_configuration_id is 'Tool used to compute the information';


-- The table content_metadata provides a translation to files
-- identified as potentially containning metadata with a translation tool (indexer_configuration_id)
create table content_metadata(
  id                       sha1   not null,
  metadata                 jsonb  not null,
  indexer_configuration_id bigint not null
);

comment on table content_metadata is 'metadata semantically translated from a content file';
comment on column content_metadata.id is 'sha1 of content file';
comment on column content_metadata.metadata is 'result of translation with defined format';
comment on column content_metadata.indexer_configuration_id is 'tool used for translation';

-- The table revision_intrinsic_metadata provides a minimal set of intrinsic
-- metadata detected with the detection  tool (indexer_configuration_id) and
-- aggregated from the content_metadata translation.
create table revision_intrinsic_metadata(
  id                       sha1_git   not null,
  metadata                 jsonb      not null,
  indexer_configuration_id bigint     not null,
  mappings                 text array not null
);

comment on table revision_intrinsic_metadata is 'metadata semantically detected and translated in a revision';
comment on column revision_intrinsic_metadata.id is 'sha1_git of revision';
comment on column revision_intrinsic_metadata.metadata is 'result of detection and translation with defined format';
comment on column revision_intrinsic_metadata.indexer_configuration_id is 'tool used for detection';
comment on column revision_intrinsic_metadata.mappings is 'type of metadata files used to obtain this metadata (eg. pkg-info, npm)';

create table origin_intrinsic_metadata(
  id                        text       not null,  -- origin url
  metadata                  jsonb,
  indexer_configuration_id  bigint     not null,
  from_revision             sha1_git   not null,
  metadata_tsvector         tsvector,
  mappings                  text array not null
);

comment on table origin_intrinsic_metadata is 'keeps intrinsic metadata for an origin';
comment on column origin_intrinsic_metadata.id is 'url of the origin';
comment on column origin_intrinsic_metadata.metadata is 'metadata extracted from a revision';
comment on column origin_intrinsic_metadata.indexer_configuration_id is 'tool used to generate this metadata';
comment on column origin_intrinsic_metadata.from_revision is 'sha1 of the revision this metadata was copied from.';
comment on column origin_intrinsic_metadata.mappings is 'type of metadata files used to obtain this metadata (eg. pkg-info, npm)';
