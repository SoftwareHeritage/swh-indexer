-- Postgresql index helper function
create or replace function hash_sha1(text)
    returns text
    language sql strict immutable
as $$
    select encode(public.digest($1, 'sha1'), 'hex')
$$;

comment on function hash_sha1(text) is 'Compute sha1 hash as text';

-- create a temporary table called tmp_TBLNAME, mimicking existing table
-- TBLNAME
--
-- Args:
--     tblname: name of the table to mimic
create or replace function swh_mktemp(tblname regclass)
    returns void
    language plpgsql
as $$
begin
    execute format('
	create temporary table if not exists tmp_%1$I
	    (like %1$I including defaults)
	    on commit delete rows;
      alter table tmp_%1$I drop column if exists object_id;
	', tblname);
    return;
end
$$;

-- create a temporary table for content_mimetype tmp_content_mimetype,
create or replace function swh_mktemp_content_mimetype()
    returns void
    language sql
as $$
  create temporary table if not exists tmp_content_mimetype (
    like content_mimetype including defaults
  ) on commit delete rows;
$$;

comment on function swh_mktemp_content_mimetype() IS 'Helper table to add mimetype information';

-- add tmp_content_mimetype entries to content_mimetype, overwriting duplicates.
--
-- If filtering duplicates is in order, the call to
-- swh_content_mimetype_missing must take place before calling this
-- function.
--
-- operates in bulk: 0. swh_mktemp(content_mimetype), 1. COPY to tmp_content_mimetype,
-- 2. call this function
create or replace function swh_content_mimetype_add()
    returns bigint
    language plpgsql
as $$
declare
  res bigint;
begin
    insert into content_mimetype (id, mimetype, encoding, indexer_configuration_id)
    select id, mimetype, encoding, indexer_configuration_id
    from tmp_content_mimetype tcm
    order by id, indexer_configuration_id
    on conflict(id, indexer_configuration_id)
    do update set mimetype = excluded.mimetype,
                  encoding = excluded.encoding;

    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_content_mimetype_add() IS 'Add new content mimetypes';

-- create a temporary table for content_fossology_license tmp_content_fossology_license,
create or replace function swh_mktemp_content_fossology_license()
    returns void
    language sql
as $$
  create temporary table if not exists tmp_content_fossology_license (
    id                       sha1,
    license                  text,
    indexer_configuration_id integer
  ) on commit delete rows;
$$;

comment on function swh_mktemp_content_fossology_license() is 'Helper table to add content license';

-- add tmp_content_fossology_license entries to content_fossology_license,
-- overwriting duplicates.
--
-- operates in bulk: 0. swh_mktemp(content_fossology_license), 1. COPY to
-- tmp_content_fossology_license, 2. call this function
create or replace function swh_content_fossology_license_add()
  returns bigint
  language plpgsql
as $$
declare
  res bigint;
begin
    -- insert unknown licenses first
    insert into fossology_license (name)
    select distinct license from tmp_content_fossology_license tmp
    where not exists (select 1 from fossology_license where name=tmp.license)
    on conflict(name) do nothing;

    insert into content_fossology_license (id, license_id, indexer_configuration_id)
    select tcl.id,
          (select id from fossology_license where name = tcl.license) as license,
          indexer_configuration_id
    from tmp_content_fossology_license tcl
    order by tcl.id, license, indexer_configuration_id
    on conflict(id, license_id, indexer_configuration_id)
    do update set license_id = excluded.license_id;

    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_content_fossology_license_add() IS 'Add new content licenses';


-- content_metadata functions

-- add tmp_content_metadata entries to content_metadata, overwriting duplicates
--
-- If filtering duplicates is in order, the call to
-- swh_content_metadata_missing must take place before calling this
-- function.
--
-- operates in bulk: 0. swh_mktemp(content_metadata), 1. COPY to
-- tmp_content_metadata, 2. call this function
create or replace function swh_content_metadata_add()
    returns bigint
    language plpgsql
as $$
declare
  res bigint;
begin
    insert into content_metadata (id, metadata, indexer_configuration_id)
    select id, metadata, indexer_configuration_id
    from tmp_content_metadata tcm
    order by id, indexer_configuration_id
    on conflict(id, indexer_configuration_id)
    do update set metadata = excluded.metadata;

    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_content_metadata_add() IS 'Add new content metadata';

-- create a temporary table for retrieving content_metadata
create or replace function swh_mktemp_content_metadata()
    returns void
    language sql
as $$
  create temporary table if not exists tmp_content_metadata (
    like content_metadata including defaults
  ) on commit delete rows;
$$;

comment on function swh_mktemp_content_metadata() is 'Helper table to add content metadata';

-- end content_metadata functions

-- add tmp_directory_intrinsic_metadata entries to directory_intrinsic_metadata,
-- overwriting duplicates.
--
-- If filtering duplicates is in order, the call to
-- swh_directory_intrinsic_metadata_missing must take place before calling this
-- function.
--
-- operates in bulk: 0. swh_mktemp(directory_intrinsic_metadata), 1. COPY to
-- tmp_directory_intrinsic_metadata, 2. call this function
create or replace function swh_directory_intrinsic_metadata_add()
    returns bigint
    language plpgsql
as $$
declare
  res bigint;
begin
    insert into directory_intrinsic_metadata (id, metadata, mappings, indexer_configuration_id)
    select id, metadata, mappings, indexer_configuration_id
    from tmp_directory_intrinsic_metadata tcm
    order by id, indexer_configuration_id
    on conflict(id, indexer_configuration_id)
    do update set
        metadata = excluded.metadata,
        mappings = excluded.mappings;

    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_directory_intrinsic_metadata_add() IS 'Add new directory intrinsic metadata';

-- create a temporary table for retrieving directory_intrinsic_metadata
create or replace function swh_mktemp_directory_intrinsic_metadata()
    returns void
    language sql
as $$
  create temporary table if not exists tmp_directory_intrinsic_metadata (
    like directory_intrinsic_metadata including defaults
  ) on commit delete rows;
$$;

comment on function swh_mktemp_directory_intrinsic_metadata() is 'Helper table to add directory intrinsic metadata';

-- create a temporary table for retrieving origin_intrinsic_metadata
create or replace function swh_mktemp_origin_intrinsic_metadata()
    returns void
    language sql
as $$
  create temporary table if not exists tmp_origin_intrinsic_metadata (
    like origin_intrinsic_metadata including defaults
  ) on commit delete rows;
$$;

comment on function swh_mktemp_origin_intrinsic_metadata() is 'Helper table to add origin intrinsic metadata';

create or replace function swh_mktemp_indexer_configuration()
    returns void
    language sql
as $$
    create temporary table if not exists tmp_indexer_configuration (
      like indexer_configuration including defaults
    ) on commit delete rows;
    alter table tmp_indexer_configuration drop column if exists id;
$$;

-- add tmp_origin_intrinsic_metadata entries to origin_intrinsic_metadata,
-- overwriting duplicates.
--
-- If filtering duplicates is in order, the call to
-- swh_origin_intrinsic_metadata_missing must take place before calling this
-- function.
--
-- operates in bulk: 0. swh_mktemp(origin_intrinsic_metadata), 1. COPY to
-- tmp_origin_intrinsic_metadata, 2. call this function
create or replace function swh_origin_intrinsic_metadata_add()
    returns bigint
    language plpgsql
as $$
declare
  res bigint;
begin
    perform swh_origin_intrinsic_metadata_compute_tsvector();

    insert into origin_intrinsic_metadata (id, metadata, indexer_configuration_id, from_directory, metadata_tsvector, mappings)
    select id, metadata, indexer_configuration_id, from_directory,
           metadata_tsvector, mappings
    from tmp_origin_intrinsic_metadata
    order by id, indexer_configuration_id
    on conflict(id, indexer_configuration_id)
    do update set
        metadata = excluded.metadata,
        metadata_tsvector = excluded.metadata_tsvector,
        mappings = excluded.mappings,
        from_directory = excluded.from_directory;

    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_origin_intrinsic_metadata_add() IS 'Add new origin intrinsic metadata';


-- Compute the metadata_tsvector column in tmp_origin_intrinsic_metadata.
--
-- It uses the "pg_catalog.simple" dictionary, as it has no stopword,
-- so it should be suitable for proper names and non-English text.
create or replace function swh_origin_intrinsic_metadata_compute_tsvector()
    returns void
    language plpgsql
as $$
begin
    update tmp_origin_intrinsic_metadata
        set metadata_tsvector = to_tsvector('pg_catalog.simple', metadata);
end
$$;

-- create a temporary table for retrieving origin_extrinsic_metadata
create or replace function swh_mktemp_origin_extrinsic_metadata()
    returns void
    language sql
as $$
  create temporary table if not exists tmp_origin_extrinsic_metadata (
    like origin_extrinsic_metadata including defaults
  ) on commit delete rows;
$$;

comment on function swh_mktemp_origin_extrinsic_metadata() is 'Helper table to add origin extrinsic metadata';

create or replace function swh_mktemp_indexer_configuration()
    returns void
    language sql
as $$
    create temporary table if not exists tmp_indexer_configuration (
      like indexer_configuration including defaults
    ) on commit delete rows;
    alter table tmp_indexer_configuration drop column if exists id;
$$;

-- add tmp_origin_extrinsic_metadata entries to origin_extrinsic_metadata,
-- overwriting duplicates.
--
-- If filtering duplicates is in order, the call to
-- swh_origin_extrinsic_metadata_missing must take place before calling this
-- function.
--
-- operates in bulk: 0. swh_mktemp(origin_extrinsic_metadata), 1. COPY to
-- tmp_origin_extrinsic_metadata, 2. call this function
create or replace function swh_origin_extrinsic_metadata_add()
    returns bigint
    language plpgsql
as $$
declare
  res bigint;
begin
    perform swh_origin_extrinsic_metadata_compute_tsvector();

    insert into origin_extrinsic_metadata (id, metadata, indexer_configuration_id, from_remd_id, metadata_tsvector, mappings)
    select id, metadata, indexer_configuration_id, from_remd_id,
           metadata_tsvector, mappings
    from tmp_origin_extrinsic_metadata
    order by id, indexer_configuration_id
    on conflict(id, indexer_configuration_id)
    do update set
        metadata = excluded.metadata,
        metadata_tsvector = excluded.metadata_tsvector,
        mappings = excluded.mappings,
        from_remd_id = excluded.from_remd_id;

    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_origin_extrinsic_metadata_add() IS 'Add new origin extrinsic metadata';


-- Compute the metadata_tsvector column in tmp_origin_extrinsic_metadata.
--
-- It uses the "pg_catalog.simple" dictionary, as it has no stopword,
-- so it should be suitable for proper names and non-English text.
create or replace function swh_origin_extrinsic_metadata_compute_tsvector()
    returns void
    language plpgsql
as $$
begin
    update tmp_origin_extrinsic_metadata
        set metadata_tsvector = to_tsvector('pg_catalog.simple', metadata);
end
$$;


-- add tmp_indexer_configuration entries to indexer_configuration,
-- overwriting duplicates if any.
--
-- operates in bulk: 0. create temporary tmp_indexer_configuration, 1. COPY to
-- it, 2. call this function to insert and filtering out duplicates
create or replace function swh_indexer_configuration_add()
    returns setof indexer_configuration
    language plpgsql
as $$
begin
      insert into indexer_configuration(tool_name, tool_version, tool_configuration)
      select tool_name, tool_version, tool_configuration from tmp_indexer_configuration tmp
      order by tool_name, tool_version, tool_configuration
      on conflict(tool_name, tool_version, tool_configuration) do nothing;

      return query
          select id, tool_name, tool_version, tool_configuration
          from tmp_indexer_configuration join indexer_configuration
              using(tool_name, tool_version, tool_configuration);

      return;
end
$$;
