-- SWH Indexer DB schema upgrade
-- from_version: 135
-- to_version: 136
-- description: Insert from temporary tables in consistent order

insert into dbversion(version, release, description)
      values(136, now(), 'Work In Progress');


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


create or replace function swh_content_language_add()
    returns bigint
    language plpgsql
as $$
declare
  res bigint;
begin
    insert into content_language (id, lang, indexer_configuration_id)
    select id, lang, indexer_configuration_id
    from tmp_content_language tcl
    order by id, indexer_configuration_id
    on conflict(id, indexer_configuration_id)
    do update set lang = excluded.lang;

    get diagnostics res = ROW_COUNT;
    return res;
end
$$;


create or replace function swh_content_ctags_add()
    returns bigint
    language plpgsql
as $$
declare
  res bigint;
begin
    insert into content_ctags (id, name, kind, line, lang, indexer_configuration_id)
    select id, name, kind, line, lang, indexer_configuration_id
    from tmp_content_ctags tct
    order by id, hash_sha1(name), kind, line, lang, indexer_configuration_id
    on conflict(id, hash_sha1(name), kind, line, lang, indexer_configuration_id)
    do nothing;

    get diagnostics res = ROW_COUNT;
    return res;
end
$$;


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


