-- SWH Indexer DB schema upgrade
-- from_version: 132
-- to_version: 133
-- description: remove 'conflict_update' argument

insert into dbversion(version, release, description)
      values(133, now(), 'Work In Progress');

drop function swh_content_mimetype_add(conflict_update boolean);
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
    on conflict(id, indexer_configuration_id)
    do update set mimetype = excluded.mimetype,
                  encoding = excluded.encoding;

    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_content_mimetype_add() IS 'Add new content mimetypes';



drop function swh_content_language_add(conflict_update boolean);
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
    on conflict(id, indexer_configuration_id)
    do update set lang = excluded.lang;

    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_content_language_add() IS 'Add new content languages';



drop function swh_content_ctags_add(conflict_update boolean);
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
    on conflict(id, hash_sha1(name), kind, line, lang, indexer_configuration_id)
    do nothing;

    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_content_ctags_add() IS 'Add new ctags symbols per content';



drop  function swh_content_fossology_license_add(conflict_update boolean);
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
    on conflict(id, license_id, indexer_configuration_id)
    do update set license_id = excluded.license_id;

    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_content_fossology_license_add() IS 'Add new content licenses';



drop function swh_content_metadata_add(conflict_update boolean);
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
    on conflict(id, indexer_configuration_id)
    do update set metadata = excluded.metadata;

    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_content_metadata_add() IS 'Add new content metadata';



drop function swh_revision_intrinsic_metadata_add(conflict_update boolean);
create or replace function swh_revision_intrinsic_metadata_add()
    returns bigint
    language plpgsql
as $$
declare
  res bigint;
begin
    insert into revision_intrinsic_metadata (id, metadata, mappings, indexer_configuration_id)
    select id, metadata, mappings, indexer_configuration_id
    from tmp_revision_intrinsic_metadata tcm
    on conflict(id, indexer_configuration_id)
    do update set
        metadata = excluded.metadata,
        mappings = excluded.mappings;

    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_revision_intrinsic_metadata_add() IS 'Add new revision intrinsic metadata';



drop function swh_origin_intrinsic_metadata_add(conflict_update boolean);
create or replace function swh_origin_intrinsic_metadata_add()
    returns bigint
    language plpgsql
as $$
declare
   res bigint;
begin
    perform swh_origin_intrinsic_metadata_compute_tsvector();

    insert into origin_intrinsic_metadata (id, metadata, indexer_configuration_id, from_revision, metadata_tsvector, mappings)
    select id, metadata, indexer_configuration_id, from_revision,
           metadata_tsvector, mappings
    from tmp_origin_intrinsic_metadata
    on conflict(id, indexer_configuration_id)
    do update set
        metadata = excluded.metadata,
        metadata_tsvector = excluded.metadata_tsvector,
        mappings = excluded.mappings,
        from_revision = excluded.from_revision;

    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_origin_intrinsic_metadata_add() IS 'Add new origin intrinsic metadata';
