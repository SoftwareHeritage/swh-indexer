-- SWH Indexer DB schema upgrade
-- from_version: 130
-- to_version: 131
-- description: _add function returns the inserted rows

insert into dbversion(version, release, description)
      values(131, now(), 'Work In Progress');

drop function swh_content_mimetype_add(boolean);
drop function swh_content_language_add(boolean);
drop function swh_content_ctags_add(boolean);
drop function swh_content_fossology_license_add(boolean);
drop function swh_content_metadata_add(boolean);
drop function swh_revision_intrinsic_metadata_add(boolean);
drop function swh_origin_intrinsic_metadata_add(boolean);

create or replace function swh_content_mimetype_add(conflict_update boolean)
    returns bigint
    language plpgsql
as $$
declare
  res bigint;
begin
    if conflict_update then
        insert into content_mimetype (id, mimetype, encoding, indexer_configuration_id)
        select id, mimetype, encoding, indexer_configuration_id
        from tmp_content_mimetype tcm
        on conflict(id, indexer_configuration_id)
        do update set mimetype = excluded.mimetype,
                      encoding = excluded.encoding;
    else
        insert into content_mimetype (id, mimetype, encoding, indexer_configuration_id)
        select id, mimetype, encoding, indexer_configuration_id
        from tmp_content_mimetype tcm
        on conflict(id, indexer_configuration_id)
        do nothing;
    end if;
    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_content_mimetype_add(boolean) IS 'Add new content mimetypes';

create or replace function swh_content_language_add(conflict_update boolean)
    returns bigint
    language plpgsql
as $$
declare
  res bigint;
begin
    if conflict_update then
      insert into content_language (id, lang, indexer_configuration_id)
      select id, lang, indexer_configuration_id
      from tmp_content_language tcl
      on conflict(id, indexer_configuration_id)
      do update set lang = excluded.lang;
    else
        insert into content_language (id, lang, indexer_configuration_id)
        select id, lang, indexer_configuration_id
    	from tmp_content_language tcl
        on conflict(id, indexer_configuration_id)
        do nothing;
    end if;
    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_content_language_add(boolean) IS 'Add new content languages';


create or replace function swh_content_ctags_add(conflict_update boolean)
    returns bigint
    language plpgsql
as $$
declare
  res bigint;
begin
    if conflict_update then
        delete from content_ctags
        where id in (select tmp.id
                     from tmp_content_ctags tmp
                     inner join indexer_configuration i on i.id=tmp.indexer_configuration_id);
    end if;

    insert into content_ctags (id, name, kind, line, lang, indexer_configuration_id)
    select id, name, kind, line, lang, indexer_configuration_id
    from tmp_content_ctags tct
    on conflict(id, hash_sha1(name), kind, line, lang, indexer_configuration_id)
    do nothing;
    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_content_ctags_add(boolean) IS 'Add new ctags symbols per content';

create or replace function swh_content_fossology_license_add(conflict_update boolean)
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

    if conflict_update then
        insert into content_fossology_license (id, license_id, indexer_configuration_id)
        select tcl.id,
              (select id from fossology_license where name = tcl.license) as license,
              indexer_configuration_id
        from tmp_content_fossology_license tcl
        on conflict(id, license_id, indexer_configuration_id)
        do update set license_id = excluded.license_id;
    end if;

    insert into content_fossology_license (id, license_id, indexer_configuration_id)
    select tcl.id,
          (select id from fossology_license where name = tcl.license) as license,
          indexer_configuration_id
    from tmp_content_fossology_license tcl
    on conflict(id, license_id, indexer_configuration_id)
    do nothing;

    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

create or replace function swh_content_metadata_add(conflict_update boolean)
    returns bigint
    language plpgsql
as $$
declare
  res bigint;
begin
    if conflict_update then
      insert into content_metadata (id, metadata, indexer_configuration_id)
      select id, metadata, indexer_configuration_id
      from tmp_content_metadata tcm
      on conflict(id, indexer_configuration_id)
      do update set metadata = excluded.metadata;
    else
        insert into content_metadata (id, metadata, indexer_configuration_id)
        select id, metadata, indexer_configuration_id
    	from tmp_content_metadata tcm
        on conflict(id, indexer_configuration_id)
        do nothing;
    end if;
    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_content_metadata_add(boolean) IS 'Add new content metadata';


create or replace function swh_revision_intrinsic_metadata_add(conflict_update boolean)
    returns bigint
    language plpgsql
as $$
declare
  res bigint;
begin
    if conflict_update then
      insert into revision_intrinsic_metadata (id, metadata, mappings, indexer_configuration_id)
      select id, metadata, mappings, indexer_configuration_id
      from tmp_revision_intrinsic_metadata tcm
      on conflict(id, indexer_configuration_id)
      do update set
          metadata = excluded.metadata,
          mappings = excluded.mappings;
    else
        insert into revision_intrinsic_metadata (id, metadata, mappings, indexer_configuration_id)
        select id, metadata, mappings, indexer_configuration_id
    	from tmp_revision_intrinsic_metadata tcm
        on conflict(id, indexer_configuration_id)
        do nothing;
    end if;
    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_revision_intrinsic_metadata_add(boolean) IS 'Add new revision intrinsic metadata';

create or replace function swh_origin_intrinsic_metadata_add(
        conflict_update boolean)
    returns bigint
    language plpgsql
as $$
declare
  res bigint;
begin
    perform swh_origin_intrinsic_metadata_compute_tsvector();
    if conflict_update then
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
    else
        insert into origin_intrinsic_metadata (id, metadata, indexer_configuration_id, from_revision, metadata_tsvector, mappings)
        select id, metadata, indexer_configuration_id, from_revision,
               metadata_tsvector, mappings
    	from tmp_origin_intrinsic_metadata
        on conflict(id, indexer_configuration_id)
        do nothing;
    end if;
    get diagnostics res = ROW_COUNT;
    return res;
end
$$;
