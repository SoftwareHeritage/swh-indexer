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
--     tblname: name of the table to mimick
create or replace function swh_mktemp(tblname regclass)
    returns void
    language plpgsql
as $$
begin
    execute format('
	create temporary table tmp_%1$I
	    (like %1$I including defaults)
	    on commit drop;
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
  create temporary table tmp_content_mimetype (
    like content_mimetype including defaults
  ) on commit drop;
$$;

comment on function swh_mktemp_content_mimetype() IS 'Helper table to add mimetype information';

-- add tmp_content_mimetype entries to content_mimetype, overwriting
-- duplicates if conflict_update is true, skipping duplicates otherwise.
--
-- If filtering duplicates is in order, the call to
-- swh_content_mimetype_missing must take place before calling this
-- function.
--
--
-- operates in bulk: 0. swh_mktemp(content_mimetype), 1. COPY to tmp_content_mimetype,
-- 2. call this function
create or replace function swh_content_mimetype_add(conflict_update boolean)
    returns void
    language plpgsql
as $$
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
            on conflict(id, indexer_configuration_id) do nothing;
    end if;
    return;
end
$$;

comment on function swh_content_mimetype_add(boolean) IS 'Add new content mimetypes';

-- add tmp_content_language entries to content_language, overwriting
-- duplicates if conflict_update is true, skipping duplicates otherwise.
--
-- If filtering duplicates is in order, the call to
-- swh_content_language_missing must take place before calling this
-- function.
--
-- operates in bulk: 0. swh_mktemp(content_language), 1. COPY to
-- tmp_content_language, 2. call this function
create or replace function swh_content_language_add(conflict_update boolean)
    returns void
    language plpgsql
as $$
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
    return;
end
$$;

comment on function swh_content_language_add(boolean) IS 'Add new content languages';

-- create a temporary table for retrieving content_language
create or replace function swh_mktemp_content_language()
    returns void
    language sql
as $$
  create temporary table tmp_content_language (
    like content_language including defaults
  ) on commit drop;
$$;

comment on function swh_mktemp_content_language() is 'Helper table to add content language';


-- create a temporary table for content_ctags tmp_content_ctags,
create or replace function swh_mktemp_content_ctags()
    returns void
    language sql
as $$
  create temporary table tmp_content_ctags (
    like content_ctags including defaults
  ) on commit drop;
$$;

comment on function swh_mktemp_content_ctags() is 'Helper table to add content ctags';


-- add tmp_content_ctags entries to content_ctags, overwriting
-- duplicates if conflict_update is true, skipping duplicates otherwise.
--
-- operates in bulk: 0. swh_mktemp(content_ctags), 1. COPY to tmp_content_ctags,
-- 2. call this function
create or replace function swh_content_ctags_add(conflict_update boolean)
    returns void
    language plpgsql
as $$
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
    return;
end
$$;

comment on function swh_content_ctags_add(boolean) IS 'Add new ctags symbols per content';

create type content_ctags_signature as (
  id sha1,
  name text,
  kind text,
  line bigint,
  lang ctags_languages,
  tool_id integer,
  tool_name text,
  tool_version text,
  tool_configuration jsonb
);

-- Search within ctags content.
--
create or replace function swh_content_ctags_search(
       expression text,
       l integer default 10,
       last_sha1 sha1 default '\x0000000000000000000000000000000000000000')
    returns setof content_ctags_signature
    language sql
as $$
    select c.id, name, kind, line, lang,
           i.id as tool_id, tool_name, tool_version, tool_configuration
    from content_ctags c
    inner join indexer_configuration i on i.id = c.indexer_configuration_id
    where hash_sha1(name) = hash_sha1(expression)
    and c.id > last_sha1
    order by id
    limit l;
$$;

comment on function swh_content_ctags_search(text, integer, sha1) IS 'Equality search through ctags'' symbols';


-- create a temporary table for content_fossology_license tmp_content_fossology_license,
create or replace function swh_mktemp_content_fossology_license()
    returns void
    language sql
as $$
  create temporary table tmp_content_fossology_license (
    id                       sha1,
    license                  text,
    indexer_configuration_id integer
  ) on commit drop;
$$;

comment on function swh_mktemp_content_fossology_license() is 'Helper table to add content license';

-- add tmp_content_fossology_license entries to content_fossology_license, overwriting
-- duplicates if conflict_update is true, skipping duplicates otherwise.
--
-- operates in bulk: 0. swh_mktemp(content_fossology_license), 1. COPY to
-- tmp_content_fossology_license, 2. call this function
create or replace function swh_content_fossology_license_add(conflict_update boolean)
    returns void
    language plpgsql
as $$
begin
    -- insert unknown licenses first
    insert into fossology_license (name)
    select distinct license from tmp_content_fossology_license tmp
    where not exists (select 1 from fossology_license where name=tmp.license)
    on conflict(name) do nothing;

    if conflict_update then
        -- delete from content_fossology_license c
        --   using tmp_content_fossology_license tmp, indexer_configuration i
        --   where c.id = tmp.id and i.id=tmp.indexer_configuration_id
        delete from content_fossology_license
        where id in (select tmp.id
                     from tmp_content_fossology_license tmp
                     inner join indexer_configuration i on i.id=tmp.indexer_configuration_id);
    end if;

    insert into content_fossology_license (id, license_id, indexer_configuration_id)
    select tcl.id,
          (select id from fossology_license where name = tcl.license) as license,
          indexer_configuration_id
    from tmp_content_fossology_license tcl
        on conflict(id, license_id, indexer_configuration_id)
        do nothing;
    return;
end
$$;

comment on function swh_content_fossology_license_add(boolean) IS 'Add new content licenses';

-- content_metadata functions

-- add tmp_content_metadata entries to content_metadata, overwriting
-- duplicates if conflict_update is true, skipping duplicates otherwise.
--
-- If filtering duplicates is in order, the call to
-- swh_content_metadata_missing must take place before calling this
-- function.
--
-- operates in bulk: 0. swh_mktemp(content_language), 1. COPY to
-- tmp_content_metadata, 2. call this function
create or replace function swh_content_metadata_add(conflict_update boolean)
    returns void
    language plpgsql
as $$
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
    return;
end
$$;

comment on function swh_content_metadata_add(boolean) IS 'Add new content metadata';

-- create a temporary table for retrieving content_metadata
create or replace function swh_mktemp_content_metadata()
    returns void
    language sql
as $$
  create temporary table tmp_content_metadata (
    like content_metadata including defaults
  ) on commit drop;
$$;

comment on function swh_mktemp_content_metadata() is 'Helper table to add content metadata';

-- end content_metadata functions

-- add tmp_revision_intrinsic_metadata entries to revision_intrinsic_metadata,
-- overwriting duplicates if conflict_update is true, skipping duplicates
-- otherwise.
--
-- If filtering duplicates is in order, the call to
-- swh_revision_intrinsic_metadata_missing must take place before calling this
-- function.
--
-- operates in bulk: 0. swh_mktemp(content_language), 1. COPY to
-- tmp_revision_intrinsic_metadata, 2. call this function
create or replace function swh_revision_intrinsic_metadata_add(conflict_update boolean)
    returns void
    language plpgsql
as $$
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
    return;
end
$$;

comment on function swh_revision_intrinsic_metadata_add(boolean) IS 'Add new revision intrinsic metadata';

-- create a temporary table for retrieving revision_intrinsic_metadata
create or replace function swh_mktemp_revision_intrinsic_metadata()
    returns void
    language sql
as $$
  create temporary table tmp_revision_intrinsic_metadata (
    like revision_intrinsic_metadata including defaults
  ) on commit drop;
$$;

comment on function swh_mktemp_revision_intrinsic_metadata() is 'Helper table to add revision intrinsic metadata';

-- create a temporary table for retrieving origin_intrinsic_metadata
create or replace function swh_mktemp_origin_intrinsic_metadata()
    returns void
    language sql
as $$
  create temporary table tmp_origin_intrinsic_metadata (
    like origin_intrinsic_metadata including defaults
  ) on commit drop;
$$;

comment on function swh_mktemp_origin_intrinsic_metadata() is 'Helper table to add origin intrinsic metadata';

create or replace function swh_mktemp_indexer_configuration()
    returns void
    language sql
as $$
    create temporary table tmp_indexer_configuration (
      like indexer_configuration including defaults
    ) on commit drop;
    alter table tmp_indexer_configuration drop column id;
$$;


-- add tmp_indexer_configuration entries to indexer_configuration,
-- skipping duplicates if any.
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
      on conflict(tool_name, tool_version, tool_configuration) do nothing;

      return query
          select id, tool_name, tool_version, tool_configuration
          from tmp_indexer_configuration join indexer_configuration
              using(tool_name, tool_version, tool_configuration);

      return;
end
$$;

-- add tmp_origin_intrinsic_metadata entries to origin_intrinsic_metadata,
-- overwriting duplicates if conflict_update is true, skipping duplicates
-- otherwise.
--
-- If filtering duplicates is in order, the call to
-- swh_origin_intrinsic_metadata_missing must take place before calling this
-- function.
--
-- operates in bulk: 0. swh_mktemp(content_language), 1. COPY to
-- tmp_origin_intrinsic_metadata, 2. call this function
create or replace function swh_origin_intrinsic_metadata_add(
        conflict_update boolean)
    returns void
    language plpgsql
as $$
begin
    perform swh_origin_intrinsic_metadata_compute_tsvector();
    if conflict_update then
      insert into origin_intrinsic_metadata (id, origin_url, metadata, indexer_configuration_id, from_revision, metadata_tsvector, mappings)
      select id, origin_url, metadata, indexer_configuration_id, from_revision,
             metadata_tsvector, mappings
    	from tmp_origin_intrinsic_metadata
            on conflict(id, indexer_configuration_id)
                do update set
                    metadata = excluded.metadata,
                    metadata_tsvector = excluded.metadata_tsvector,
                    mappings = excluded.mappings,
                    origin_url = excluded.origin_url,
                    from_revision = excluded.from_revision;

    else
        insert into origin_intrinsic_metadata (id, origin_url, metadata, indexer_configuration_id, from_revision, metadata_tsvector, mappings)
        select id, origin_url, metadata, indexer_configuration_id, from_revision,
               metadata_tsvector, mappings
    	from tmp_origin_intrinsic_metadata
            on conflict(id, indexer_configuration_id)
            do nothing;
    end if;
    return;
end
$$;

comment on function swh_origin_intrinsic_metadata_add(boolean) IS 'Add new origin intrinsic metadata';


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

