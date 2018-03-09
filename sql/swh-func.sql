-- Postgresql index helper function
create or replace function hash_sha1(text)
    returns text
    language sql strict immutable
as $$
    select encode(digest($1, 'sha1'), 'hex')
$$;

comment on function hash_sha1(text) is 'Compute sha1 hash as text';

-- create a temporary table with a single "bytea" column for fast object lookup.
create or replace function swh_mktemp_bytea()
    returns void
    language sql
as $$
    create temporary table tmp_bytea (
      id bytea
    ) on commit drop;
$$;

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

-- create a temporary table for content_ctags tmp_content_mimetype_missing,
create or replace function swh_mktemp_content_mimetype_missing()
    returns void
    language sql
as $$
  create temporary table tmp_content_mimetype_missing (
    id sha1,
    indexer_configuration_id bigint
  ) on commit drop;
$$;

comment on function swh_mktemp_content_mimetype_missing() IS 'Helper table to filter existing mimetype information';

-- check which entries of tmp_bytea are missing from content_mimetype
--
-- operates in bulk: 0. swh_mktemp_bytea(), 1. COPY to tmp_bytea,
-- 2. call this function
create or replace function swh_content_mimetype_missing()
    returns setof sha1
    language plpgsql
as $$
begin
    return query
	(select id::sha1 from tmp_content_mimetype_missing as tmp
	 where not exists
	     (select 1 from content_mimetype as c
              where c.id = tmp.id and c.indexer_configuration_id = tmp.indexer_configuration_id));
    return;
end
$$;

comment on function swh_content_mimetype_missing() is 'Filter existing mimetype information';

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

create type content_mimetype_signature as(
    id sha1,
    mimetype bytea,
    encoding bytea,
    tool_id integer,
    tool_name text,
    tool_version text,
    tool_configuration jsonb
);

-- Retrieve list of content mimetype from the temporary table.
--
-- operates in bulk: 0. mktemp(tmp_bytea), 1. COPY to tmp_bytea,
-- 2. call this function
create or replace function swh_content_mimetype_get()
    returns setof content_mimetype_signature
    language plpgsql
as $$
begin
    return query
        select c.id, mimetype, encoding,
               i.id as tool_id, tool_name, tool_version, tool_configuration
        from tmp_bytea t
        inner join content_mimetype c on c.id=t.id
        inner join indexer_configuration i on c.indexer_configuration_id=i.id;
    return;
end
$$;

comment on function swh_content_mimetype_get() IS 'List content''s mimetypes';

-- create a temporary table for content_language tmp_content_language,
create or replace function swh_mktemp_content_language_missing()
    returns void
    language sql
as $$
  create temporary table tmp_content_language_missing (
    id sha1,
    indexer_configuration_id integer
  ) on commit drop;
$$;

comment on function swh_mktemp_content_language_missing() is 'Helper table to filter missing language';

-- check which entries of tmp_bytea are missing from content_language
--
-- operates in bulk: 0. swh_mktemp_bytea(), 1. COPY to tmp_bytea,
-- 2. call this function
create or replace function swh_content_language_missing()
    returns setof sha1
    language plpgsql
as $$
begin
    return query
	select id::sha1 from tmp_content_language_missing as tmp
	where not exists
	    (select 1 from content_language as c
             where c.id = tmp.id and c.indexer_configuration_id = tmp.indexer_configuration_id);
    return;
end
$$;

comment on function swh_content_language_missing() IS 'Filter missing content languages';

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

create type content_language_signature as (
    id sha1,
    lang languages,
    tool_id integer,
    tool_name text,
    tool_version text,
    tool_configuration jsonb
);

-- Retrieve list of content language from the temporary table.
--
-- operates in bulk: 0. mktemp(tmp_bytea), 1. COPY to tmp_bytea, 2. call this function
create or replace function swh_content_language_get()
    returns setof content_language_signature
    language plpgsql
as $$
begin
    return query
        select c.id, lang, i.id as tool_id, tool_name, tool_version, tool_configuration
        from tmp_bytea t
        inner join content_language c on c.id = t.id
        inner join indexer_configuration i on i.id=c.indexer_configuration_id;
    return;
end
$$;

comment on function swh_content_language_get() is 'List content''s language';


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

-- create a temporary table for content_ctags missing routine
create or replace function swh_mktemp_content_ctags_missing()
    returns void
    language sql
as $$
  create temporary table tmp_content_ctags_missing (
    id           sha1,
    indexer_configuration_id    integer
  ) on commit drop;
$$;

comment on function swh_mktemp_content_ctags_missing() is 'Helper table to filter missing content ctags';

-- check which entries of tmp_bytea are missing from content_ctags
--
-- operates in bulk: 0. swh_mktemp_bytea(), 1. COPY to tmp_bytea,
-- 2. call this function
create or replace function swh_content_ctags_missing()
    returns setof sha1
    language plpgsql
as $$
begin
    return query
	(select id::sha1 from tmp_content_ctags_missing as tmp
	 where not exists
	     (select 1 from content_ctags as c
              where c.id = tmp.id and c.indexer_configuration_id=tmp.indexer_configuration_id
              limit 1));
    return;
end
$$;

comment on function swh_content_ctags_missing() IS 'Filter missing content ctags';

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

-- Retrieve list of content ctags from the temporary table.
--
-- operates in bulk: 0. mktemp(tmp_bytea), 1. COPY to tmp_bytea, 2. call this function
create or replace function swh_content_ctags_get()
    returns setof content_ctags_signature
    language plpgsql
as $$
begin
    return query
        select c.id, c.name, c.kind, c.line, c.lang,
               i.id as tool_id, i.tool_name, i.tool_version, i.tool_configuration
        from tmp_bytea t
        inner join content_ctags c using(id)
        inner join indexer_configuration i on i.id = c.indexer_configuration_id
        order by line;
    return;
end
$$;

comment on function swh_content_ctags_get() IS 'List content ctags';

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

create type content_fossology_license_signature as (
  id                 sha1,
  tool_id            integer,
  tool_name          text,
  tool_version       text,
  tool_configuration jsonb,
  licenses           text[]
);

-- Retrieve list of content license from the temporary table.
--
-- operates in bulk: 0. mktemp(tmp_bytea), 1. COPY to tmp_bytea,
-- 2. call this function
create or replace function swh_content_fossology_license_get()
    returns setof content_fossology_license_signature
    language plpgsql
as $$
begin
    return query
      select cl.id,
             ic.id as tool_id,
             ic.tool_name,
             ic.tool_version,
             ic.tool_configuration,
             array(select name
                   from fossology_license
                   where id = ANY(array_agg(cl.license_id))) as licenses
      from tmp_bytea tcl
      inner join content_fossology_license cl using(id)
      inner join indexer_configuration ic on ic.id=cl.indexer_configuration_id
      group by cl.id, ic.id, ic.tool_name, ic.tool_version, ic.tool_configuration;
    return;
end
$$;

comment on function swh_content_fossology_license_get() IS 'List content licenses';

-- content_metadata functions
--
-- create a temporary table for content_metadata tmp_content_metadata,
create or replace function swh_mktemp_content_metadata_missing()
    returns void
    language sql
as $$
  create temporary table tmp_content_metadata_missing (
    id sha1,
    indexer_configuration_id integer
  ) on commit drop;
$$;

comment on function swh_mktemp_content_metadata_missing() is 'Helper table to filter missing metadata in content_metadata';

-- check which entries of tmp_bytea are missing from content_metadata
--
-- operates in bulk: 0. swh_mktemp_bytea(), 1. COPY to tmp_bytea,
-- 2. call this function
create or replace function swh_content_metadata_missing()
    returns setof sha1
    language plpgsql
as $$
begin
    return query
	select id::sha1 from tmp_content_metadata_missing as tmp
	where not exists
	    (select 1 from content_metadata as c
             where c.id = tmp.id and c.indexer_configuration_id = tmp.indexer_configuration_id);
    return;
end
$$;

comment on function swh_content_metadata_missing() IS 'Filter missing content metadata';

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
      insert into content_metadata (id, translated_metadata, indexer_configuration_id)
      select id, translated_metadata, indexer_configuration_id
    	from tmp_content_metadata tcm
            on conflict(id, indexer_configuration_id)
                do update set translated_metadata = excluded.translated_metadata;

    else
        insert into content_metadata (id, translated_metadata, indexer_configuration_id)
        select id, translated_metadata, indexer_configuration_id
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

--
create type content_metadata_signature as (
    id sha1,
    translated_metadata jsonb,
    tool_id integer,
    tool_name text,
    tool_version text,
    tool_configuration jsonb
);

-- Retrieve list of content metadata from the temporary table.
--
-- operates in bulk: 0. mktemp(tmp_bytea), 1. COPY to tmp_bytea, 2. call this function
create or replace function swh_content_metadata_get()
    returns setof content_metadata_signature
    language plpgsql
as $$
begin
    return query
        select c.id, translated_metadata, i.id as tool_id, tool_name, tool_version, tool_configuration
        from tmp_bytea t
        inner join content_metadata c on c.id = t.id
        inner join indexer_configuration i on i.id=c.indexer_configuration_id;
    return;
end
$$;

comment on function swh_content_metadata_get() is 'List content''s metadata';
-- end content_metadata functions

-- revision_metadata functions
--
-- create a temporary table for revision_metadata tmp_revision_metadata,
create or replace function swh_mktemp_revision_metadata_missing()
    returns void
    language sql
as $$
  create temporary table tmp_revision_metadata_missing (
    id sha1_git,
    indexer_configuration_id integer
  ) on commit drop;
$$;

comment on function swh_mktemp_revision_metadata_missing() is 'Helper table to filter missing metadata in revision_metadata';

-- check which entries of tmp_bytea are missing from revision_metadata
--
-- operates in bulk: 0. swh_mktemp_bytea(), 1. COPY to tmp_bytea,
-- 2. call this function
create or replace function swh_revision_metadata_missing()
    returns setof sha1
    language plpgsql
as $$
begin
    return query
	select id::sha1 from tmp_revision_metadata_missing as tmp
	where not exists
	    (select 1 from revision_metadata as c
             where c.id = tmp.id and c.indexer_configuration_id = tmp.indexer_configuration_id);
    return;
end
$$;

comment on function swh_revision_metadata_missing() IS 'Filter missing content metadata';

-- add tmp_revision_metadata entries to revision_metadata, overwriting
-- duplicates if conflict_update is true, skipping duplicates otherwise.
--
-- If filtering duplicates is in order, the call to
-- swh_revision_metadata_missing must take place before calling this
-- function.
--
-- operates in bulk: 0. swh_mktemp(content_language), 1. COPY to
-- tmp_revision_metadata, 2. call this function
create or replace function swh_revision_metadata_add(conflict_update boolean)
    returns void
    language plpgsql
as $$
begin
    if conflict_update then
      insert into revision_metadata (id, translated_metadata, indexer_configuration_id)
      select id, translated_metadata, indexer_configuration_id
    	from tmp_revision_metadata tcm
            on conflict(id, indexer_configuration_id)
                do update set translated_metadata = excluded.translated_metadata;

    else
        insert into revision_metadata (id, translated_metadata, indexer_configuration_id)
        select id, translated_metadata, indexer_configuration_id
    	from tmp_revision_metadata tcm
            on conflict(id, indexer_configuration_id)
            do nothing;
    end if;
    return;
end
$$;

comment on function swh_revision_metadata_add(boolean) IS 'Add new revision metadata';

-- create a temporary table for retrieving revision_metadata
create or replace function swh_mktemp_revision_metadata()
    returns void
    language sql
as $$
  create temporary table tmp_revision_metadata (
    like revision_metadata including defaults
  ) on commit drop;
$$;

comment on function swh_mktemp_revision_metadata() is 'Helper table to add revision metadata';

--
create type revision_metadata_signature as (
    id sha1_git,
    translated_metadata jsonb,
    tool_id integer,
    tool_name text,
    tool_version text,
    tool_configuration jsonb
);

-- Retrieve list of revision metadata from the temporary table.
--
-- operates in bulk: 0. mktemp(tmp_bytea), 1. COPY to tmp_bytea, 2. call this function
create or replace function swh_revision_metadata_get()
    returns setof revision_metadata_signature
    language plpgsql
as $$
begin
    return query
        select c.id, translated_metadata, i.id as tool_id, tool_name, tool_version, tool_configuration
        from tmp_bytea t
        inner join revision_metadata c on c.id = t.id
        inner join indexer_configuration i on i.id=c.indexer_configuration_id;
    return;
end
$$;

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
