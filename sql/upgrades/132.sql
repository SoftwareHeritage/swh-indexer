-- SWH Indexer DB schema upgrade
-- from_version: 131
-- to_version: 132
-- description: _add function returns the inserted rows

insert into dbversion(version, release, description)
      values(132, now(), 'Work In Progress');

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
    else
        insert into content_fossology_license (id, license_id, indexer_configuration_id)
        select tcl.id,
              (select id from fossology_license where name = tcl.license) as license,
              indexer_configuration_id
        from tmp_content_fossology_license tcl
        on conflict(id, license_id, indexer_configuration_id)
        do nothing;
    end if;

    get diagnostics res = ROW_COUNT;
    return res;
end
$$;

comment on function swh_content_fossology_license_add(boolean) IS 'Add new content licenses';
