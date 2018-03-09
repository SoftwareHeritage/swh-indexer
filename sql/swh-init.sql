create extension if not exists btree_gist;
create extension if not exists pgcrypto;

create or replace language plpgsql;

create or replace function hash_sha1(text)
    returns text
    language sql strict immutable
as $$
    select encode(digest($1, 'sha1'), 'hex')
$$;

comment on function hash_sha1(text) is 'Compute sha1 hash as text';
