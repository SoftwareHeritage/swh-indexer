create extension if not exists btree_gist;
create extension if not exists pgcrypto;

create or replace language plpgsql;

create or replace function hash_sha1(text)
returns text
as $$
select encode(digest($1, 'sha1'), 'hex')
$$ language sql strict immutable;

comment on function hash_sha1(text) is 'Compute sha1 hash as text';
