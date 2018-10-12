--
-- PostgreSQL database dump
--

-- Dumped from database version 10.4 (Debian 10.4-2)
-- Dumped by pg_dump version 10.4 (Debian 10.4-2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


--
-- Name: btree_gist; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;


--
-- Name: EXTENSION btree_gist; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION btree_gist IS 'support for indexing common datatypes in GiST';


--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


--
-- Name: ctags_languages; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.ctags_languages AS ENUM (
    'Ada',
    'AnsiblePlaybook',
    'Ant',
    'Asm',
    'Asp',
    'Autoconf',
    'Automake',
    'Awk',
    'Basic',
    'BETA',
    'C',
    'C#',
    'C++',
    'Clojure',
    'Cobol',
    'CoffeeScript [disabled]',
    'CSS',
    'ctags',
    'D',
    'DBusIntrospect',
    'Diff',
    'DosBatch',
    'DTS',
    'Eiffel',
    'Erlang',
    'Falcon',
    'Flex',
    'Fortran',
    'gdbinit [disabled]',
    'Glade',
    'Go',
    'HTML',
    'Iniconf',
    'Java',
    'JavaProperties',
    'JavaScript',
    'JSON',
    'Lisp',
    'Lua',
    'M4',
    'Make',
    'man [disabled]',
    'MatLab',
    'Maven2',
    'Myrddin',
    'ObjectiveC',
    'OCaml',
    'OldC
  [disabled]',
    'OldC++ [disabled]',
    'Pascal',
    'Perl',
    'Perl6',
    'PHP',
    'PlistXML',
    'pod',
    'Protobuf',
    'Python',
    'PythonLoggingConfig',
    'R',
    'RelaxNG',
    'reStructuredText',
    'REXX',
    'RpmSpec',
    'Ruby',
    'Rust',
    'Scheme',
    'Sh',
    'SLang',
    'SML',
    'SQL',
    'SVG',
    'SystemdUnit',
    'SystemVerilog',
    'Tcl',
    'Tex',
    'TTCN',
    'Vera',
    'Verilog',
    'VHDL',
    'Vim',
    'WindRes',
    'XSLT',
    'YACC',
    'Yaml',
    'YumRepo',
    'Zephir'
);


--
-- Name: TYPE ctags_languages; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TYPE public.ctags_languages IS 'Languages recognized by ctags indexer';


--
-- Name: sha1; Type: DOMAIN; Schema: public; Owner: -
--

CREATE DOMAIN public.sha1 AS bytea
	CONSTRAINT sha1_check CHECK ((length(VALUE) = 20));


--
-- Name: content_ctags_signature; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.content_ctags_signature AS (
	id public.sha1,
	name text,
	kind text,
	line bigint,
	lang public.ctags_languages,
	tool_id integer,
	tool_name text,
	tool_version text,
	tool_configuration jsonb
);


--
-- Name: languages; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.languages AS ENUM (
    'abap',
    'abnf',
    'actionscript',
    'actionscript-3',
    'ada',
    'adl',
    'agda',
    'alloy',
    'ambienttalk',
    'antlr',
    'antlr-with-actionscript-target',
    'antlr-with-c#-target',
    'antlr-with-cpp-target',
    'antlr-with-java-target',
    'antlr-with-objectivec-target',
    'antlr-with-perl-target',
    'antlr-with-python-target',
    'antlr-with-ruby-target',
    'apacheconf',
    'apl',
    'applescript',
    'arduino',
    'aspectj',
    'aspx-cs',
    'aspx-vb',
    'asymptote',
    'autohotkey',
    'autoit',
    'awk',
    'base-makefile',
    'bash',
    'bash-session',
    'batchfile',
    'bbcode',
    'bc',
    'befunge',
    'blitzbasic',
    'blitzmax',
    'bnf',
    'boo',
    'boogie',
    'brainfuck',
    'bro',
    'bugs',
    'c',
    'c#',
    'c++',
    'c-objdump',
    'ca65-assembler',
    'cadl',
    'camkes',
    'cbm-basic-v2',
    'ceylon',
    'cfengine3',
    'cfstatement',
    'chaiscript',
    'chapel',
    'cheetah',
    'cirru',
    'clay',
    'clojure',
    'clojurescript',
    'cmake',
    'cobol',
    'cobolfree',
    'coffeescript',
    'coldfusion-cfc',
    'coldfusion-html',
    'common-lisp',
    'component-pascal',
    'coq',
    'cpp-objdump',
    'cpsa',
    'crmsh',
    'croc',
    'cryptol',
    'csound-document',
    'csound-orchestra',
    'csound-score',
    'css',
    'css+django/jinja',
    'css+genshi-text',
    'css+lasso',
    'css+mako',
    'css+mozpreproc',
    'css+myghty',
    'css+php',
    'css+ruby',
    'css+smarty',
    'cuda',
    'cypher',
    'cython',
    'd',
    'd-objdump',
    'darcs-patch',
    'dart',
    'debian-control-file',
    'debian-sourcelist',
    'delphi',
    'dg',
    'diff',
    'django/jinja',
    'docker',
    'dtd',
    'duel',
    'dylan',
    'dylan-session',
    'dylanlid',
    'earl-grey',
    'easytrieve',
    'ebnf',
    'ec',
    'ecl',
    'eiffel',
    'elixir',
    'elixir-iex-session',
    'elm',
    'emacslisp',
    'embedded-ragel',
    'erb',
    'erlang',
    'erlang-erl-session',
    'evoque',
    'ezhil',
    'factor',
    'fancy',
    'fantom',
    'felix',
    'fish',
    'fortran',
    'fortranfixed',
    'foxpro',
    'fsharp',
    'gap',
    'gas',
    'genshi',
    'genshi-text',
    'gettext-catalog',
    'gherkin',
    'glsl',
    'gnuplot',
    'go',
    'golo',
    'gooddata-cl',
    'gosu',
    'gosu-template',
    'groff',
    'groovy',
    'haml',
    'handlebars',
    'haskell',
    'haxe',
    'hexdump',
    'html',
    'html+cheetah',
    'html+django/jinja',
    'html+evoque',
    'html+genshi',
    'html+handlebars',
    'html+lasso',
    'html+mako',
    'html+myghty',
    'html+php',
    'html+smarty',
    'html+twig',
    'html+velocity',
    'http',
    'hxml',
    'hy',
    'hybris',
    'idl',
    'idris',
    'igor',
    'inform-6',
    'inform-6-template',
    'inform-7',
    'ini',
    'io',
    'ioke',
    'irc-logs',
    'isabelle',
    'j',
    'jade',
    'jags',
    'jasmin',
    'java',
    'java-server-page',
    'javascript',
    'javascript+cheetah',
    'javascript+django/jinja',
    'javascript+genshi-text',
    'javascript+lasso',
    'javascript+mako',
    'javascript+mozpreproc',
    'javascript+myghty',
    'javascript+php',
    'javascript+ruby',
    'javascript+smarty',
    'jcl',
    'json',
    'json-ld',
    'julia',
    'julia-console',
    'kal',
    'kconfig',
    'koka',
    'kotlin',
    'lasso',
    'lean',
    'lesscss',
    'lighttpd-configuration-file',
    'limbo',
    'liquid',
    'literate-agda',
    'literate-cryptol',
    'literate-haskell',
    'literate-idris',
    'livescript',
    'llvm',
    'logos',
    'logtalk',
    'lsl',
    'lua',
    'makefile',
    'mako',
    'maql',
    'mask',
    'mason',
    'mathematica',
    'matlab',
    'matlab-session',
    'minid',
    'modelica',
    'modula-2',
    'moinmoin/trac-wiki-markup',
    'monkey',
    'moocode',
    'moonscript',
    'mozhashpreproc',
    'mozpercentpreproc',
    'mql',
    'mscgen',
    'msdos-session',
    'mupad',
    'mxml',
    'myghty',
    'mysql',
    'nasm',
    'nemerle',
    'nesc',
    'newlisp',
    'newspeak',
    'nginx-configuration-file',
    'nimrod',
    'nit',
    'nix',
    'nsis',
    'numpy',
    'objdump',
    'objdump-nasm',
    'objective-c',
    'objective-c++',
    'objective-j',
    'ocaml',
    'octave',
    'odin',
    'ooc',
    'opa',
    'openedge-abl',
    'pacmanconf',
    'pan',
    'parasail',
    'pawn',
    'perl',
    'perl6',
    'php',
    'pig',
    'pike',
    'pkgconfig',
    'pl/pgsql',
    'postgresql-console-(psql)',
    'postgresql-sql-dialect',
    'postscript',
    'povray',
    'powershell',
    'powershell-session',
    'praat',
    'prolog',
    'properties',
    'protocol-buffer',
    'puppet',
    'pypy-log',
    'python',
    'python-3',
    'python-3.0-traceback',
    'python-console-session',
    'python-traceback',
    'qbasic',
    'qml',
    'qvto',
    'racket',
    'ragel',
    'ragel-in-c-host',
    'ragel-in-cpp-host',
    'ragel-in-d-host',
    'ragel-in-java-host',
    'ragel-in-objective-c-host',
    'ragel-in-ruby-host',
    'raw-token-data',
    'rconsole',
    'rd',
    'rebol',
    'red',
    'redcode',
    'reg',
    'resourcebundle',
    'restructuredtext',
    'rexx',
    'rhtml',
    'roboconf-graph',
    'roboconf-instances',
    'robotframework',
    'rpmspec',
    'rql',
    'rsl',
    'ruby',
    'ruby-irb-session',
    'rust',
    's',
    'sass',
    'scala',
    'scalate-server-page',
    'scaml',
    'scheme',
    'scilab',
    'scss',
    'shen',
    'slim',
    'smali',
    'smalltalk',
    'smarty',
    'snobol',
    'sourcepawn',
    'sparql',
    'sql',
    'sqlite3con',
    'squidconf',
    'stan',
    'standard-ml',
    'supercollider',
    'swift',
    'swig',
    'systemverilog',
    'tads-3',
    'tap',
    'tcl',
    'tcsh',
    'tcsh-session',
    'tea',
    'termcap',
    'terminfo',
    'terraform',
    'tex',
    'text-only',
    'thrift',
    'todotxt',
    'trafficscript',
    'treetop',
    'turtle',
    'twig',
    'typescript',
    'urbiscript',
    'vala',
    'vb.net',
    'vctreestatus',
    'velocity',
    'verilog',
    'vgl',
    'vhdl',
    'viml',
    'x10',
    'xml',
    'xml+cheetah',
    'xml+django/jinja',
    'xml+evoque',
    'xml+lasso',
    'xml+mako',
    'xml+myghty',
    'xml+php',
    'xml+ruby',
    'xml+smarty',
    'xml+velocity',
    'xquery',
    'xslt',
    'xtend',
    'xul+mozpreproc',
    'yaml',
    'yaml+jinja',
    'zephir',
    'unknown'
);


--
-- Name: TYPE languages; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TYPE public.languages IS 'Languages recognized by language indexer';


--
-- Name: sha1_git; Type: DOMAIN; Schema: public; Owner: -
--

CREATE DOMAIN public.sha1_git AS bytea
	CONSTRAINT sha1_git_check CHECK ((length(VALUE) = 20));


--
-- Name: hash_sha1(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.hash_sha1(text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $_$
    select encode(public.digest($1, 'sha1'), 'hex')
$_$;


--
-- Name: FUNCTION hash_sha1(text); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.hash_sha1(text) IS 'Compute sha1 hash as text';


--
-- Name: swh_content_ctags_add(boolean); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_content_ctags_add(conflict_update boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$
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


--
-- Name: FUNCTION swh_content_ctags_add(conflict_update boolean); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_content_ctags_add(conflict_update boolean) IS 'Add new ctags symbols per content';


--
-- Name: swh_content_ctags_search(text, integer, public.sha1); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_content_ctags_search(expression text, l integer DEFAULT 10, last_sha1 public.sha1 DEFAULT '\x0000000000000000000000000000000000000000'::bytea) RETURNS SETOF public.content_ctags_signature
    LANGUAGE sql
    AS $$
    select c.id, name, kind, line, lang,
           i.id as tool_id, tool_name, tool_version, tool_configuration
    from content_ctags c
    inner join indexer_configuration i on i.id = c.indexer_configuration_id
    where hash_sha1(name) = hash_sha1(expression)
    and c.id > last_sha1
    order by id
    limit l;
$$;


--
-- Name: FUNCTION swh_content_ctags_search(expression text, l integer, last_sha1 public.sha1); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_content_ctags_search(expression text, l integer, last_sha1 public.sha1) IS 'Equality search through ctags'' symbols';


--
-- Name: swh_content_fossology_license_add(boolean); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_content_fossology_license_add(conflict_update boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$
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


--
-- Name: FUNCTION swh_content_fossology_license_add(conflict_update boolean); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_content_fossology_license_add(conflict_update boolean) IS 'Add new content licenses';


--
-- Name: swh_content_language_add(boolean); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_content_language_add(conflict_update boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$
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


--
-- Name: FUNCTION swh_content_language_add(conflict_update boolean); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_content_language_add(conflict_update boolean) IS 'Add new content languages';


--
-- Name: swh_content_metadata_add(boolean); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_content_metadata_add(conflict_update boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$
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


--
-- Name: FUNCTION swh_content_metadata_add(conflict_update boolean); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_content_metadata_add(conflict_update boolean) IS 'Add new content metadata';


--
-- Name: swh_content_mimetype_add(boolean); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_content_mimetype_add(conflict_update boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$
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


--
-- Name: FUNCTION swh_content_mimetype_add(conflict_update boolean); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_content_mimetype_add(conflict_update boolean) IS 'Add new content mimetypes';


SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: indexer_configuration; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.indexer_configuration (
    id integer NOT NULL,
    tool_name text NOT NULL,
    tool_version text NOT NULL,
    tool_configuration jsonb
);


--
-- Name: TABLE indexer_configuration; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.indexer_configuration IS 'Indexer''s configuration version';


--
-- Name: COLUMN indexer_configuration.id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.indexer_configuration.id IS 'Tool identifier';


--
-- Name: COLUMN indexer_configuration.tool_version; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.indexer_configuration.tool_version IS 'Tool version';


--
-- Name: COLUMN indexer_configuration.tool_configuration; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.indexer_configuration.tool_configuration IS 'Tool configuration: command line, flags, etc...';


--
-- Name: swh_indexer_configuration_add(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_indexer_configuration_add() RETURNS SETOF public.indexer_configuration
    LANGUAGE plpgsql
    AS $$
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


--
-- Name: swh_mktemp(regclass); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_mktemp(tblname regclass) RETURNS void
    LANGUAGE plpgsql
    AS $_$
begin
    execute format('
	create temporary table tmp_%1$I
	    (like %1$I including defaults)
	    on commit drop;
      alter table tmp_%1$I drop column if exists object_id;
	', tblname);
    return;
end
$_$;


--
-- Name: swh_mktemp_content_ctags(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_mktemp_content_ctags() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_content_ctags (
    like content_ctags including defaults
  ) on commit drop;
$$;


--
-- Name: FUNCTION swh_mktemp_content_ctags(); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_mktemp_content_ctags() IS 'Helper table to add content ctags';


--
-- Name: swh_mktemp_content_fossology_license(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_mktemp_content_fossology_license() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_content_fossology_license (
    id                       sha1,
    license                  text,
    indexer_configuration_id integer
  ) on commit drop;
$$;


--
-- Name: FUNCTION swh_mktemp_content_fossology_license(); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_mktemp_content_fossology_license() IS 'Helper table to add content license';


--
-- Name: swh_mktemp_content_language(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_mktemp_content_language() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_content_language (
    like content_language including defaults
  ) on commit drop;
$$;


--
-- Name: FUNCTION swh_mktemp_content_language(); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_mktemp_content_language() IS 'Helper table to add content language';


--
-- Name: swh_mktemp_content_metadata(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_mktemp_content_metadata() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_content_metadata (
    like content_metadata including defaults
  ) on commit drop;
$$;


--
-- Name: FUNCTION swh_mktemp_content_metadata(); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_mktemp_content_metadata() IS 'Helper table to add content metadata';


--
-- Name: swh_mktemp_content_mimetype(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_mktemp_content_mimetype() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_content_mimetype (
    like content_mimetype including defaults
  ) on commit drop;
$$;


--
-- Name: FUNCTION swh_mktemp_content_mimetype(); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_mktemp_content_mimetype() IS 'Helper table to add mimetype information';


--
-- Name: swh_mktemp_indexer_configuration(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_mktemp_indexer_configuration() RETURNS void
    LANGUAGE sql
    AS $$
    create temporary table tmp_indexer_configuration (
      like indexer_configuration including defaults
    ) on commit drop;
    alter table tmp_indexer_configuration drop column id;
$$;


--
-- Name: swh_mktemp_revision_metadata(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_mktemp_revision_metadata() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_revision_metadata (
    like revision_metadata including defaults
  ) on commit drop;
$$;


--
-- Name: FUNCTION swh_mktemp_revision_metadata(); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_mktemp_revision_metadata() IS 'Helper table to add revision metadata';


--
-- Name: swh_revision_metadata_add(boolean); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_revision_metadata_add(conflict_update boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$
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


--
-- Name: FUNCTION swh_revision_metadata_add(conflict_update boolean); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_revision_metadata_add(conflict_update boolean) IS 'Add new revision metadata';


--
-- Name: content_ctags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.content_ctags (
    id public.sha1 NOT NULL,
    name text NOT NULL,
    kind text NOT NULL,
    line bigint NOT NULL,
    lang public.ctags_languages NOT NULL,
    indexer_configuration_id bigint NOT NULL
);


--
-- Name: TABLE content_ctags; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.content_ctags IS 'Ctags information on a raw content';


--
-- Name: COLUMN content_ctags.id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_ctags.id IS 'Content identifier';


--
-- Name: COLUMN content_ctags.name; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_ctags.name IS 'Symbol name';


--
-- Name: COLUMN content_ctags.kind; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_ctags.kind IS 'Symbol kind (function, class, variable, const...)';


--
-- Name: COLUMN content_ctags.line; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_ctags.line IS 'Symbol line';


--
-- Name: COLUMN content_ctags.lang; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_ctags.lang IS 'Language information for that content';


--
-- Name: COLUMN content_ctags.indexer_configuration_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_ctags.indexer_configuration_id IS 'Tool used to compute the information';


--
-- Name: content_fossology_license; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.content_fossology_license (
    id public.sha1 NOT NULL,
    license_id smallint NOT NULL,
    indexer_configuration_id bigint NOT NULL
);


--
-- Name: TABLE content_fossology_license; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.content_fossology_license IS 'license associated to a raw content';


--
-- Name: COLUMN content_fossology_license.id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_fossology_license.id IS 'Raw content identifier';


--
-- Name: COLUMN content_fossology_license.license_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_fossology_license.license_id IS 'One of the content''s license identifier';


--
-- Name: COLUMN content_fossology_license.indexer_configuration_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_fossology_license.indexer_configuration_id IS 'Tool used to compute the information';


--
-- Name: content_fossology_license_license_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.content_fossology_license_license_id_seq
    AS smallint
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: content_fossology_license_license_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.content_fossology_license_license_id_seq OWNED BY public.content_fossology_license.license_id;


--
-- Name: content_language; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.content_language (
    id public.sha1 NOT NULL,
    lang public.languages NOT NULL,
    indexer_configuration_id bigint NOT NULL
);


--
-- Name: TABLE content_language; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.content_language IS 'Language information on a raw content';


--
-- Name: COLUMN content_language.lang; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_language.lang IS 'Language information';


--
-- Name: COLUMN content_language.indexer_configuration_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_language.indexer_configuration_id IS 'Tool used to compute the information';


--
-- Name: content_metadata; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.content_metadata (
    id public.sha1 NOT NULL,
    translated_metadata jsonb NOT NULL,
    indexer_configuration_id bigint NOT NULL
);


--
-- Name: TABLE content_metadata; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.content_metadata IS 'metadata semantically translated from a content file';


--
-- Name: COLUMN content_metadata.id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_metadata.id IS 'sha1 of content file';


--
-- Name: COLUMN content_metadata.translated_metadata; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_metadata.translated_metadata IS 'result of translation with defined format';


--
-- Name: COLUMN content_metadata.indexer_configuration_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_metadata.indexer_configuration_id IS 'tool used for translation';


--
-- Name: content_mimetype; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.content_mimetype (
    id public.sha1 NOT NULL,
    mimetype bytea NOT NULL,
    encoding bytea NOT NULL,
    indexer_configuration_id bigint NOT NULL
);


--
-- Name: TABLE content_mimetype; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.content_mimetype IS 'Metadata associated to a raw content';


--
-- Name: COLUMN content_mimetype.mimetype; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_mimetype.mimetype IS 'Raw content Mimetype';


--
-- Name: COLUMN content_mimetype.encoding; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_mimetype.encoding IS 'Raw content encoding';


--
-- Name: COLUMN content_mimetype.indexer_configuration_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_mimetype.indexer_configuration_id IS 'Tool used to compute the information';


--
-- Name: dbversion; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dbversion (
    version integer NOT NULL,
    release timestamp with time zone,
    description text
);


--
-- Name: fossology_license; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.fossology_license (
    id smallint NOT NULL,
    name text NOT NULL
);


--
-- Name: TABLE fossology_license; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.fossology_license IS 'Possible license recognized by license indexer';


--
-- Name: COLUMN fossology_license.id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.fossology_license.id IS 'License identifier';


--
-- Name: COLUMN fossology_license.name; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.fossology_license.name IS 'License name';


--
-- Name: fossology_license_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.fossology_license_id_seq
    AS smallint
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: fossology_license_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.fossology_license_id_seq OWNED BY public.fossology_license.id;


--
-- Name: indexer_configuration_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.indexer_configuration_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: indexer_configuration_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.indexer_configuration_id_seq OWNED BY public.indexer_configuration.id;


--
-- Name: origin_metadata_translation; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.origin_metadata_translation (
    id bigint NOT NULL,
    result jsonb,
    tool_id bigint
);


--
-- Name: TABLE origin_metadata_translation; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.origin_metadata_translation IS 'keeps translated for an origin_metadata entry';


--
-- Name: COLUMN origin_metadata_translation.id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.origin_metadata_translation.id IS 'the entry id in origin_metadata';


--
-- Name: COLUMN origin_metadata_translation.result; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.origin_metadata_translation.result IS 'translated_metadata result after translation with tool';


--
-- Name: COLUMN origin_metadata_translation.tool_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.origin_metadata_translation.tool_id IS 'tool used for translation';


--
-- Name: origin_metadata_translation_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.origin_metadata_translation_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: origin_metadata_translation_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.origin_metadata_translation_id_seq OWNED BY public.origin_metadata_translation.id;


--
-- Name: revision_metadata; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.revision_metadata (
    id public.sha1_git NOT NULL,
    translated_metadata jsonb NOT NULL,
    indexer_configuration_id bigint NOT NULL
);


--
-- Name: TABLE revision_metadata; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.revision_metadata IS 'metadata semantically detected and translated in a revision';


--
-- Name: COLUMN revision_metadata.id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.revision_metadata.id IS 'sha1_git of revision';


--
-- Name: COLUMN revision_metadata.translated_metadata; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.revision_metadata.translated_metadata IS 'result of detection and translation with defined format';


--
-- Name: COLUMN revision_metadata.indexer_configuration_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.revision_metadata.indexer_configuration_id IS 'tool used for detection';


--
-- Name: content_fossology_license license_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_fossology_license ALTER COLUMN license_id SET DEFAULT nextval('public.content_fossology_license_license_id_seq'::regclass);


--
-- Name: fossology_license id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fossology_license ALTER COLUMN id SET DEFAULT nextval('public.fossology_license_id_seq'::regclass);


--
-- Name: indexer_configuration id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.indexer_configuration ALTER COLUMN id SET DEFAULT nextval('public.indexer_configuration_id_seq'::regclass);


--
-- Name: origin_metadata_translation id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.origin_metadata_translation ALTER COLUMN id SET DEFAULT nextval('public.origin_metadata_translation_id_seq'::regclass);


--
-- Data for Name: content_ctags; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.content_ctags (id, name, kind, line, lang, indexer_configuration_id) FROM stdin;
\.


--
-- Data for Name: content_fossology_license; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.content_fossology_license (id, license_id, indexer_configuration_id) FROM stdin;
\.


--
-- Data for Name: content_language; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.content_language (id, lang, indexer_configuration_id) FROM stdin;
\.


--
-- Data for Name: content_metadata; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.content_metadata (id, translated_metadata, indexer_configuration_id) FROM stdin;
\.


--
-- Data for Name: content_mimetype; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.content_mimetype (id, mimetype, encoding, indexer_configuration_id) FROM stdin;
\.


--
-- Data for Name: dbversion; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.dbversion (version, release, description) FROM stdin;
115	2018-06-22 18:02:38.144382+02	Work In Progress
\.


--
-- Data for Name: fossology_license; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.fossology_license (id, name) FROM stdin;
\.


--
-- Data for Name: indexer_configuration; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.indexer_configuration (id, tool_name, tool_version, tool_configuration) FROM stdin;
1	nomos	3.1.0rc2-31-ga2cbb8c	{"command_line": "nomossa <filepath>"}
2	file	5.22	{"command_line": "file --mime <filepath>"}
3	universal-ctags	~git7859817b	{"command_line": "ctags --fields=+lnz --sort=no --links=no --output-format=json <filepath>"}
4	pygments	2.0.1+dfsg-1.1+deb8u1	{"type": "library", "debian-package": "python3-pygments"}
5	pygments	2.0.1+dfsg-1.1+deb8u1	{"type": "library", "debian-package": "python3-pygments", "max_content_size": 10240}
6	swh-metadata-translator	0.0.1	{"type": "local", "context": "npm"}
7	swh-metadata-detector	0.0.1	{"type": "local", "context": ["npm", "codemeta"]}
8	swh-deposit	0.0.1	{"sword_version": "2"}
9	file	1:5.30-1+deb9u1	{"type": "library", "debian-package": "python3-magic"}
\.


--
-- Data for Name: origin_metadata_translation; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.origin_metadata_translation (id, result, tool_id) FROM stdin;
\.


--
-- Data for Name: revision_metadata; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.revision_metadata (id, translated_metadata, indexer_configuration_id) FROM stdin;
\.


--
-- Name: content_fossology_license_license_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.content_fossology_license_license_id_seq', 1, false);


--
-- Name: fossology_license_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.fossology_license_id_seq', 1, false);


--
-- Name: indexer_configuration_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.indexer_configuration_id_seq', 9, true);


--
-- Name: origin_metadata_translation_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.origin_metadata_translation_id_seq', 1, false);


--
-- Name: content_fossology_license content_fossology_license_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_fossology_license
    ADD CONSTRAINT content_fossology_license_pkey PRIMARY KEY (id, license_id, indexer_configuration_id);


--
-- Name: content_language content_language_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_language
    ADD CONSTRAINT content_language_pkey PRIMARY KEY (id, indexer_configuration_id);


--
-- Name: content_metadata content_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_metadata
    ADD CONSTRAINT content_metadata_pkey PRIMARY KEY (id, indexer_configuration_id);


--
-- Name: content_mimetype content_mimetype_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_mimetype
    ADD CONSTRAINT content_mimetype_pkey PRIMARY KEY (id, indexer_configuration_id);


--
-- Name: dbversion dbversion_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dbversion
    ADD CONSTRAINT dbversion_pkey PRIMARY KEY (version);


--
-- Name: fossology_license fossology_license_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fossology_license
    ADD CONSTRAINT fossology_license_pkey PRIMARY KEY (id);


--
-- Name: indexer_configuration indexer_configuration_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.indexer_configuration
    ADD CONSTRAINT indexer_configuration_pkey PRIMARY KEY (id);


--
-- Name: revision_metadata revision_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.revision_metadata
    ADD CONSTRAINT revision_metadata_pkey PRIMARY KEY (id, indexer_configuration_id);


--
-- Name: content_ctags_hash_sha1_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX content_ctags_hash_sha1_idx ON public.content_ctags USING btree (public.hash_sha1(name));


--
-- Name: content_ctags_id_hash_sha1_kind_line_lang_indexer_configura_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX content_ctags_id_hash_sha1_kind_line_lang_indexer_configura_idx ON public.content_ctags USING btree (id, public.hash_sha1(name), kind, line, lang, indexer_configuration_id);


--
-- Name: content_ctags_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX content_ctags_id_idx ON public.content_ctags USING btree (id);


--
-- Name: fossology_license_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX fossology_license_name_idx ON public.fossology_license USING btree (name);


--
-- Name: indexer_configuration_tool_name_tool_version_tool_configura_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX indexer_configuration_tool_name_tool_version_tool_configura_idx ON public.indexer_configuration USING btree (tool_name, tool_version, tool_configuration);


--
-- Name: content_ctags content_ctags_indexer_configuration_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_ctags
    ADD CONSTRAINT content_ctags_indexer_configuration_id_fkey FOREIGN KEY (indexer_configuration_id) REFERENCES public.indexer_configuration(id);


--
-- Name: content_fossology_license content_fossology_license_indexer_configuration_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_fossology_license
    ADD CONSTRAINT content_fossology_license_indexer_configuration_id_fkey FOREIGN KEY (indexer_configuration_id) REFERENCES public.indexer_configuration(id);


--
-- Name: content_fossology_license content_fossology_license_license_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_fossology_license
    ADD CONSTRAINT content_fossology_license_license_id_fkey FOREIGN KEY (license_id) REFERENCES public.fossology_license(id);


--
-- Name: content_language content_language_indexer_configuration_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_language
    ADD CONSTRAINT content_language_indexer_configuration_id_fkey FOREIGN KEY (indexer_configuration_id) REFERENCES public.indexer_configuration(id);


--
-- Name: content_metadata content_metadata_indexer_configuration_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_metadata
    ADD CONSTRAINT content_metadata_indexer_configuration_id_fkey FOREIGN KEY (indexer_configuration_id) REFERENCES public.indexer_configuration(id);


--
-- Name: content_mimetype content_mimetype_indexer_configuration_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_mimetype
    ADD CONSTRAINT content_mimetype_indexer_configuration_id_fkey FOREIGN KEY (indexer_configuration_id) REFERENCES public.indexer_configuration(id);


--
-- Name: revision_metadata revision_metadata_indexer_configuration_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.revision_metadata
    ADD CONSTRAINT revision_metadata_indexer_configuration_id_fkey FOREIGN KEY (indexer_configuration_id) REFERENCES public.indexer_configuration(id);


--
-- PostgreSQL database dump complete
--

