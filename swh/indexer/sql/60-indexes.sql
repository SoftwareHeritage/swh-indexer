-- fossology_license
create unique index fossology_license_pkey on fossology_license(id);
alter table fossology_license add primary key using index fossology_license_pkey;

create unique index on fossology_license(name);

-- indexer_configuration
create unique index concurrently indexer_configuration_pkey on indexer_configuration(id);
alter table indexer_configuration add primary key using index indexer_configuration_pkey;

create unique index on indexer_configuration(tool_name, tool_version, tool_configuration);

-- content_metadata
create unique index content_metadata_pkey on content_metadata(id, indexer_configuration_id);
alter table content_metadata add primary key using index content_metadata_pkey;

alter table content_metadata add constraint content_metadata_indexer_configuration_id_fkey foreign key (indexer_configuration_id) references indexer_configuration(id) not valid;
alter table content_metadata validate constraint content_metadata_indexer_configuration_id_fkey;

-- directory_intrinsic_metadata
create unique index directory_intrinsic_metadata_pkey on directory_intrinsic_metadata(id, indexer_configuration_id);
alter table directory_intrinsic_metadata add primary key using index directory_intrinsic_metadata_pkey;

alter table directory_intrinsic_metadata add constraint directory_intrinsic_metadata_indexer_configuration_id_fkey foreign key (indexer_configuration_id) references indexer_configuration(id) not valid;
alter table directory_intrinsic_metadata validate constraint directory_intrinsic_metadata_indexer_configuration_id_fkey;

-- content_mimetype
create unique index content_mimetype_pkey on content_mimetype(id, indexer_configuration_id);
alter table content_mimetype add primary key using index content_mimetype_pkey;

alter table content_mimetype add constraint content_mimetype_indexer_configuration_id_fkey foreign key (indexer_configuration_id) references indexer_configuration(id) not valid;
alter table content_mimetype validate constraint content_mimetype_indexer_configuration_id_fkey;

create index on content_mimetype(id) where mimetype like 'text/%';

-- content_fossology_license
create unique index content_fossology_license_pkey on content_fossology_license(id, license_id, indexer_configuration_id);
alter table content_fossology_license add primary key using index content_fossology_license_pkey;

alter table content_fossology_license add constraint content_fossology_license_license_id_fkey foreign key (license_id) references fossology_license(id) not valid;
alter table content_fossology_license validate constraint content_fossology_license_license_id_fkey;

alter table content_fossology_license add constraint content_fossology_license_indexer_configuration_id_fkey foreign key (indexer_configuration_id) references indexer_configuration(id) not valid;
alter table content_fossology_license validate constraint content_fossology_license_indexer_configuration_id_fkey;

-- origin_intrinsic_metadata
create unique index origin_intrinsic_metadata_pkey on origin_intrinsic_metadata(id, indexer_configuration_id);
alter table origin_intrinsic_metadata add primary key using index origin_intrinsic_metadata_pkey;

alter table origin_intrinsic_metadata add constraint origin_intrinsic_metadata_indexer_configuration_id_fkey foreign key (indexer_configuration_id) references indexer_configuration(id) not valid;
alter table origin_intrinsic_metadata validate constraint origin_intrinsic_metadata_indexer_configuration_id_fkey;

create index origin_intrinsic_metadata_fulltext_idx on origin_intrinsic_metadata using gin (metadata_tsvector);
create index origin_intrinsic_metadata_mappings_idx on origin_intrinsic_metadata using gin (mappings);

-- origin_extrinsic_metadata
create unique index origin_extrinsic_metadata_pkey on origin_extrinsic_metadata(id, indexer_configuration_id);
alter table origin_extrinsic_metadata add primary key using index origin_extrinsic_metadata_pkey;

alter table origin_extrinsic_metadata add constraint origin_extrinsic_metadata_indexer_configuration_id_fkey foreign key (indexer_configuration_id) references indexer_configuration(id) not valid;
alter table origin_extrinsic_metadata validate constraint origin_extrinsic_metadata_indexer_configuration_id_fkey;

create index origin_extrinsic_metadata_fulltext_idx on origin_extrinsic_metadata using gin (metadata_tsvector);
create index origin_extrinsic_metadata_mappings_idx on origin_extrinsic_metadata using gin (mappings);
