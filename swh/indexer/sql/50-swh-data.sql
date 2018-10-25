insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('nomos', '3.1.0rc2-31-ga2cbb8c', '{"command_line": "nomossa <filepath>"}');

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('file', '5.22', '{"command_line": "file --mime <filepath>"}');

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('universal-ctags', '~git7859817b', '{"command_line": "ctags --fields=+lnz --sort=no --links=no --output-format=json <filepath>"}');

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('pygments', '2.0.1+dfsg-1.1+deb8u1', '{"type": "library", "debian-package": "python3-pygments"}');

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('pygments', '2.0.1+dfsg-1.1+deb8u1', '{"type": "library", "debian-package": "python3-pygments", "max_content_size": 10240}');

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('swh-metadata-translator', '0.0.1', '{"type": "local", "context": "NpmMapping"}');

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('swh-metadata-detector', '0.0.1', '{"type": "local", "context": ["NpmMapping", "CodemetaMapping"]}');

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('swh-deposit', '0.0.1', '{"sword_version": "2"}');

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('file', '1:5.30-1+deb9u1', '{"type": "library", "debian-package": "python3-magic"}');
