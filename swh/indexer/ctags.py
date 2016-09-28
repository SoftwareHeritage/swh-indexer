# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import logging
import os
import subprocess

# maximum number of detailed warnings for malformed tags that will be emitted.
# used to avoid flooding logs
BAD_TAGS_THRESHOLD = 5

# Option used to generate the tag file
CTAGS_FLAGS = ['--recurse',
               '--excmd=number',
               '--fields=+lnz',
               '--sort=no',
               '--links=no']


def run_ctags(pkg_path):
    """Run ctags for pkg_path.

    """
    ctagsfile = pkg_path + '/tags'
    ctagsfile_tmp = ctagsfile + '.new'
    # debian: 'sudo update-alternatives --config ctags' and choose
    # ctags-exuberant
    cmd = ['ctags'] + CTAGS_FLAGS + ['-o', ctagsfile_tmp]
    # ASSUMPTION: will be run under pkgdir as CWD, which is needed to
    # get relative paths right. The assumption is enforced by the
    # updater
    with open(os.devnull, 'w') as null:
        subprocess.check_call(cmd, stderr=null)
    os.rename(ctagsfile_tmp, ctagsfile)


def parse_ctags(path):
    """Parse exuberant ctags tags file.

    Args:
        path: Path to the ctag file

    Yields:
        For each tag, a tag dictionary with the keys:
            - tag:  'TAG_NAME',
            - path: 'PATH/WITH/IN/PACKAGE',
            - line: LINE_NUMBER, # int
            - kind: 'TAG_KIND', # 1 letter
            - language: 'TAG_LANGUAGE',

    """
    def parse_tag(line):
        tag = {'kind': None, 'line': None, 'language': None}
        # initialize with extension fields which are not guaranteed to exist
        fields = line.rstrip().split('\t')

        tag['tag'] = fields[0]
        tag['path'] = fields[1]

        for ext in fields[3:]:  # parse extension fields
            k, v = ext.split(':', 1)  # caution: "typeref:struct:__RAW_R_INFO"
            if k == 'kind':
                tag['kind'] = v
            elif k == 'line':
                tag['line'] = int(v)
            elif k == 'language':
                tag['language'] = v.lower()
            else:
                pass  # ignore other fields

        assert tag['line'] is not None
        return tag

    with open(path) as ctags:
        bad_tags = 0
        for line in ctags:
            # e.g. 'music\tsound.c\t13;"\tkind:v\tline:13\tlanguage:C\tfile:\n'
            # see CTAGS(1), section "TAG FILE FORMAT"
            if line.startswith('!_TAG'):  # skip ctags metadata
                continue
            try:
                yield parse_tag(line)
            except:
                bad_tags += 1
                if bad_tags <= BAD_TAGS_THRESHOLD:
                    logging.warn('ignore malformed tag "%s"' % line.rstrip())
        if bad_tags > BAD_TAGS_THRESHOLD:
            logging.warn('%d extra malformed tag(s) ignored' %
                         (bad_tags - BAD_TAGS_THRESHOLD))


@click.command()
@click.option('--path', help="Path to execute index on")
def main(path):
    r = list(parse_ctags(path))
    print(r)


if __name__ == '__main__':
    main()
