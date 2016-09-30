# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Module to communicate with storage (mongodb instance).

"""

import click

from pymongo import MongoClient

from swh.core.config import SWHConfig


class Storage(SWHConfig):
    """SWH Storage encompassing mongodb.

    """
    DEFAULT_CONFIG = {
        'db': ('dict', {
            'conn': 'mongodb://mongodb0.example.net:27017',
            'name': 'content',
        })
    }

    CONFIG_BASE_FILENAME = 'storage/mongo.ini'

    def __init__(self, db_conn=None, db_name=None):
        """Initialize a storage.

        """
        config = {}
        if not db_conn and not db_name:
            config = self.parse_config_file()

        if not db_conn:
            db_conn = config['db']['conn']
        if not db_name:
            db_name = config['db']['name']

        client = MongoClient(db_conn)
        self.db = client[db_name]

    def content_get(self, content_id):
        """Return the full content from its id.

        Args:
            content_id (str): the hex sha1 representing the content's
            identifier

        Returns:
            The content as dict.

        """
        c = list(self.db['content'].find({'sha1': content_id}).limit(1))
        if c:
            return c[0]

    def content_add(self, content):
        """Insert or Update an existing content.

        Args:
            content (dict): the content with the new data to update.
            This dict should have at least the key 'sha1' filled.

        Returns:
            The internal id of the data updated or upserted.

        """
        _id = None
        if '_id' in content:
            _id = content.pop('_id')

        r = self.db['content'].update_one(
            filter={
                'sha1': content['sha1']
            },
            update={
                "$set": content,
            },
            upsert=True)  # if not existing, this will create it

        if r.upserted_id:
            return str(r.upserted_id)
        return _id


@click.command()
def main():
    """Basic tryout:
    - Instanciate a storage
    - Create a content
    - Update it
    - Read it

    """
    hash1 = 'a5c0e4c876d10a313b6c38facb4dd9d807f64ec4'
    content = {
        'sha1': hash1
    }

    storage = Storage(db_conn='mongodb://localhost:27017', db_name='content')

    # insert new content
    content_internal_id = storage.content_add(content)
    print('content internal id:', content_internal_id)

    # update existing content
    updated_content = content.copy()
    updated_content.update({"mime-type": "text-plain"})
    content_updated_id = storage.content_add(updated_content)
    print('content updated id:', content_updated_id)

    # Retrieve content
    actual_content = storage.content_get(hash1)
    print('content 1:', actual_content)


if __name__ == '__main__':
    main()
