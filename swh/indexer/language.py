# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import click
import json
import os
import math
import sys

from pygments.lexers import guess_lexer
from pygments.util import ClassNotFound


def cleanup_classname(classname):
    """Determine the language from the pygments' lexer names.

    """
    return classname.lower().replace(' ', '-')


def run_language(path, encoding=None):
    """Determine mime-type from file at path.

    Args:
        path (str): filepath to determine the mime type
        encoding (str): optional file's encoding

    Returns:
        Dict with keys:
        - lang: None if nothing found or the possible language
        - decoding_failure: True if a decoding failure happened

    """
    try:
        with open(path, 'r', encoding=encoding) as f:
            try:
                raw_content = f.read()
                lang = cleanup_classname(
                    guess_lexer(raw_content).name)
                return {
                    'lang': lang
                }
            except ClassNotFound as e:
                return {
                    'lang': None
                }
    except LookupError as e:  # Unknown encoding
        return {
            'decoding_failure': True,
            'lang': None
        }


class LanguageDetector():
    """Bayesian filter to learn and detect languages based on the source
    code.

    """
    def __init__(self):
        self.data = {}
        self.totals = {}

    def tokenize(self, code):
        """Split the source code in tokens.

        Args:
            code (str): source code

        Yields:
            list of words

        """
        for token in code.split():
            if not token:
                continue
            yield token

    def train(self, code, lang):
        """Train to recognize the sample code as language lang.

        Args:
            code: the source code in language lang
            lang: the language the source code is written in

        """
        lang_entry = {}
        for word in self.tokenize(code):
            lang_entry[word] = lang_entry.get(word, 0) + 1
            self.totals[word] = self.totals.get(word, 0) + 1

        self.data[lang] = lang_entry

    def compute_prob(self, words, lang):
        """Calculates the probability on the sample words for language lang.

        Args:
            words: word sample to compute probability on
            lang: language concerned

        Returns:
            The probability for that sample in that language

        """
        res = 0.0
        for word in words:
            try:
                res = res + math.log(self.totals[word]/self.data[lang][word])
            except(KeyError):
                continue
        return res

    def detect(self, code):
        """Determine the most probable language for that source code.

        This is completely dependent on the state of the classifier
        once it has been trained (cf. `train` function)

        Args:
            code: the file's source code

        Returns:
            the most probable language for that source code

        """
        lang_prob = {}
        words = list(self.tokenize(code))
        for lang in self.data.keys():
            prob = self.compute_prob(words, lang)
            lang_prob[prob] = lang
        return lang_prob[min(lang_prob.keys())]


class JsonLanguageDetector(LanguageDetector):
    """Language detector with load/dump json on disk abilities.

    """
    def dump(self, path):
        """Dumps the detector's state into json at path.

        Args:
            path: Path to where dumping the json.

        """
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f:
            f.write(json.dumps({
                'data': self.data,
                'totals': self.totals
            }))

    def load(self, path):
        """Load the detector's state from a json file.

        Args:
            path: Path to a json file. If the file does not exist, do nothing.

        """
        if not os.path.exists(path):
            return

        with open(path, 'r') as f:
            data = json.loads(f.read())
            if 'data'in data:
                self.data = data['data']
            if 'totals' in data:
                self.totals = data['totals']


@click.group()
def cli():
    pass


@click.command(help='Learn language from language snippet')
@click.option('--path', required=1, help='Path to source code file to '
              'learn from')
@click.option('--lang', required=1, help='Language for that source code')
@click.option('--dump', help='Read and dump computed state at that path')
def learn_one_language(path, lang, dump):
    detector = JsonLanguageDetector()
    if dump:
        detector.load(path=dump)

    with open(path, 'r') as f:
        detector.train(f.read(), lang=lang)

    if dump:
        detector.dump(path=dump)


@click.command(help='Learn language from language snippets')
@click.option('--dump', help='Read and dump computed state at that path')
def learn(dump):
    detector = JsonLanguageDetector()

    if dump:
        detector.load(path=dump)

    for path in sys.stdin:
        path = path.rstrip()
        name = os.path.splitext(path)[0]
        lang = name.lower().replace('_', '-').replace(' ', '-')
        print('From %s, learning %s' % (path, lang))
        with open(path, 'r') as f:
            detector.train(f.read(), lang=lang)

    if dump:
        detector.dump(path=dump)


@click.command(help='Detect language from source code')
@click.option('--path', required=1, help='Path to source code to detect '
              'language')
@click.option('--dump', help='Read and dump computed state at that path')
def detect(path, dump):
    detector = JsonLanguageDetector()
    if dump:
        detector.load(path=dump)

    with open(path, 'r') as f:
        try:
            lang = detector.detect(f.read())
        except:
            lang = None

    print(lang)
    return lang


cli.add_command(learn_one_language)
cli.add_command(learn)
cli.add_command(detect)


if __name__ == '__main__':
    cli()
