from setuptools import setup, find_packages


def parse_requirements():
    requirements = []
    for reqf in ('requirements.txt', 'requirements-swh.txt'):
        with open(reqf) as f:
            for line in f.readlines():
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                requirements.append(line)
    return requirements


setup(
    name='swh.indexer',
    description='Software Heritage Content Indexer',
    author='Software Heritage developers',
    author_email='swh-devel@inria.fr',
    url='https://forge.softwareheritage.org/diffusion/78/',
    packages=find_packages(),
    scripts=[],
    install_requires=parse_requirements(),
    setup_requires=['vcversioner'],
    vcversioner={},
    include_package_data=True,
)
