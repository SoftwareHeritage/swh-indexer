from setuptools import setup


def parse_requirements():
    requirements = []
    with open('requirements.txt') as f:
        for line in f.readlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            requirements.append(line)

    return requirements


# Edit this part to match your module
# full sample: https://forge.softwareheritage.org/diffusion/DCORE/browse/master/setup.py
setup(
    name='swh.<module-name>',
    description='Software Heritage <Module\'s intent>',
    author='Software Heritage developers',
    author_email='swh-devel@inria.fr',
    url='https://forge.softwareheritage.org/diffusion/<module-git-code>',
    packages=[],  # packages's modules
    scripts=[],   # scripts to package
    install_requires=parse_requirements(),
    setup_requires=['vcversioner'],
    vcversioner={},
    include_package_data=True,
)
