
from setuptools import setup, find_packages
from auntisara.version import get_version

with open("README.md", "r") as fh:
    long_description = fh.read()

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='auntisara',
    version=get_version(),
    url="https://github.com/michel4j/aunt-isara",   # modify this
    license='MIT',                              # modify this
    author='Michel Fodje',                      # modify this
    author_email='michel.fodje@lightsource.ca',              # modify this
    description='A python based Soft IOC Server for ISARA Automounter',
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords='epics device ioc development',
    include_package_data=True,
    packages=find_packages(),
    install_requires=requirements + [
        'devioc',
        'importlib-metadata ~= 1.0 ; python_version < "3.8"',
    ],
    scripts=[
        'bin/app.server',
    ],
    classifiers=[
        'Intended Audience :: Developers',
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
