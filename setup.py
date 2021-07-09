
from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()
    
setup(
    name='auntisara',
    version="1.0.0",
    url="https://github.com/michel4j/aunt-isara",   # modify this
    license='MIT',                              # modify this
    author='Michel Fodje',                      # modify this
    author_email='michel.fodje@lightsource.ca',              # modify this
    description='A python based Soft IOC Server for ISARA Automounter',
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords='epics device ioc development',
    packages=['auntisara'],
    scripts=[
        'bin/app.server'
    ],
    install_requires= [
        'devioc',
    ],
    classifiers=[
        'Intended Audience :: Developers',
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
