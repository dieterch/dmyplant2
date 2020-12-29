import os
import sys
import re

from dmyplant2 import __version__ as version

readme = os.path.join(os.path.dirname(__file__), 'README.rst')
long_description = open(readme).read()

SETUP_ARGS = dict(
    name='dmyplant',
    version=version,
    description=(
        'Grabs Myplant Information for a Validation fleet and populates an Excel File'),
    long_description=long_description,
    url='https://github.com/<your login>/dmyplant',
    author='Dieter Chvatal',
    author_email='dieter.chvatal@innio.com',
    license='MIT',
    include_package_data=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: CLI Environment',
        'Intended Audience :: INNIO Engineers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.7+',
    ],
    py_modules=['dmyplant', ],
    install_requires=[
        'requests>=2.22',
        'matplotlib>=3.2.2',
        'numpy>=1.18.5',
        'pandas>=1.0.5',
        'scipy>=1.5.2'
    ],
)

if __name__ == '__main__':
    from setuptools import setup, find_packages

    SETUP_ARGS['packages'] = find_packages()
    setup(**SETUP_ARGS)
