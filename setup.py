from setuptools import setup
from io import open


with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='puma',
    description='Python Unified Multi-tasking API (PUMA)',
    long_description=long_description,
    long_description_content_type='text/markdown',

    version='0.0.0rc1',

    url='https://github.com/gift-surg/puma',
    author='GIFT-Surg Consortium',

    packages=[
        'puma',
    ],
    extras_require={
        'dev': [
            'pytest==5.2.1',
        ],
    }
)
