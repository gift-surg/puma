import logging
from platform import system
from typing import List

from setuptools import setup, find_packages

import versioneer

dev_deps = [
    "flake8==3.6.0",
    "flake8-import-order==0.18.1",
    "mypy==0.641",
    "parameterized==0.6.1",
    "pprofile==2.0.2",
    "pytest==4.0.0",
    "pytest-html==1.20.0",
    "tox==3.5.3"
]


def is_macos() -> bool:
    return platform_name() == "Darwin"


def platform_name() -> str:
    return system()


def get_gpu_dependencies() -> List[str]:
    if is_macos():
        # This warning doesn't get logged unless pip is run with the verbose (-v) flag. Nonetheless, it might
        # be handy in debugging problems later esp. if PUMA is installed on unsupported platforms.
        logging.warning(f'macOS is not a fully supported platform - some dependencies may not be available and'
                        f' you may not be able to use all capabilities of PUMA')
    return ["cupy-cuda100", ]


with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='puma',
    description='Python Unified Multi-tasking API (PUMA)',
    long_description=long_description,
    long_description_content_type='text/markdown',

    version=versioneer.get_version(),

    url='https://github.com/gift-surg/puma',
    author='GIFT-Surg Consortium',

    packages=find_packages(),
    # packages=[
    #     'puma'
    # ],

    cmdclass=versioneer.get_cmdclass(),
    include_package_data=True,
    install_requires=[
        "PyYAML==5.1.1",
        "tblib==1.3.2",
        "typing-extensions==3.7.4",
    ],
    extras_require={
        "dev": dev_deps,
        "gpu": get_gpu_dependencies(),
    }
)
