from setuptools import setup, find_packages
from mindsdb_sdk import __about__ as C

with open("README.md", "r") as fh:
    long_description = fh.read()

with open('requirements.txt') as req_file:
    requirements = req_file.read().splitlines()

setup(
    name=C.__title__,
    version=C.__version__,
    url=C.__github__,
    download_url=C.__pypi__,
    license=C.__license__,
    author=C.__author__,
    author_email=C.__email__,
    description=C.__description__,
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=('tests*', 'testing*')),
    install_requires=requirements,
    extras_require={
        'dev': [
            'pytest',
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
