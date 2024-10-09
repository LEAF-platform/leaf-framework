from setuptools import setup, find_packages

with open("README", 'r') as f:
    long_description = f.read()

setup(
    name='gridion-adapter',
    version='0.1',
    description='Adapter for Gridion',
    long_description=long_description,
    author='Jasper Koehorst',
    author_email='jasper.koehorst@wur.nl',
    packages=find_packages(),
    install_requires=[
        'requests',
    ],
)
