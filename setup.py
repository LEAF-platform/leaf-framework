# core/setup.py
from setuptools import setup, find_packages

setup(
    name='leaf_core',
    version='0.1',
    packages=find_packages(include=['core', 'core.*']),  # This includes the 'core' package
    install_requires=[
        'paho-mqtt',  # Add other dependencies if necessary
    ],
    description='Core functionality for LEAF including MQTT client',
    author='Your Name',
    author_email='you@example.com',
)

