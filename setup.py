# encoding: utf-8
from setuptools import setup

setup(
    name='hotel-elasticsearch',
    version='2.0.0',
    packages=['hotel_elasticsearch'],
    url='',
    license='',
    author='Kristian Øllegaard',
    author_email='kristian@plecto.com',
    description='',
    install_requires=[
        'PyYAML',
        'boto3',
        'requests',
        'frigga-snake',
        'aws-secretsmanager-caching'
    ],
    entry_points={
        'console_scripts': [
            'hotel-elasticsearch = hotel_elasticsearch.app:main',
        ]
    }
)
