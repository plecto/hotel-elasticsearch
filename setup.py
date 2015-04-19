# encoding: utf-8
from setuptools import setup

setup(
    name='hotel-elasticsearch',
    version='0.1',
    packages=['hotel_elasticsearch'],
    url='',
    license='',
    author='Kristian Øllegaard',
    author_email='kristian@plecto.com',
    description='',
    install_requires=[
        'PyYAML',
        'flask',
        'boto',
        'requests',
        'frigga-snake'
    ],
    entry_points={
        'console_scripts': [
            'hotel-elasticsearch = hotel_elasticsearch.app:run',
        ]
    }
)
