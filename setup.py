# encoding: utf-8
from setuptools import setup

setup(
    name='hotel-elasticsearch',
    version='1.0.5',
    packages=['hotel_elasticsearch'],
    url='',
    license='',
    author='Kristian Øllegaard',
    author_email='kristian@plecto.com',
    description='',
    install_requires=[
        'PyYAML',
        'flask==2.2.5',
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
