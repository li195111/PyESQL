import setuptools
from setuptools import setup

dependencies = [
    "psycopg[binary] ; platform_system!='Windows'",
    "psycopg2 ; platform_system!='Windows'",
    "psycopg2-binary ; platform_system=='Windows'"
]

packages = [
    package
    for package in setuptools.PEP420PackageFinder().find()
]

setup (
    name='pyesql',
    version='0.2.22',
    description='Python Easy SQL Connector',
    url='https://github.com/li195111/PyESQL',
    author='Yue Li',
    author_email='green07111@gmail.com',
    install_requires=dependencies,
    lincense='MIT',
    packages=packages,
    zip_safe=False,
    keywords=[
        'SQL',
        'NoSQL',
        'Easy SQL',
        'SQL Connector'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7'
    ],
)
