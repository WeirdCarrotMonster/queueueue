from setuptools import setup, find_packages


setup(
    name="queueueue",
    version="0.1",
    maintainer="Eugene Protozanov",
    maintainer_email="protozanov@noblecode.ru",
    description="Advanced task queueueue",
    packages=find_packages(),
    install_requires=[
        'aiohttp==3.3.2'
    ],
    entry_points={
        'console_scripts': [
            'queueueue = queueueue.main:main',
        ],
    },
)
