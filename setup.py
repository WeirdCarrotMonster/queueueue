from setuptools import setup, find_packages


setup(
    name="overseer",
    version="0.1",
    maintainer="Eugene Protozanov",
    maintainer_email="protozanov@noblecode.ru",
    description="Overseer task queue",
    packages=find_packages(),
    install_requires=[
        'aiohttp',
        'simplejson'
    ],
    entry_points={
        'console_scripts': [
            'overseer = overseer.overseer:main',
        ],
    },
)
