from setuptools import setup, find_packages

setup(
    name="celrix",
    version="0.1.0",
    description="Python client for CELRIX database",
    packages=find_packages(),
    install_requires=[
        "asyncio",
    ],
)
