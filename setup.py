# setup.py

from setuptools import setup, find_packages

setup(
    name="labellerr_sdk",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "requests",
        "unique_names_generator"
    ],
    description="Python SDK for Labellerr API",
    author="Your Name",
    author_email="your.email@example.com",
    url="https://github.com/tensormatics/SDKPython",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)

