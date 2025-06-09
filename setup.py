from setuptools import setup, find_packages

setup(
    name="keviusdb",
    version="1.0.0",
    description="A fast key-value storage library with ordered mapping",
    author="Ivan APEDO",
    packages=find_packages(),
    install_requires=[
        "lz4>=4.0.0",  # Fast compression
        "sortedcontainers>=2.4.0",  # Efficient sorted data structures
    ],
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
