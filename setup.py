from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="mirrulations-fetch",
    version="0.1.0",
    author="Ben Coleman",
    author_email="colemanb@moravian.edu",
    description="A command line tool to download docket data from the mirrulations S3 bucket",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mirrulations/mirrulations-fetch",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.9",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "mirrulations-fetch=mirrulations_fetch.download_docket:main",
        ],
    },
    keywords="data science, government, regulations, s3, download",
    project_urls={
        "Bug Reports": "https://github.com/mirrulations/mirrulations-fetch/issues",
        "Source": "https://github.com/mirrulations/mirrulations-fetch",
    },
) 