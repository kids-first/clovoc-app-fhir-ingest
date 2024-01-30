import os
from setuptools import setup, find_packages


root_dir = os.path.dirname(os.path.abspath(__file__))

# Read in long description from README
with open(os.path.join(root_dir, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

# Read in requirements from requirements.txt
req_file = os.path.join(root_dir, "requirements.txt")
with open(req_file) as f:
    requirements = f.read().splitlines()

setup(
    name="clovoc-app-fhir-ingest",
    version="0.1.0",
    description="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    install_requires=requirements,
    python_requires=">=3.8, <4",
)
