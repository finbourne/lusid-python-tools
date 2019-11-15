from setuptools import setup, find_packages  # noqa: H301

version = {}
with open("./__version__.py") as fp:
    exec(fp.read(), version)

setup(
    name="lusidtools",
    version=version["__version__"],
    description="Python Tools for LUSID",
    url="https://github.com/finbourne/lusid-python-tools",
    author="FINBOURNE Technology",
    author_email="engineering@finbourne.com",
    license="MIT",
    keywords=["FINBOURNE", "LUSID", "LUSID API", "python"],
    packages=find_packages(exclude=["tests*"]),
    install_requires=[
        "lusid-sdk-preview>=0.10.840",
        "msrest",
        "pandas",
        "numpy",
        "pytz",
        "tqdm>=4.30.0",
        "requests",
        "coloredlogs",
        "detect_delimiter",
    ],
    include_package_data=True,
    python_requires=">=3.6",
)
