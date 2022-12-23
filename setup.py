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
        "urllib3>=1.26.9",
        "requests>=2.27.1",
        "coloredlogs>=14.0",
        "detect_delimiter>=0.1",
        "flatten-json>=0.1.7",
        "pandas>=1.1.4",
        "PyYAML>=5.4",
        "tqdm>=4.52.0",
        "openpyxl>=3.0.7",
        "xlrd~=1.2",
        "pytz>=2019.3",
        "IPython>=7.31.1",
        "lusid-sdk-preview>=0.11.4425, < 2",
    ],
    include_package_data=True,
    python_requires=">=3.7",
    entry_points={
        "console_scripts": [
            "upsert_portfolios=lusidtools.apps.upsert_portfolios:main",
            "lusidtools=lusidtools.commands.commands:main",
            "upsert_instruments=lusidtools.apps.upsert_instruments:main",
            "upsert_holdings=lusidtools.apps.upsert_holdings:main",
            "upsert_quotes=lusidtools.apps.upsert_quotes:main",
            "upsert_transactions=lusidtools.apps.upsert_transactions:main",
            "flush_transactions=lusidtools.apps.flush_transactions:main",
        ],
    },
)
