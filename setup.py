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
        "coloredlogs==14.0",
        "detect_delimiter==0.1.1",
        "flatten-json==0.1.7",
        "lusid-sdk-preview>=0.11.*",
        "msrest==0.6.16",
        "numpy>=1.19.0",
        "openpyxl==3.0.7",
        "pandas>=1.1.4",
        "pyyaml==5.4",
        "pytz==2019.3",
        "tqdm==4.52.0",
        "requests==2.27.1",
        "urllib3==1.26.9",
        "xlrd==1.2.0"
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
