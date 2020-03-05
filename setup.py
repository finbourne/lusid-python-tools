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
        "lusid-sdk-preview>=0.10.975",
        "msrest==0.6.*",
        "xlrd==1.*",
        "pandas==1.0.*",
        "numpy==1.*",
        "pytz==2019.*",
        "tqdm==4.*",
        "requests==2.*",
        "coloredlogs==10.*",
        "detect_delimiter==0.1.*",
        "pyyaml==5.*",
        "flatten-json==0.1.*",
    ],
    include_package_data=True,
    python_requires=">=3.7",
    entry_points={
        "console_scripts": [
            "lusidtools=lusidtools.commands.commands:main",
            "upsert_instruments=lusidtools.apps.upsert_instruments:main",
            "upsert_holdings=lusidtools.apps.upsert_holdings:main",
            "upsert_quotes=lusidtools.apps.upsert_quotes:main",
            "upsert_transactions=lusidtools.apps.upsert_transactions:main",
        ],
    },
)
