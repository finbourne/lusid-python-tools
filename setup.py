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
        "lusid-sdk-preview==0.10.*",
        "msrest==0.6.*",
        "xlrd==1.*",
        "pandas==0.25.*",
        "numpy==1.*",
        "pytz==2019.*",
        "tqdm==4.*",
        "requests==2.*",
        "coloredlogs==10.*",
        "detect_delimiter==0.1.*",
        "pyyaml==5.*",
    ],
    include_package_data=True,
    python_requires=">=3.7",
)
