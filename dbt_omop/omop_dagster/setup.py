from setuptools import find_packages, setup

setup(
    name="omop_dagster",
    version="0.0.1",
    packages=find_packages(),
    package_data={
        "omop_dagster": [
            "dbt-project/**/*",
        ],
    },
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-trino<1.9",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
        ]
    },
)