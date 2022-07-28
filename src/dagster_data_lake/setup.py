import setuptools

setuptools.setup(
    name="dagster_data_lake",
    packages=setuptools.find_packages(exclude=["dagster_data_lake_tests"]),
    install_requires=[
        "dagster==0.15.8",
        "dagit==0.15.8",
        "pytest",
    ],
)
