import setuptools

setuptools.setup(
    name="dagter_data_lake",
    packages=setuptools.find_packages(exclude=["dagter_data_lake_tests"]),
    install_requires=[
        "dagster==0.15.7",
        "dagit==0.15.7",
        "pytest",
    ],
)
