from setuptools import find_packages, setup

setup(
    name="ralf",
    version="0.0.1",
    author="Ralf Team",
    description=("A feature computation engine."),
    url="https://github.com/sarahwooders/flink-feature-flow/",
    keywords=("feature store streaming machine learning python"),
    packages=find_packages(),
    install_requires=["ray[serve]", "requests",],
)
