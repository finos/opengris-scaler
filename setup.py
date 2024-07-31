from setuptools import find_packages, setup

from scaler.about import __version__

with open("requirements.txt", "rt") as f:
    requirements = [i.strip() for i in f.readlines()]

setup(
    name="scaler",
    version=__version__,
    packages=find_packages(exclude=("tests",)),
    install_requires=requirements,
    extras_require={
        "graphblas": ["python-graphblas", "numpy"],
        "uvloop": ["uvloop"],
        "gui": ["nicegui[plotly]"],
        "all": ["python-graphblas", "numpy", "uvloop", "nicegui[plotly]"],
    },
    url="",
    license="",
    author="Citi",
    author_email="opensource@citi.com",
    description="Scaler Distributed Framework",
    entry_points={
        "console_scripts": [
            "scaler_scheduler=scaler.entry_points.scheduler:main",
            "scaler_cluster=scaler.entry_points.cluster:main",
            "scaler_top=scaler.entry_points.top:main",
            "scaler_ui=scaler.entry_points.webui:main",
        ]
    },
)
