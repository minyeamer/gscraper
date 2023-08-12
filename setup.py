from setuptools import setup, find_packages
from gscraper import __version__

REQUIRES = [
    "pandas>=1.1.4",
    "openpyxl>=3.0.10",
    "tqdm>=4.64.0",
    "asyncio>=3.4.3",
    "bs4>=0.0.1",
    "requests>=2.28.1",
    "aiohttp>=3.8.3",
    "aiohttp-proxy>=0.1.2",
    "lxml>=4.9.1",
    "gspread>=5.6.2",
    "google-cloud-bigquery>=3.4.0",
    "pandas-gbq>=0.17.9"]

setup(
    name="gcp-scraper",
    version=__version__,
    description="Scraping utils with GCP functions",
    url="https://github.com/minyeamer/gscraper.git",
    author="minyeamer",
    author_email="minyeamer@gmail.com",
    license="minyeamer",
    install_requires=REQUIRES,
    packages=find_packages(),
    keywords=["gcp-scraper", "scraper", "gcp"],
    python_requires=">=3.7",
)