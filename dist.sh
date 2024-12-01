#!/bin/bash
python -m setup.py sdist bdist_wheel
for file in dist/gcp-scraper*; do
    mv "$file" "${file/gcp-scraper/gcp_scraper}"
done
python -m twine upload dist/*
rm -rf build
rm -rf dist
rm -rf gcp_scraper.egg-info