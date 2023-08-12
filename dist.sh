#!/bin/bash
python -m setup.py sdist bdist_wheel
python -m twine upload dist/*
rm -rf build
rm -rf dist
rm -rf gcp_scraper.egg-info