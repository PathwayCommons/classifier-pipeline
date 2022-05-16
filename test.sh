#! /bin/bash
echo "Tests"
poetry run pytest tests --cov=classifier_pipeline --cov-config=.coveragerc

echo "Linting"
poetry run flake8 ./classifier_pipeline --count --select=E9,F63,F7,F82 --show-source --statistics
poetry run flake8 ./classifier_pipeline --count --exit-zero --max-complexity=10 --statistics

echo "Type checking"
poetry run mypy . --cache-dir=/dev/null