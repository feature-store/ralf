format:
	autoflake --in-place --remove-all-unused-imports --remove-unused-variables -r .
	isort --profile=black .
	black .

lint:
	black --check .
	isort --check --profile=black .
	flake8