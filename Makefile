.PHONY: default install bundle release test
.DEFAULT_GOAL := default

default: install
install: venv
	@. venv/bin/activate && pip install -r requirements.txt
venv:
	@test -d venv || python -m venv venv
bundle:
	@create-corva-app zip . --bump-version=skip
release:
	@create-corva-app release . --bump-version=skip
test: venv
	@. venv/bin/activate && pytest
