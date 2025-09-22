SHELL := /bin/bash

VENV := .venv
PY   := $(VENV)/bin/python
PIP  := $(VENV)/bin/pip

.PHONY: bootstrap observe api fmt clean

bootstrap:
	python3 -m venv $(VENV)
	$(PIP) install -U pip
	$(PIP) install -r requirements.txt

observe:
	$(PY) -m services.engine.arb_loop --tokens configs/tokens.base.yml --pairs configs/pairs.base.yml

api:
	$(PY) -m uvicorn services.web.app:app --host 0.0.0.0 --port 8000 --reload

fmt:
	$(PY) -m pip install ruff || true
	ruff check . || true

clean:
	rm -rf $(VENV) __pycache__ .pytest_cache **/*.pyc *.pyc

