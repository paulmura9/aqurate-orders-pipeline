PY ?= python3
export PYTHONPATH := src

.PHONY: install setup ingest fx transform all profile report health test

install:          ## install dependencies
	$(PY) -m pip install -r requirements.txt

setup:            ## create schema, functions and views
	$(PY) -m pipeline.run setup

ingest:           ## step 1 - load orders_raw
	$(PY) -m pipeline.run ingest

fx:               ## step 3 - load fx_rates
	$(PY) -m pipeline.run fx

transform:        ## steps 2, 4, 5 - clean + marts + quality gate
	$(PY) -m pipeline.run transform

all:              ## the full daily run
	$(PY) -m pipeline.run all

profile:          ## data profiling report for orders_raw
	$(PY) -m pipeline.run profile

report:           ## what the marts currently contain
	$(PY) -m pipeline.run report

health:           ## last run per step + open quality issues
	$(PY) -m pipeline.run health

test:             ## unit tests (no database needed)
	$(PY) -m pytest -q
