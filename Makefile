.PHONY: init

# install Poetry and Python dependencies
init:
	curl -sSL https://install.python-poetry.org | python3 -
	poetry install

# backfill OHLC data from $(from_day) to $(to_day)
backfill:
	poetry run python src/backfill.py --from_day $(from_day) --to_day $(to_day)