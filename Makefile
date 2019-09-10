.DEFAULT_GOAL := test

test:
	pylint tap_mongodb tap_mongodb/sync_strategies -d missing-docstring,fixme
