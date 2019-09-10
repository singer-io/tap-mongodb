.DEFAULT_GOAL := test

test:
	pylint tap_mongodb -d missing-docstring,fixme
