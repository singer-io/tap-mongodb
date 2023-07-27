.DEFAULT_GOAL := test

test:
	pylint tap_mongodb tap_mongodb/sync_strategies -d missing-docstring,fixme,duplicate-code,line-too-long,too-many-statements,too-many-locals,consider-using-f-string,consider-using-from-import,broad-exception-raised,superfluous-parens,consider-using-generator
