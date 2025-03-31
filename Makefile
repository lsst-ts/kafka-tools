.PHONY: help
help:
	@echo "Make targets for kafka-tools:"
	@echo "make init - Set up dev environment (install pre-commit hooks)"

.PHONY: init
init:
	generate_pre_commit_conf --overwrite
	pip install -e .[dev]
