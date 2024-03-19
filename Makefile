.PHONY: fmt pkg

fmt:
	@python -m black .

pkg:
	@python -m build
