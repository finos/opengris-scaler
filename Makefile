.PHONY: doc

# declare your targets here & implement them with '_' prefix
doc:
# target name is formatted:
# bold - \033[1m, purple - \033[095m, normal text - \033[0m
	@echo -e "Executing \033[1m\033[095m$@\033[0m target:"
	@$(MAKE) --no-print-directory _$@
	@echo

_doc:
	rm -fr docsvenv build; mkdir build
	python3.8 -m venv docsvenv
	. docsvenv/bin/activate; \
		pip install -r docs/requirements_docs.txt; \
		pip install -r requirements.txt; \
		cd docs; make clean && make html
	zip -r build/scaler_docs.zip docs/build/html/*
