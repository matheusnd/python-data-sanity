install_deps:
	@pip3 install -r requirements.txt --user

run: install_deps
	@python3 validation_job/main.py
