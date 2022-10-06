help:
	@echo "init - set up the virtual environment prior to running the application"
	@echo "analyze - check style"
	@echo "run_tests - run the test cases with coverage"
	@echo "run - clean python artifacts, zip and run the job"

# set the default goal to run the application
.DEFAULT_GOAL := run

# set up the virtual environment prior to running the application
init:
	pipenv --three install
	pipenv shell

# to analyze the code style
analyze:
	flake8 ./src

# run the test cases with coverage
run_tests:
	pytest --cov=src test/jobs/

# command line example: make run MASTER_URL=local JOB_NAME=pi CONF_PATH=/your/path/hellofresh-pyspark-test/src/jobs
run:
	# cleanup
	find . -name '__pycache__' | xargs rm -rf
	rm -f jobs.zip

	# create the zip
	cd src/ && zip -r ../jobs.zip jobs/

  # run the job
	spark-submit --master $(MASTER_URL) --py-files jobs.zip src/main.py --job $(JOB_NAME) --res-path $(CONF_PATH)