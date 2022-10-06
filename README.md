# Solution Approach to the requirements
  Following are the tools/libraries I have used to implement the 
	pipenv - To set up the dependencies in a isolated virtual environment
	Makefile - To automate the development steps in Linux/Mac environments
	pyspark - As per the requirement with python 3.7
	flake8 - To check the quality of the code (code style)
	pytest-spark - To implement unit tests for pyspark jobs
	pytest-cov - To generate a test coverage report to tell if we have enough unit tests
	pytest-ordering - To control the order of running the test cases (preprocessor first then aggregator)
	isodate - To parse the duration in PT format
	
  Also, I have created a generic project structure to run multiple pyspark jobs with logging and configuration enabled for each job.
	
  ## Task1
	Created a job named "recipedata_preprocessor" which does the following things,
	1) Read the archive json from local resources directory (downloaded to local from s3)
	2) Perform data cleansing by considering only recipes which do have either cookTime or prepTime to get accurate analysis
	3) Extract the time elapsed in minutes from isodate format columns cookTime and prepTime and compute the total cook time for further analysis
	4) Consider only recipes with total cooking time > 0 (to eliminate any records with cookTime=PT and prepTime=PT)
	5) Finally, save the processed output to parquet format for further processing as parquet format best fits for analytical purposes and Spark plays well with parquet utilising all the optimizations and benefits of parquet file format.
	
  ## Task2
	Created a job named "recipedata_aggregator" which does the following things,
	1) Read the processed parquet format data
	2) Consider only recipes with ingredient as "beef" in it (parameterized through config file)
	3) Derive a new column "difficulty" from the total cook time as per the requirement
	4) Compute the average cooking time per difficulty level
	5) Finally, save the aggregated output to csv file to view as a report.


# Data Exploration
	Following are the observations made while exploring the recipes data,
	1) There are many records which are older than past 8 years (before 2012, though it is mentioned as past 8 years archive)
	2) There are many records which do not have cookTime, prepTime, recipeyield and even name of the recipe
	3) Count of source archive recipe records : 1042
	4) Count of valid and processed recipe records: 738
	5) Count of recipes with beef as one of the ingredients: 47
	

# Assumptions/Considerations
	Following assumptions/considerations are made during the development of the solution for the requirements,
	1) Considered the data store as "local" against "s3". However, solution can be configured and extended to work with s3 by adding hadoop-aws and aws-sdk libraries with proper authentication mechanism
	2) Considered the Operating system environment as Linux/Mac against Windows. However, no special changes need to be made to the code to work in Windows except for automating the development steps using Makefile as it doesn't work natively in Windows
	3) Considered the following criteria for identifying bad data,
	   a. if there is no name for the recipe
	   b. if both cookTime and prepTime are empty (I assumed that every recipe should have either cookTime or prepTime) and this criteria covered "a" as well
	   
	   I tried to explore more by browsing the URLs of the recipes which do not have both cookTime and prepTime,
	   
	   {"name": "", "ingredients": "Salt\nOne 35-ounce can Italian plum tomatoes (preferably San Marzano) with their liquid\n1 pound penne\n1/4 cup extra-virgin olive oil\n10 cloves garlic, peeled\nCrushed hot red pepper\n1/4 cup vodka\n1/2 cup heavy cream\n2 tablespoons unsalted butter or olive oil for finishing the sauce, if you like\n2 to 3 tablespoons chopped fresh Italian parsley\n3/4 cup freshly grated Parmigiano-Reggiano, plus more for passing if you like", "url": "http://www.101cookbooks.com/archives/000054.html", "image": "http://www.101cookbooks.com/mt-static/images/food/.jpg", "cookTime": "", "recipeYield": "", "datePublished": "2003-05-27", "prepTime": "", "description": "101 Cookbooks: Penne alla Vodka Recipe"}
	   
	   I browsed the URL http://www.101cookbooks.com/archives/000054.html to check for any description mentioning cooking/heating time, I found a sentence mentioning heat up to 8 to 10 minutes but same information I couldn't grab by looking at the html elements from the "View page source" with cookTime and prepTime mentioned as just "PT".
	   
	   I didn't focus on realigning the missing attributes in the data.
	4) I considered and populated avg_total_cooking_time value in minutes in the report
	5) I didn't consider usage of any orchestration tool like Airflow, Luigi to create a workflow. However, we can use crontab integration with Makefile to schedule the jobs
	
	I am happy to walk through my code if needed.
	
## Covering other areas,
  1) Config handling - Configuration is enabled at each job level through a json file in the resources directory
  2) Logging and alerting - Logging is enabled using Spark's log4j logger. We can even plug custom logger using logging python module
  3) Consider scaling of your application - This solution is scalable by appropriately configuring the number of executors, executor memory, executor cores depending on the volumn of the data through spark-submit
  4) CI/CD explained - This solution can be easily integrated with Git, Jenkins
  5) Performance tuning explained - 
     1) Using parquet format will enhance the performance of analytical scenarios like aggregations
	 2) Applying data cleansing during preprocessing phase so as to limit the data to access in the aggregator jobs
	