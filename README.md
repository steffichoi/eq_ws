Source code for first 3 tasks:
	data-mr/data/cleanup.py

Run Instructions:
	Submitted as a spark job:

	>> spark-submit --master spark:localhost.8080 ../../tmp/data/cleanup.py

Results for each task:

	1. data-mr/data/filtered.csv

	2. data-mr/data/POI_labeled.csv

	3. a)data-mr/data/avg_stddev.csv

	   b)data-mr/data/density.csv


Source code for the last task:
	data-mr/pipeline/scheduler.py

Run Instructions:

	The program reads from questions.txt to get start and end goals

	The program reads from relations.txt to form the edge relationships(builds the hashtable)
	Outputs to results.txt

	>> python3 scheduler.py

Results:
	data-mr/pipeline/results.txt