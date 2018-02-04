
# Data Exploration and ML Modeling using Spark on HDInsight cluster

------------
### Overview

This notebook helps you to get started with using Spark HDInsight clusters for data science and machine learning (ML). The notebook provides the links to <a href="https://azure.microsoft.com/en-us/documentation/articles/machine-learning-data-science-spark-overview/" target="_blank">Data Science Process on Spark</a>, a suite of topics and pySpark notebooks on a <a href="https://github.com/Azure/Azure-MachineLearning-DataScience/tree/master/Misc/Spark/pySpark" target="_blank">public GitHub repository</a> that show how to use HDInsight Spark and the <a href="http://spark.apache.org/docs/latest/mllib-guide.html" target="_blank">MLlib API</a> to conduct common data science and machine learning (ML) tasks, such as:

1. Data ingestion

2. Data exploration and visualization

3. Feature engineering

4. Creating ML models and evaluating those models

5. Saving ML models and consuming ML models


<b>The datasets and ML problems:</b>
1. The <a href="http://www.andresmh.com/nyctaxitrips/" target="_blank">2013 NYC taxi trip and fare dataset</a> for regression problem. The ML task performed with his dataset was to predict the amount of tip paid for taxi trips in NY City in 2013, based on features such as the pickup time, number of passengers in the trip, the mode of payment (cash, credit etc.)
2. The <a href="https://www.transtats.bts.gov/ONTIME/" target="_blank">2011 and 2012 airline on-time departure and arrival dataset</a> for classification problem. The ML task performed with this dataset was to predict if flight departures were delayed by 15 mins or more, based on flight and airline information, as well as weather data at the departure and destination airports.


The models built include logistic and linear regression, random forests and gradient boosted trees. The topics also show how to store these models in Azure blob storage (WASB) and how to score and evaluate their predictive performance. We cover how models can be trained using cross-validation and hyper-parameter sweeping. Relevant plots were generated using Python's matplotlib functions.

----------
### Public GitHub repository for pySpark data-science and ML notebooks:

A <a href="https://github.com/Azure/Azure-MachineLearning-DataScience/tree/master/Misc/Spark/pySpark" target="_blank">Public GitHub repository</a> is availble for pySpark Notebooks that can be loaded directly to Spark HDI cluster and run immediately.

The notebooks correspond to the topics mentioned above.


<a href="https://azure.microsoft.com/en-us/documentation/articles/machine-learning-data-science-spark-overview/#execute-code-from-a-jupyter-notebook-on-the-spark-cluster" target="_blank">Instructions</a> for loading the notebooks are provided in the Overview topic (see section: "Execute code from a Jupyter notebook on the Spark cluster"). Note that you will have to load the "raw" version of a notebook from GitHub, which you can access by clicking "Raw" near the top right of the notebooks.
