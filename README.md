# stat480_project

Order to run .py files:
1. construct_df.py
2. Construct_POI.py
3. pre-possessing_model_df_construct.py
4. eda.py
5. model.py

There are two places we used Pandas instead of Spark:

In some of the data processing steps, pyspark was broken after running a long time

When building a screen plot to choose the optimal K for K-Means Cluster, we cannot find a package in Spark to make this plot, so we use sklearn.
