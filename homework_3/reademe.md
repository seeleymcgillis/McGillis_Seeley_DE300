Setup.
Code should download agnews_clean.csv from github. Then code should install pyspark and create a spark session. agnews variable should be created by reading the csv file, and the second column should be converted from a string to an array.

1. tf-idf definition
Imports math functions to the environment. Creates variable documents which is a subset of agnews, with columns _c0 and filtered. Total_documents is the amount of entries in documents. Defines map_phase function which computes the term frequency of each word in each document. Defines shuffle_and_sort function which groups the data by word. Defines reduce_phase function which computes the TF-IDF score for each word in each document.

The tf_idf_matrix is then computed using these defined functions sequentially with documents as the input. agnews_with_tfidf is created which saves the measures in a new column.

The tf-idf measure is printed for the first 5 documents.

2. SVM objective function
Downloads w.csv, bias.csv, and data_for_svm.csv from github. Imports SparkSession and pandas as pd. Initializes spark, loads CSV as spark dataframe, loads model parameters using pandas, and converts dataFrame to RDD with row index. 

The code defines documents as a list of tuples in the form (x_i, y_i). Total_documents is the total number of these documents. Defines map_phase which computes hinge loss for each (x_i, y_i). Defines shuffle_and_sort which groups all mapped outputs by their hinge loss. Defines reduce_phase which computes the average hinge loss, and returns the total loss as used in SVM optimization.

The code then defines the loss_SVM function, which calculates the SVM objective for a given choice of w, b with data stored in X, y. It then prints the predictions for all of the data using the provided weights and bias.

