Setup.

Code should download agnews_clean.csv from github. Then code should install pyspark and create a spark session. agnews variable should be created by reading the csv file, and the second column should be converted from a string to an array.

1. tf-idf definition

Imports math functions to the environment. Creates variable documents which is a subset of agnews, with columns _c0 and filtered. Total_documents is the amount of entries in documents. Defines map_phase function which computes the term frequency of each word in each document. Defines shuffle_and_sort function which groups the data by word. Defines reduce_phase function which computes the TF-IDF score for each word in each document.

The tf_idf_matrix is then computed using these defined functions sequentially with documents as the input. agnews_with_tfidf is created which saves the measures in a new column.

The tf-idf measure is printed for the first 5 documents. The output should look as follows:

Document: 0
  cynics: 0.5637
  wall: 0.5116
  claw: 0.4991
  dwindling: 0.4572
  sellers: 0.4468

Document: 9
  cynics: 0.5341
  wall: 0.4847
  claw: 0.4728
  dwindling: 0.4332
  sellers: 0.4233

Document: 215
  auction: 0.3942
  overstated: 0.2995
  face: 0.2839
  aspects: 0.2785
  playboy: 0.2724

Document: 806
  lowe: 0.4704
  earnings: 0.2776
  higher: 0.2735
  2q: 0.2506
  home: 0.2436

Document: 821
  changed: 0.4158
  little: 0.3211
  wall: 0.3175
  street: 0.3064
  liabilities: 0.3004

2. SVM objective function

Downloads w.csv, bias.csv, and data_for_svm.csv from github. Imports SparkSession and pandas as pd. Initializes spark, loads CSV as spark dataframe, loads model parameters using pandas, and converts dataFrame to RDD with row index. 

The code defines documents as a list of tuples in the form (x_i, y_i). Total_documents is the total number of these documents. Defines map_phase which computes hinge loss for each (x_i, y_i). Defines shuffle_and_sort which groups all mapped outputs by their hinge loss. Defines reduce_phase which computes the average hinge loss, and returns the total loss as used in SVM optimization.

The code then defines the loss_SVM function, def loss_SVM(w, bias, X, y, λ):,which calculates the SVM objective for a given choice of w, b, lambda with data stored in X, y. Using the weights and bias provided in w.csv and bias.csv, the objective value is calculated and printed. With a chosen lambda value of 0.01, the output should look like this:

SVM Objective Value: 0.9999963377570842

The code creates the MapReduce function required to make predictions. It then prints the predictions for all of the data using the provided weights and bias. This output is very long, but the beginning of this output should look as follows:

Predictions: [1, -1, -1, -1, -1, -1, -1, 1, -1, -1, 1, 1, -1, -1, 1, -1, -1, 1, -1, -1, -1 ...

