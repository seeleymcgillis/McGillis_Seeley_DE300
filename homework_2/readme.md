Part 1: Relational.

Setup.
Setup code should install duckdb. Then establish a connection to mimic.db. Code to add PRESCRIPTIONS table from PRESCRIPTIONS.csv should be run once. To confirm the right tables are added, code should then show tables.

Question 1.
Code should display PATIENTS and ADMISSIONS tables to investigate columns. 

Then code should create a DRUGS_ETHN table with columns ethnicity, drug, and total_drug which represents the amount of times drug is used by ethnicity. Code should then display this table. 

Code should then display a table with columns ethnicity, the most used drug of that ethnicity, and the amount of times that drug is used by that ethnicity.

Question 2.
Code should display PROCS_ICD and D_ICDPROCS tables to investigate columns. The DOB column from the PATIENTS table was also printed.

Code should then create a table PROC_AGE with columns AGE_GROUP, icd9_code, and TOTAL_PROC which represents the amount of time each procedure was performed on each age group. The column AGE_GROUP was calculated by subtracting the year from DOB from the admissions time of each patient. Code should then display this table.

Code should then create four tables, PROC_AGE_1, PROC_AGE_2, PROC_AGE_3, and PROC_AGE_4. These tables show the procedure name and the top three procedures performed on each age group for the age groups, <=19, 20-49, 50-79, and >80 respectively.

Question 3.
Code should display ICUSTAYS table to investigate the columns.

Code should then display the average length of stay in the ICU for all patients.

Code should then create a table ICU_GENDER with columns gender and AVG_LOS which shows the average length of stay in the ICU for each gender, F and M. Code should display this table.

Code should then create a table ICU_ETHN with columns ethnicity and AVG_LOS which shows the average length of stay in the ICU for each ethnicity. Code should then display this table.

Part 2: Non-Relational.

Setup.
Setup code should install cassandra, ssl setup, boto3 session setup, authorization setup with SigV4, cluster setup, establish connection to Keyspace, create a keyspace for HW2, define execution profile with LOCAL_QUORUM, setup cluster with correct profile, and connect to the seeley_hw2 keyspace. 

Question 1.
Code should create a DRUGS_ETHN table for the session. Code should then create a dataframe, drugs_ethn_df, which contains the information from the table DRUGS_ETHN from part 1, question 1, of this homework. Code should then insert the information from the dataframe into the cassandra table, DRUGS_ETHN. Code should then export this table as the csv file drugs_ethn.csv in order to ensure data is properly exported.

Question 2.
Code should create a PROC_AGE table for the session. Code should then create a dataframe, proc_age_df, which contains the information from the table PROC_AGE from part 1, question 2, of this homework. Code should then insert the information from the dataframe into the cassandra table, PROC_AGE. Code should then export this table as the csv file proc_age.csv in order to ensure data is properly exported.

Code should create a TOP_PROC_AGE table for the session. Code should then create four dataframes, proc_age_df_1, proc_age_df_2, proc_age_df_3, and proc_age_df_4, which correspond to the four tables PROC_AGE_1, PROC_AGE_2, PROC_AGE_3, and PROC_AGE_4, from part 1, question 2 of this homework. Code should then insert the information from these four data frames into the singular cassandra table, TOP_PROC_AGE. Code should then export this table as the csv file proc_age_top.csv in order to ensure data is properly exported.

Question 3.
Code should create an ICU_GENDER table for the session. Code should then create a dataframe, gender_icu_df, which contains the information from the table ICU_GENDER from part 1, question 3, of this homework. Code should then insert the information from the dataframe into the cassandra table, ICU_GENDER. Code should then export this table as the csv file gender_icu.csv to ensure data is properly exported.

Code should then create an ICU_ETHN table for the session. Code should then create a dataframe, ethn_icu_df, which contains the information from the table ICU_ETHN from part 1, question 3, of this homework. Code should then insert the information from the dataframe into the cassandra table, ICU_ETHN. Code should then export this table as the csv file ethnicity_icu.csv to ensure data is properly exported.

