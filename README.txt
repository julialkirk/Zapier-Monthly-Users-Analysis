Readme.txt file for Zapier User Churn Analysis created 2019-12-06



Step 1: SQL

I downloaded and used DataGrip to connect to Zapier’s Redshift analytical DB. The SQL script does not return the data in the format that I wanted for analysis. I hope the script at least describes my intent! I did not have access to an ETL tool, which turned out to be a bigger frustration than I initially expected.



Step 2: R

Because I was unable to get the data in the format I wanted via SQL, I did not import the data from the jkirkpatrick schema as I intended. Instead I pull the source data directly from zapier.source_data.tasks_used_da table and reshape and reformat in R (which I recognize is much less efficient!). The beginning of my code completes this task.

R version: 3.6.1
Packages used: 
package installation and library loading is accomplished via a function at the top of my code
RPostgreSQL - connecting to the data
tidyverse - dplyr for data manipulation, ggplot for graphics…
padr - padding the data with missing dates (should have been accomplished in SQL)
data.table - data manipulation / calculations (uniqueN)
zoo - time manipulation (rollapply / rollsum)
psych - my favorite univariate stats tool (describe())
survival, survminer, ggfortify, ranger - survival analysis



Step 3: Analysis

Analysis created using google slides, for speed. Most of my current stakeholders prefer PowerPoint presentations of analyses rather than reports. I considered using RMarkdown, but haven’t used it recently, and I haven’t had access to or experience with Lookr. 

I set up my data planning to run a Cox Proportional Hazards model to be able to directly interpret each feature’s effect on churn likelihood, but the model gave unexpected results - the two predictors that should have the same sign had opposite. I would need to investigate further before presenting results, so I pivoted to a random forest to describe which features were most important without coefficient estimates.
