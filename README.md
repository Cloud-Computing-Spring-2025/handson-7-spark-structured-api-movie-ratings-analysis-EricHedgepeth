# Handson 7 report 

## Overview 



This report looks at user movie-watching habits using Apache Spark. We analyzed binge-watching behavior, churn risk, and trends in movie-watching over the years.



## **Steps**


#### **1. Running Locally**

1. **Checked Python**:
   ```bash
   python3 --version
   ```

2. **Installed PySpark**:
   ```bash
   pip install pyspark
   ```



3. **Check Spark**:
   
   ```bash
   spark-submit --version
   ```


### I completed the TODOs in each task and then ran the following

1. **Task 1**:
   
   ```bash
   spark-submit src/task1_binge_watching_patterns.py
   ```


2. **Task 2**:
   
   ```bash
   spark-submit src/task2_churn_risk_users.py
   ```


3. **Task 3**:
   
   ```bash
   spark-submit src/task3_movie_watching_trends.py
   ```


#### **I verified the output of each file was created and that it matched the requirments**

## Findings 

### Tasks 1: Binge-Watching Patterns

Goal

   Find out how many people in different age groups binge-watch movies.

What I Did

   1. Picked users with IsBingeWatched = True.

   2. Counted how many binge-watchers are in each age group.

   3. Counted total users in each age group.

   4. Calculated what percentage of users binge-watch.


Takeaways 

   1. Key Takeaways

   2. Teens and Seniors binge-watch the most (about 57%).

   3. Adults binge-watch less (41%).

   4. Streaming platforms may want to target Teens and Seniors for binge-worthy content.

## 

### Tasks 2: Churn Risk Users


Goal

   Find users who are likely to stop using their subscriptions.

What I Did

   1. Picked users where SubscriptionStatus = 'Canceled'.

   2. Picked users with WatchTime < 100 minutes.

   3. Counted how many users met both conditions.


Results  

   1. Churn risk users found: 15

## 

### Tasks 3: Movie-Watching Trends Over Time

Goal

   See how movie-watching changed over the years.

What I Did

   1. Counted how many movies were watched each year.

   2. Sorted the results to see the trend.

  

Takeaways 

   1. More movies are watched every year.

   2. Biggest jump was from 2022 to 2023.


