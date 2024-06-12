# Big Data Exploration with Cassandra 🍿📊

This project is part of the Computer Engineering and Informatics Department (CEID) of University of Patras Big Data course. It explores the MovieLens 20M dataset using Cassandra, a NoSQL database designed for handling big data. It demonstrates how to design, implement, and query a Cassandra database to efficiently manage and analyze large movie-related datasets.

## Project Overview

The project focuses on the following key areas:

1. **Database Design**: A Cassandra database schema is designed to support various movie-related queries, such as finding popular movies, searching by title or genre, and retrieving movie details.
2. **Data Ingestion**: Python scripts are used to load the MovieLens 20M dataset (tags, movies, ratings, genome tags) into the Cassandra database.
3. **Query Execution**: Python scripts are developed to execute a variety of queries against the Cassandra database, including retrieving top-rated movies, finding movies by genre, and more.
4. **Performance Evaluation**: The impact of different consistency levels (ALL, QUORUM, ONE) on write and read performance is evaluated using the DataStax AstraDB cluster.

## Dataset

The MovieLens 20M dataset contains information about movies, tags, ratings, and genome scores. It's a valuable resource for exploring user preferences, movie recommendations, and other data-driven insights.

**Source**: [MovieLens 20M Dataset on Kaggle](https://www.kaggle.com/grouplens/movielens-20m-dataset)

## Tools and Technologies

- **Cassandra**: NoSQL database for managing big data.
- **DataStax AstraDB**: Cloud-based Cassandra instance for performance evaluation.
- **Python**: Programming language used for data loading, querying, and analysis.

## Repository Contents

1. **Schema Design**: Conceptual model, application workflow diagram, and Chebotko diagram.
2. **DDL Statements**: Cassandra Data Definition Language (DDL) statements to create keyspaces and tables.
3. **Python Scripts**:
    - Data loading scripts for each dataset file.
    - Query execution scripts for various movie-related queries.
    - Graphs for comparing consistency levels.
4. **Results**: Analysis and graphs comparing the performance of different consistency levels.
