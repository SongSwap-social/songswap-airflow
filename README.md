
<div align="center">
<h1 align="center">
<img src="https://raw.githubusercontent.com/PKief/vscode-material-icon-theme/ec559a9f6bfd399b82bb44393651661b08aaf7ba/icons/folder-markdown-open.svg" width="100" />
<br>songswap-airflow
</h1>
<h3>Developed with the software and tools listed below</h3>

<p align="center">
<img src="https://img.shields.io/badge/Docker-3.8-2496ED.svg?style&logo=Docker&logoColor=white" alt="Docker" />
<img src="https://img.shields.io/badge/Apache_Airflow-2.6.1-017CEE.svg?style&logo=apacheairflow&logoColor=white" alt="Apache Airflow" />
<img src="https://img.shields.io/badge/Python-3.10.12-3776AB.svg?style&logo=Python&logoColor=white" alt="Python" />
</p>
<p align="center">
<img src="https://img.shields.io/badge/Amazon RDS-527FFF.svg?style&logo=amazonrds&logoColor=white" alt="Amazon RDS" />
<img src="https://img.shields.io/badge/Amazon S3-569A31.svg?style&logo=amazons3&logoColor=white" alt="Amazon S3" />
<img src="https://img.shields.io/badge/Amazon EC2-FF9900.svg?style&logo=amazonec2&logoColor=white" alt="Amazon EC2" />
<img src="https://img.shields.io/badge/Amazon CloudWatch-FF4F8B.svg?style&logo=amazoncloudwatch&logoColor=white" alt="Amazon CloudWatch" />
</p>
<p align="center">
<img src="https://img.shields.io/badge/PostgreSQL-4169E1.svg?style&logo=postgresql&logoColor=white" alt="postgresql" />
<img src="https://img.shields.io/badge/Redis-DC382D.svg?style&logo=Redis&logoColor=white" alt="Redis" />
<img src="https://img.shields.io/badge/Spotify-1DB954.svg?style&logo=spotify&logoColor=white" alt="Spotify" />
<img src="https://img.shields.io/badge/Discord-5865F2.svg?style&logo=discord&logoColor=white" alt="Discord" />
</p>
</div>

---

## 📒 Table of Contents
- [📒 Table of Contents](#-table-of-contents)
- [📍 Overview](#-overview)
- [⚙️ Features](#-features)
- [📂 Project Structure](#project-structure)
- [🧩 Modules](#modules)
- [🚀 Getting Started](#-getting-started)
- [🗺 Roadmap](#-roadmap)
- [🤝 Contributing](#-contributing)
- [📄 License](#-license)
- [👏 Acknowledgments](#-acknowledgments)

---


## 📍 Overview

The repository primarily focuses on the orchestration and automation of data ingestion, processing, and management tasks related to the SongSwap-social platform. It uses Apache Airflow as the workflow management system to periodically backfill users' listening history data from the Spotify API, verifying system connections to external services (Postgres, S3, RDS, Spotify, Discord, etc.), and transforming and loading data into a relational database. This project enables scalable and reliable orchestration of data pipelines, ensuring the availability and accuracy of critical data for the SongSwap-social platform.

---

## ⚙️ Features

| Feature                | Description                           |
| ---------------------- | ------------------------------------- |
| **⚙️ Architecture**     | The codebase follows the DAG (Directed Acyclic Graph) architecture, using Apache Airflow as the workflow management tool. It leverages Airflow's available modules for third-party services, and hand-built utilities to perform ETL processes, test connections and webhooks, handle file operations, and interact with external systems such as Postgres, S3, Discord, and Spotify. |
| **📖 Documentation**   | The codebase includes in-line comments and well-structured file names that offer some guidance on the functionality of each file. However, there is a lack of high-level documentation and detailed explanations regarding the overall design and usage of the project.                             |
| **🔗 Dependencies**    | The codebase depends on several external dependencies, such as Apache Airflow, Amazon, Discord, and Postgres plugins, as well as packages specified in the requirements.txt file. These dependencies provide the necessary functionality for interacting with various services and systems. |
| **🧩 Modularity**      | The codebase demonstrates a level of modularity with separate utility modules for handling alerting operations, transformation and validation of listening history data, ETL relational database operations, Spotify API interactions, S3 file operations, and general file operations. This modular organization allows for easier maintenance, testing, and extensibility of the system. |
| **✔️ Testing**          | Tests are written as Airflow DAGs. They use a small subset of existing data and are ran both daily and before/after any changes made to existing ETL-related DAGs to ensure DAGs are not broken. The goal of these tests is to: 1) execute test in conditions as close to production as possible and 2) observe and monitor both test and production runs with the same lens (the Airflow WebUI). |
| **🔐 Security**        | This Airflow instance is intended to run on an isolated server or network. Access should only be granted to SongSwap-social administrators or engineers. It is recommended to lock down the `docker-compose.yaml` file will fewer default values prior to deploying this project to production. |
| **🔌 Integrations**    | The system integrates various systems and services such as Amazon S3, Discord, Postgres, and Spotify. It leverages libraries and plugins for seamless integration with these external systems, enabling data retrieval, validation, transformation, storage, and webhook notifications as part of its functionality. |
| **📶 Scalability**     | The project requires minimal compute in its current state. However, Airflow utilizes job queues with the help of Redis and Celery to allow for parallel DAG execution. In the future, when the data processing's compute requirements increase, the projcet is expected to utilize PySpark in a local, distributed compute environment.   |

---


## 📂 Project Structure


```bash
repo
├── Dockerfile
├── dags
│   ├── __init__.py
│   ├── backfill_data.py
│   ├── load_listening_history.py
│   ├── sanity
│   │   ├── test_discord.py
│   │   ├── test_postgres.py
│   │   ├── test_s3.py
│   │   └── test_spotipy.py
│   ├── src
│   │   ├── __init__.py
│   │   └── utils
│   │       ├── __init__.py
│   │       ├── artist_utils.py
│   │       ├── discord_utils.py
│   │       ├── file_utils.py
│   │       ├── history_utils.py
│   │       ├── rds_utils.py
│   │       ├── s3_utils.py
│   │       ├── spotify_utils.py
│   │       └── track_utils.py
│   └── tests
│       ├── data
│       │   ├── tracks_spotify.json
│       │   └── user_spotify.json
│       ├── test_discord_failure_notifications.py
│       ├── test_file_utils.py
│       ├── test_rds_load_artist.py
│       ├── test_rds_load_history.py
│       ├── test_rds_load_track.py
│       ├── test_rds_load_track_features.py
│       └── test_s3_user_load_history.py
├── docker-compose.yaml
└── requirements.txt

6 directories, 29 files
```

---

## 🧩 Modules

<details closed><summary>Root</summary>

| File| Summary|
| ---| ---|
| Dockerfile | Pulls and extends the Apache Airflow image, installs specific dependencies including Amazon, Discord, and Postgres plugins, and installs additional packages specified in the requirements.txt file. |
| docker-compose.yaml | Defines the services that make up the Airflow instance. It also defines the environment variables for the Airflow instance, including the Postgres connection string, Spotify client ID and secret, and Discord webhook endpoint. |

</details>

<details closed><summary>Dags</summary>

| File| Summary|
| ---| ---|
| load_listening_history.py | Every hour, this Airflow DAG performs a full load of all users' listening history data. It retrieves the user's listening history from the Spotify API, loads the raw response to an S3 bucket, transforms the data to fit the database schema, and then loads it into the database. After loading the history to the database, the DAG retrieves additional data from the Spotify API for all unique tracks and artists: track metadata, artist metadata, and track features (audio analysis). Upon loading failure, the data is saved locally and loading is retried during the following execution. |
| backfill_data.py          | This Airflow DAG performs a backfilling process for various data related to tracks, artists, and track features. It retrieves IDs that are not in the database tables, makes requests to the Spotify API to gather the data, transforms the data to fit the database schema, and then loads it into the database. The process is performed separately for each type of data (artist, track, track features). |

</details>

<details closed><summary>Sanity tests</summary>

| File| Summary|
| ---| ---|
| test_discord.py  | Verifies if the Discord provider is installed and if the Discord connection or webhook endpoint variable is set. It uses PythonOperator to execute two functions that check for the presence of the provider and verify the connection or variable.|
| test_s3.py       | Verifies the existence of a variable called "bucket_name" in Airflow. It then performs a test upload, read, and deletion operation on an S3 bucket using the provided bucket_name. The DAG runs daily and handles dependencies between the tasks. |
| test_spotipy.py  | Verifies if the library `spotipy` is installed and if the required environment variables are set. It uses `PythonOperators` to execute the verification tasks.|
| test_postgres.py | Creates a DAG in Airflow that tests the connection to a Postgres database. It uses the PostgresHook to establish a connection and executes a simple query to check if the connection is working.|

</details>

<details closed><summary>Utils</summary>

| File             | Summary                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ---              | ---                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| discord_utils.py | Defines a custom `DiscordWebhookHook` that allows for sending embeds in a Discord webhook message. It also includes a function to convert UTC time to PST, and a function to create a Discord embed object for a failed task. Additionally, there is a function that can be used as a callback to send a Discord notification when a task or DAG fails.                                                                                                                    |
| history_utils.py | Functions for transforming, validating, and inserting listening history data into a PostgreSQL database. It also includes functions for verifying the inserted data.                                                                                                                                                                                                                                                                            |
| rds_utils.py     | Functions for transforming and loading listening history data to a relational database. It includes functions for generating bulk insert queries, fetching query results in chunks to handle large datasets, and executing SQL queries with data. The main function, insert_bulk, uses these utility functions to insert multiple rows of transformed data into the database in a single transaction.                                 |
| spotify_utils.py | Functions for fetching, parsing, and saving Spotify data to a PostgreSQL database. It provides functions for fetching data from the Spotify API, transforming raw Spotify data, validating assumptions about data order, and inserting data into the database. The code also includes validation functions for verifying the keys and values of the transformed data dictionary. |
| artist_utils.py  | Functions for fetching, parsing, and saving artist data to a PostgreSQL database. It provides functions for transforming raw Spotify data, extracting artist and date information from a listening history, fetching artist data from the Spotify API, validating assumptions about data order, and inserting data into the database. The code also includes validation functions for verifying the keys and values of the transformed data dictionary. |
| s3_utils.py      | Functions for uploading JSON data to an S3 bucket, with the option to convert a dictionary to a JSON string. It also has a function for generating the object name for an S3 object based on user ID and timestamp.                                                                                                                                                                                                                                        |
| track_utils.py   | Functions for fetching, parsing, and saving track data to a PostgreSQL database. It provides functions for transforming raw Spotify data, extracting track and date information from a listening history, fetching track data from the Spotify API, validating assumptions about data order, and inserting data into the database. The code also includes validation functions for verifying the keys and values of the transformed data dictionary.           |
| file_utils.py    | Functions for reading, saving, and deleting JSON and general files. It handles file reading and writing, JSON encoding and decoding, and directory creation if necessary.                                                                                                                                                                                                                                                                                               |

</details>

---

## 🚀 Getting Started

### ✔️ Prerequisites

Before you begin, ensure that you have the following prerequisites installed:
> - `ℹ️ Docker`

### 📦 Installation

1. Clone the songswap-airflow repository:
```sh
git clone git@github.com:SongSwap-social/songswap-airflow.git
```

2. Change to the project directory:
```sh
cd songswap-airflow
```

### 🎮 Using songswap-airflow

```sh
docker compose up --profile flower --build -d
```

### 🧪 Running Tests

Filter Airflow DAGs to include only the `test` tag. Then, run the test DAGs.

---


## 🗺 Roadmap

> - [X] `ℹ️  Implement tests for ETL dags`
> - [X] `ℹ️  Implement tests for utility modules`
> - [X] `ℹ️  Create alerts for failed tasks`
> - [ ] `ℹ️  Write high-level documentation`
> - [ ] `ℹ️  Fix Spotify's token caching bug, causing 401 errors`
> - [ ] `ℹ️  Implement PySpark for distributed data processing`


---
