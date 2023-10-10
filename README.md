**youtube_project**
This program is designed to interact with the YouTube Data API, retrieve information YouTube channels, their videos, and comments, and then store this data in MongoDB and MySQL databases. 
**Table of Contents**
Introduction
Features
Prerequisites
Getting Started
Usage
SQL Queries
Examples
License
Acknowledgments
Author
Contributions

**Introduction**
YouTube Data Harvesting and Warehousing is a Python application that enables users to extract data from YouTube channels, store it in MongoDB, and migrate it to a MySQL database for further analysis. This project leverages the YouTube Data API, pymongo for MongoDB interactions, and mysql-connector for MySQL connections, providing an efficient way to retrieve and analyze YouTube channel data.

**Features**
Data Extraction: Retrieve comprehensive channel information, playlists, videos, and comments from YouTube using the YouTube Data API.
MongoDB Storage: Seamlessly store extracted data in MongoDB, a NoSQL database.
MySQL Migration: Transfer data from MongoDB to a structured MySQL database for efficient querying.
SQL Queries: Execute predefined SQL queries to gain valuable insights from the collected data.

**Prerequisites**
Before getting started with this project, make sure you have the following prerequisites:

Python 3.x
Required Python packages can be installed by running pip install -r requirements.txt.
Obtain a YouTube Data API key from the Google Developers Console.

**Getting Started**
Clone this repository to your local machine: git clone https://github.com/your-username/youtube-data-harvesting.git
Install the necessary dependencies: pip install -r requirements.txt
Replace the placeholder API_KEY in the script with your YouTube Data API key.
Configure MongoDB and MySQL connection settings in the script.
Run the application using python YOUTUBE_DATA_HARVESTING.py.

**Usage**
Access the Streamlit web interface to interact with the application.
Extract data by inputting YouTube channel IDs and clicking the "Search" button.
Upload data to MongoDB using the "Upload to MongoDB" button.
Migrate data to MySQL by selecting a channel and clicking "Migrate to MySQL."
Execute SQL queries for data analysis within the Streamlit interface.

**SQL Queries**
The project provides a set of predefined SQL queries to facilitate data analysis. Users can select a question from the list and click "Run Query" to view the results.

**Examples**
Here are some example questions that you can answer using this project:

Question 1: What are the names of all the videos and their corresponding channels?
Question 2: Which channels have the most videos, and how many do they have?
Question 3: What are the top 10 most viewed videos and their respective channels?

**License**
This project is licensed under the MIT License.

**Acknowledgments**
This project utilizes the following libraries and services:

Streamlit - Used to create the user interface.
Google API Client Library - Utilized for interaction with the YouTube Data API.
pymongo - Employed for MongoDB interactions.
mysql-connector-python - Utilized for MySQL connections.
**Author**
Your Name
**Contributions**
Contributions to this project are welcome! Please follow the guidelines outlined in CONTRIBUTING.md to contribute to the development of this project.
