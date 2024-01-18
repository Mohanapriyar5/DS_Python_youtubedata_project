# DS_Python_youtubedata_project
Develop a Streamlit app for YouTube data analysis, allowing users to fetch and analyze channel details via Google API. Features include MongoDB storage, data collection for 10 channels, migration to SQL, and advanced search options.

LinkedIn : https://www.linkedin.com/in/mohana-priya-455530190/

Problem Statement:
  The problem entails developing a Streamlit application for YouTube data analysis. Users can input a YouTube channel ID to fetch relevant data (channel info, video details, likes, dislikes, comments) using the Google API. The app allows storing this data in MongoDB for multiple channels. Users can then migrate data from the data lake to a SQL database, enabling search and retrieval with various options, including table joins for comprehensive channel details.

REQUIRED LIBRARIES:

1.googleapiclient.discovery

2.streamlit

3.mysql connector

4.pymongo

5.pandas

Components Required:
STREAMLIT: 

	A user-friendly user interface (UI) was developed using the Streamlit library to allow users to interact with the application and perform data retrieval and analysis tasks.
PYTHON:

	Python is a well-known, capable programming language that is simple to pick up and comprehend. The main language used in this project to construct the entire application, including data processing, analysis, visualization, and retrieval, is Python.
GOOGLE API CLIENT: 

	Contacting various Google APIs is made easier using the Python googleapiclient package. Its main goal in this project is to communicate with YouTube's Data API v3, which enables the retrieval of crucial data including comments, video metadata, and channel details. Developers can quickly access and modify YouTube's vast data resources with code by using googleapiclient. 
 MONGODB: 
 
 	Developed on a scale-out architecture, MongoDB has gained popularity among developers of all stripes as a tool for creating scalable systems with dynamic data schemas. MongoDB, being a document database, facilitates the storing of both structured and unstructured data for developers. Documents are stored in a format similar to JSON.
MYSQL:

 	MySQL is an open-source relational database management system (RDBMS) that uses Structured Query Language (SQL). It is widely used for managing and organizing structured data, providing a reliable and efficient way to store, retrieve, and manage databases.
