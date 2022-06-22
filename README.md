# Data Warehouse and Data Lake Systems Design
**Data Warehouse and Data Lake Systems Design - An Application for Searching Tourism Attractions in Switzerland**

The project aims to build a travel data warehouse and develop an insight dashboard that provides information about the most popular attractions and destinations in Switzerland. 

## Database Architecture
The target of this database is to build a User Interface for decision-making of travelers and understand how the impact of social media and influencers is. 

Below figure shows the architecture of database architecture. Data sources consist of 3 API and 2 datassheets. Except static data such as Switzerland city, canton, destination, information of geographical locations, dynamic data are fetched via Instagram Graph API and pytrends API. The data lake is built in Amazon Relational Database Service (RDS). Apache Airflow is utilized to execute data pipeline processes automatically. After that, all needed datasets are stored in the data lake. 

<img src="https://user-images.githubusercontent.com/80690817/172610432-5ccb3195-a42b-44b5-966e-b5393d38cf96.png" data-canonical-src="https://user-images.githubusercontent.com/80690817/172610432-5ccb3195-a42b-44b5-966e-b5393d38cf96.png" width="80%" height="80%" />


## Traveler-insight Dashboard
The results of data analysis are visualized and presented as follows. 

<img src="https://user-images.githubusercontent.com/80690817/172610486-f657e544-4a8e-409f-b30f-0ac91263fda3.png" data-canonical-src="https://user-images.githubusercontent.com/80690817/172610486-f657e544-4a8e-409f-b30f-0ac91263fda3.png" width="70%" height="70%" />

The interactive dashboard can be accessed from the URL below.
https://public.tableau.com/app/profile/yang7231/viz/TravelerInsightDashboard/TravelerInsightDashboard


## Description of Files
trends_all.py : airflow dags files fetch the google trends of all cities in Switzerland and
the trends of all hashtags from Instagram.

upload_city_list_to RDS_PostgreSQL.py : python script uploads the datasheet to AWS RDS.
