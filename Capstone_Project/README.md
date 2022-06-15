# CAPSTONE PROJECT

### PROJECT SUMMARY
This project is built to prepare data for source-of-truth database where compile and gather data from multiple sources. Project purpose is to help stack holder can explore the insight of factors which can affect on stay duration of immigration or traveler in specific time and location

The project follows the follow steps:

**Step 1:** Scope the Project and Gather Data

**Step 2:** Explore and Assess the Data

**Step 3:** Define the Data Model

**Step 4:** Run ETL to Model the Data

**Step 5:** Complete Project Write 


### STEP 1: SCOPE THE PROJECT AND GATHER DATA

**Scope**

This project will use two dataset I94 immigration data and US demographic data to create a data warehouse which include dimensional models.

**Data Sets**

- I94 Immigration Data
- U.S. City Demographic Data

**Tools and libraries:**

- *AWS S3*: data storage
- *Python libraries:*
    - *Pandas*  - using to explore and execute on the small dataset
    - *PySpark* - using to explore and execute on the large dataset

#### Describe the dataset 
| Dataset             |Format |Description|
|---------------------|-------|-----------|
|I94 Immigration Data |SAS    |Data gather the information of visitors likes gender, age, visa, etc.|
|U.S. City Demographic Data|CSV|This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000.|


### STEP 2: EXPLORE AND ASSESS DATA

Please refer to **Capstone_Project.ipynb** for detail

### STEP 3: DEFINE THE DATA MODEL
#### **3.1 Conceptual Data Model**

![schema](images/schema.png)

**dim_personal**: The dimension table about personal information likes gender, year of birth, country of born and living. It help to specify and give more information about each immigrant or traveler. 

**dim_arrival_date**: The date which immigrants or travelers arrive US. Table will contain the separate time units like year, month, day to help stack holders query more easier.

**dim_visa**: The dimension table which contain visa type and visa purpose.

**dim_location**: The dimension table about location which immigrant arrives. It provide more information about demographics of the place where immigrants or travelers want to visit.

**dim_airline**: The dimension table has information about the airline brand and flight number which immigrants or travelers chose. 

**fact_immigration**: The fact table which has information columns to link to the dimension tables and show the stay duration of the immigrant or traveler. It will help stack holder can research and explore which elements will attract the immigrants and travelers. 


#### **3.2 Mapping Out Data Pipelines**

1. Loading datasets from S3 bucket
2. From I94 Immigration dataset we create fact_table, dim_personal, dim_arrival_date, dim_visa, dim_airline
3. From Demography dataset we create the dim_location 
4. Which cleaning steps in step 2, we modify fact_table and dim_location, drop duplicate and drop Nan in each Primary Key of tables
5. Put back the fact and dimension table into S3 bucket

### STEP 4: RUN PIPELINES TO MODEL THE DATA
#### **4.1 Create the data model**

Please refer to **etl.py** for detail

#### **4.2 Data Quality Checks**

Please refer to quality_check.py for detail

#### **4.3 Data dictionary**

**Fact table: fact_immigration**

|**Attributes**|**Description**|
|:--------------|:---------------|
|cic_id|record ID
|city_code|city code of admission
|city_name|city name of admission
|state_code|state code of arrival
|arrival_date|date of arrival
|flight_number|flight number of Airline used to arrive in U.S.
|stay_duration|stay duration of immigration


**Dimension table: dim_arrival_date**

|**Attributes**|**Description**|
|:--------------|:---------------|
|arrival_date|arrival date in SAS format
|arrival_datetime|arrival date in datetime format
|year|year of arrival
|month|month of arrival
|day|day of arrival

**Dimension table: dim_visa**
    
|**Attributes**|**Description**|
|:--------------|:---------------|
|visa_type|type of visa
|visa_purpose|common purpose of visa

**Dimension table: dim_personal**

|**Attributes**|**Description**|
|:--------------|:---------------|
|cic_id|record ID
|country_of_birth|immigrant country of birth
|country_of_residence|immigrant country of residence
|year_of_birth|Year of birth
|gender|immigrant sex

**Dimension table: dim_airline**

|**Attributes**|**Description**|
|:--------------|:---------------|
|flight_number|Flight number of Airline used to arrive in U.S.
|airline_brand|Airline used to arrive in U.S.


**Dimension table: dim_location**

|**Attributes**|**Description**|
|:--------------|:---------------|
|city_name|name of city
|state_name|name of state
|state_code|code of state
|median_age|median age of the location
|male_population|quantity of male
|female_population|quantity of female
|total_population|total quatity population
|number_of_veterans|quantity of veterans


#### STEP 5: COMPLETE PROJECT WRITE UP

#### Tools and technologie
- AWS S3 storage to store the output of pipeline
- Pandas library for exploration the small sample data
- Spark use to create pipeline and deal with large number of data

#### Data update duration:

- *Immigration data*: dataset is partition by month so the pipeline should be scheduled monthly to process catching up the updating of source data
- *Demographic data*: This dataset seem updated annually because of some attribute must need time and high cost to estimate and process statistic 

#### Solution for scenarios:

- *The data was increased by 100x:*

    For the large data, we need to use EMR to create a cluster have enough power of computing and with elastic scale feature it can easily handle bigdata


- *The data populates a dashboard that must be updated on a daily basis by 7am every day:*
    
    With the demand of schedule processing, we can apply Airflow to create dags and process exactly and automatically. 
    
- *The data needed to be accessed by 100+ people*
    
    With high connection, we should move the data storage from s3 to Redshift. Redshift allows 500 connection so we can handle the problem well.
    
    
    