# Data-Engineering-Project

Built an ETL (Extract, Transform, Load) pipeline to process data from the Realtor API, orchestrate and schedule the workflow, store and transform the data, and finally load it into a data warehouse for visualization and reporting. The pipeline leverages various Google Cloud services and tools for efficient data processing and analysis.

![FLOWCHART](https://github.com/user-attachments/assets/42f3c555-0579-4c0b-b11c-fc4b44543b91)


### Data Extraction
Source: Realtor API <br>
Tool/Language: Python <br>
Data is extracted from the Realtor API using Python scripts, which fetch the necessary real estate data for further processing. <br>

### Data Orchestration and Scheduling
Tool: Google Cloud Composer (Apache Airflow) <br>
The extracted data is orchestrated and scheduled using Google Cloud Composer, which utilizes Apache Airflow to manage and automate the workflow. <br>

### Data Storage
Tool: Google Cloud Storage <br>
The orchestrated data is temporarily stored in Google Cloud Storage, providing a staging area for further processing. <br>

### Data Transformation and Loading
Tool: Google Cloud Composer (Apache Airflow) <br>
Data transformation and loading processes are managed by Apache Airflow within Google Cloud Composer, preparing the data for analysis by cleaning, normalizing, and structuring it. <br>

### Data Warehousing
Tool: Google BigQuery <br>
The transformed data is loaded into Google BigQuery, a fully managed data warehouse that allows for scalable and efficient data analysis. <br>

### Data Visualization and Reporting
Tool: Looker Studio <br>
Data from Google BigQuery is visualized and reported using Looker Studio, creating interactive dashboards and reports for end users or BI teams. <br>

![dashboard](https://github.com/user-attachments/assets/fa8f928a-0ac2-45a6-bbc5-ff827e919ec0)

