# Netflix-Data-Analytics-Platform

## **Overview**
This project focuses on building a relational database using **SQL Server** to store and analyze Netflix show data. The database is populated from a structured CSV file containing various attributes of Netflix shows, such as type (movie/TV show), cast, directors, country, genre, and other metadata. A Python-based ETL pipeline is implemented to insert and manage data efficiently while ensuring data integrity.

Additionally, an **interactive Power BI dashboard** was created to analyze key metrics related to Netflix shows, enabling insightful data visualization.

## **Goals and Objectives**
- **Design a robust database schema** optimized for relational data storage.
- **Extract and transform data** from a CSV file into a structured format.
- **Ensure data integrity** by preventing duplicate records and maintaining relationships between entities.
- **Leverage SQLAlchemy** to interact with SQL Server using Python.
- **Normalize text data** to handle inconsistencies (e.g., removing accents, ensuring case-insensitivity).
- **Implement a Power BI dashboard** for analysis and visualization of Netflix show trends.

## **Key Features**
### **Database Design & ERD**
- A detailed **Entity-Relationship Diagram (ERD)** was created to represent the schema, ensuring optimal structuring of relationships between shows, directors, cast members, countries, and genres.
- **Normalization techniques** were applied to maintain data consistency and eliminate redundancy.

### **Database Schema**
The database consists of the following tables:
- **Show**: Stores main details about each show.
- **Director**: Contains director names.
- **Cast**: Stores cast members.
- **Country**: Lists countries where shows are available.
- **Genre**: Stores genres associated with shows.
- **Show_Metadata**: Holds additional metadata, such as date added and rating.
- **Join Tables**:
  - **Show_Director**: Links shows to directors.
  - **Show_Cast**: Links shows to cast members.
  - **Show_Country**: Links shows to countries.
  - **Show_Genre**: Links shows to genres.

### **ETL Pipeline Implementation**
The Python-based ETL pipeline follows these steps:
- **Data Extraction**: Reads raw data from the CSV file.
- **Data Transformation**:
  - Cleans and normalizes textual data.
  - Handles missing values and inconsistencies.
- **Data Loading**:
  - Inserts records while ensuring referential integrity.
  - Uses unique constraints to avoid duplicates.
  - Implements error handling for failed insertions.

### **Power BI Dashboard**
An **interactive Power BI dashboard** was developed to analyze various Netflix show metrics, including:
- Show distribution by country.
- Top genres and trends over time.
- Analysis of movie vs. TV show proportions.
- Ratings and metadata insights.

## **Project Workflow**
1. **Data Processing**: Read, clean, and transform data from the CSV file.
2. **Database Interaction**: Insert data while maintaining relationships.
3. **Integrity Handling**: Validate and prevent duplicate insertions.
4. **Error Handling**: Skip problematic records and log errors.
5. **Power BI Analysis**: Load processed data into Power BI for insights.

## **Conclusion & Future Enhancements**
- This project demonstrates the integration of **SQL Server, Python (SQLAlchemy), and Power BI** for effective database management and analysis.
- The approach ensures **data integrity, normalization, and performance optimization** for large datasets.
- **Future Enhancements**:
  - Automate data updates using **Apache Airflow**.
  - Implement **data validation rules** to improve data quality.
  - Expand the **Power BI dashboard** with additional KPIs and visualizations.

