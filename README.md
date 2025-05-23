# Tamer's SQL & Python Data Engineering Portfolio

Welcome to my portfolio of SQL and Python scripts. This repository showcases real-world ETL pipelines, web scraping projects, geo-data processing, and analytics workflows built using PostgreSQL, Redshift, PostGIS, and Python. These scripts reflect hands-on experience solving data challenges at scale, especially in the context of geospatial and business data within Switzerland.

---

## 📚 Table of Contents

- [📁 Repository Structure](#-repository-structure)
- [🛠️ Tools & Technologies Used](#️-tools--technologies-used)
- [🔍 Featured Capabilities](#-featured-capabilities)
- [📌 About Me](#-about-me)
- [📄 Legal & Credits](#-legal--credits)
- [🏷️ Tags](#️-tags)

---

## 📁 Repository Structure

PythonScripts/ → Python ETL scripts (POIs, Geo layers, helpers)
Projects/ → SQL scripts organized by project (Allianz, AZ URLs, Top CC, etc.)
Geo Database/ → SQL scripts for Google Maps POI data, postal code updates, geo-mapping


---

## 🛠️ Tools & Technologies Used

- **Languages**: SQL (PostgreSQL, Redshift), Python (pandas, psycopg2, requests)
- **Databases**: PostgreSQL + PostGIS, Amazon Redshift
- **Data Tools**: DBeaver, QGIS, DataForSEO
- **Infrastructure**: AWS S3, EC2

---

## 🔍 Featured Capabilities

- Collected all POIs in Switzerland using Google Maps and open REST APIs, and organized them into geospatial layers within the database.  
- Updated municipality (gmd) and postal code (plz) layers using geo-spatial joins in PostGIS.  
- Performed data matching, fuzzy logic, and regex-based preprocessing for normalization and deduplication.  
- Designed SQL-based migration pipelines between PostgreSQL and Amazon Redshift.  
- Aggregated and analyzed large-scale geo-business datasets to extract actionable insights.  
- Conducted Standort (location) analysis using HKT100 data and competitor POIs to evaluate project feasibility.  
- Created visual maps to display Standort potential and POI distribution using QGIS and SQL-exported layers.  
- Updated HKT data at (100×100 m) and (500×500 m) grid levels, and deanonymized suppressed resident counts in remote areas using statistical reconstruction and SQL queries. 
- Applied advanced POI filtering and enrichment techniques using Google Maps data and custom web scraping workflows  
 

---

## 📌 About Me

I'm a data engineer specializing in geo-data pipelines, SQL optimization, and end-to-end data architecture. This portfolio showcases my hands-on experience solving real business problems — from collecting and transforming raw data to delivering structured, actionable insights. It also includes trial-and-error work, reflecting the practical, iterative nature of building data solutions at scale.

---

## 📄 Legal & Credits

All scripts in this repository were developed by me during my previous role at **AFO Solutions AG**.

These files contain **no client data** — only generic or anonymized SQL and Python logic. They are shared here for **demonstration and portfolio purposes only**, and not for reuse in production environments.

This repository also includes **trial scripts, test cases, and intermediate attempts** to reflect the real process behind solving complex data challenges — not just the final product.

---

## 🏷️ Tags

`SQL` `PostgreSQL` `Python` `ETL` `Data Engineering` `PostGIS` `GeoData` `AWS Redshift` `QGIS` `DataForSEO` `Web Scraping` `GIS Analytics`
