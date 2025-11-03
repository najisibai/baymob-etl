## San Francisco 311 — City Operations Pulse

A Streamlit dashboard that tracks and visualizes San Francisco’s 311 service requests, ie: what people report, how fast it’s handled, and which neighborhoods see the most activity. Built on top of my ETL pipeline that pulls, cleans, and loads daily 311 data into Postgres.
The goal was is to show practical data engineering skills

---

## Overview
The dashboard gives a live snapshot of city operations:
	- Total requests, open rate, and median resolution time
	- Daily request trends
	- Top neighborhoods and categories
	- Week-over-week changes to spot spikes or drops in activity

All data is cached for quick access, and can be refreshed or exported anytime.

---

## Notable Features
 1.	Interactive filters: Choose any date range or category to explore
	2.	One-click refresh: Instantly pull the latest data from the database
	3.	CSV export: Download your filtered results as a clean .csv file
	4.	Week-to-week trends: Quickly spot which neighborhoods or request types changed the most

---

## Tech Stack Used:
- Python (Streamlit, Pandas, SQLAlchemy)
- PostgreSQL (ETL data store)
- Docker for local dev setup

---

## Dashboard Preview

**Overview + KPIs**  
<img width="1470" height="362" alt="kpis" src="https://github.com/user-attachments/assets/155004b0-9ac1-4340-b9e6-5b1e05b959f7" />

**Week-over-Week Insights**  
<img width="1470" height="404" alt="notable_changes" src="https://github.com/user-attachments/assets/1c334b62-7f9e-4af7-a511-33fa11565e21" />

**Trends & Top Neighborhoods**  
<img width="1470" height="670" alt="trends_and_neighborhoods" src="https://github.com/user-attachments/assets/d0dbf4f4-c055-4c11-801c-d17261c5356e" />

**Filters, Refresh, & CSV Download**  
<img width="1470" height="340" alt="Screenshot 2025-11-03 at 6 10 23 PM" src="https://github.com/user-attachments/assets/720f9aef-0b0a-4854-a681-1b9015e3e5dc" />




