San Francisco 311 — City Operations Pulse

This project shows how I built a complete data pipeline and dashboard using real 311 service request data from San Francisco. It pulls data daily from the city’s open API, cleans and stores it in a local Postgres database, and visualizes trends through an interactive Streamlit app.
The goal was to learn and show practical data engineering skills,from ETL and database work to dashboard design and storytelling with data.

---

 Overview
The dashboard highlights:
- How many service requests come in daily
- Which neighborhoods generate the most activity
- What categories are most common (like graffiti or street cleaning)
- How resolution times and open rates change over time
- Week-over-week trends for categories and neighborhoods

It’s meant to give a “city operations snapshot” that updates automatically.

---

Tech Stack
- Python, Pandas, SQLAlchemy
- PostgreSQL (via Docker)
- Streamlit for the interactive dashboard
- Socrata API for live data from [data.sfgov.org](https://data.sfgov.org)
- Cron job for daily automated refresh

---

How to Run Locally
1. Clone and install dependencies
```bash
git clone https://github.com/najisibai/baymob-etl.git
cd baymob-etl
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
