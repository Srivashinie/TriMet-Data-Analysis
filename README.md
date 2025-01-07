# TriMet Bus Data Analysis

The TriMet Bus Data Analysis System is designed to process and analyze transit data from TriMet, Portland's regional transit authority. TriMet operates buses, MAX Light Rail, and WES Commuter Rail. This project focuses on breadcrumb data (GPS sensor readings) and stop event data to provide insights into bus movements and schedules.

**Key Features**

- Data Pipeline: Automates the collection, validation, and storage of large-scale transit data.
- Real-Time Processing: Ensures seamless data flow using GCP services like Pub/Sub and PostgreSQL.
- Interactive Visualizations: Leverages Mapbox and Folium for geographical data representation.
- Scalable Design: Handles over 2 million GPS readings daily.

**System Architecture**

![GCP horizontal framework](https://github.com/chethana613/TriMet/assets/56347342/4852cfc1-b22f-4f96-9fdd-eac9654d90b6)
The system architecture comprises several coordinated components:
- Data Source: TriMet APIs for GPS sensor (breadcrumb) and stop event data.
- Data Collection: Automated using cron jobs running on GCP Virtual Machines (VMs).
- Messaging System: Google Cloud Pub/Sub for publishing and subscribing to breadcrumb and stop event data.
- Data Validation and Transformation: Performed by subscribers. Includes: Interpolating missing values, Dropping unnecessary columns, Calculating vehicle speeds and Database Server
- Data stored in PostgreSQL for analysis.
- Storage and Archiving Data archived in Google Cloud Storage buckets.
- Visualization: Built with Mapbox and Folium to generate interactive maps.

**Results**

The system processes large-scale transit data, providing actionable insights for route optimization and delay analysis.
- Daily Data Volume: ~2 million records.
- Visualizations: Highlight bottlenecks and efficiency patterns.
