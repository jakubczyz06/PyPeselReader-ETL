#PyPeselReader-ETL 🚀

A professional Python-based **ETL (Extract, Transform, Load) pipeline** designed for batch processing, validation, and enrichment of Polish PESEL numbers. This project demonstrates industrial Data Engineering practices, clean Object-Oriented Programming (OOP), and robust error handling.



##🌟 Key Features
- **ETL Architecture:** Fully automated data pipeline: **Extract** from ZIP, **Transform** via weighted validation, and **Load** to SQLite.
- **Batch CSV Processing:** Automatically scans and processes multiple `.csv` files within ZIP archives using `csv.DictReader` for header-mapped data extraction.
- **Data Quality Tracking:** Implements a dual-storage strategy:
    - **Success Table:** Validated records with enriched date segments (Day, Month Name, Year).
    - **Rejection Table:** Automatically captures "dirty" data with specific error reasons for audit purposes.
- **System Observability:** Integrated professional logging system (`pesel.log`) that tracks every step of the pipeline and system-level events.
- **Encoding Awareness:** Optimized for Polish environments with `windows-1250` support to handle special characters in names and data.
- **Robust Validation:** Implements official weighted checksum algorithms and century-aware birth date decoding (1800-2299).



##🛠 Technologies
- **Python 3.x**
- **SQLite3** (Relational storage)
- **CSV & Logging** (Standard libraries for data & monitoring)
- **Zipfile & IO** (Buffer-based archive processing)
- **Datetime** (Temporal data enrichment)



##⚙️ Usage
1. Prepare a `.zip` archive containing `.csv` files. 
2. Ensure each CSV file has a header named `pesel`.
3. Run the script:
   ```bash
   python PeselReader.py



##📈 Future Roadmap (Data Engineering Path)

[ ] Containerize the application using Docker.

[ ] Transition to PostgreSQL for scalable storage.



Developed as part of an Object-Oriented Programming course. Focused on building production-ready data pipelines.
