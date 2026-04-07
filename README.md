# PeselReader-Python-sqlite 🚀

A professional Python-based **ETL (Extract, Transform, Load) pipeline** designed for batch processing and validation of Polish PESEL numbers. This project demonstrates clean Object-Oriented Programming (OOP) principles and structured data storage.

## 🌟 Key Features
- **ETL Architecture:** Fully implemented data pipeline (Extract from ZIP, Transform via Validator, Load to SQLite).
- **Batch Processing:** Automatically scans and processes all `.txt` files within ZIP archives.
- **Robust Validation:** Implements official weighted checksum algorithms to ensure data integrity.
- **Data Decoding:** Extracts birth date (handling 18th-22nd centuries) and gender.
- **Persistent Storage:** Saves validated records into a local SQLite database with duplicate prevention.
- **Clean Code:** Fully documented with English docstrings and type-consistent naming.



## 🛠 Technologies
- **Python 3.x**
- **SQLite3** (Database)
- **Zipfile & IO** (Buffer-based file processing)
- **Datetime** (Temporal data handling)



## ⚙️ Usage
Prepare a .zip archive containing .txt files with one PESEL number per line.

Run the script:
python PeselReader_v*.py
The system will extract, validate, and store the results in PeselInfo.db.



## 📈 Future Roadmap (Data Engineering Path)
[ ] Add professional Logging to app.log.

[ ] Implement a RejectedRecords table for Data Quality monitoring.

[ ] Containerize the application using Docker.

[ ] Transition to PostgreSQL for scalable storage.



Developed as part of an Object-Oriented Programming course. Focused on building production-ready data pipelines.
