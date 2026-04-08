#Imports
from datetime import date
import sqlite3
import zipfile
import io
import csv
import logging




#Logger configuration
logging.basicConfig(
                    filename = 'pesel.log',
                    filemode = 'a',
                    style = "{",
                    level = logging.INFO,
                    format = "{asctime} - {levelname} - {message}",
                    datefmt = "%Y-%m-%d %H:%M",
                    )




#E - Extracting files
def extract_from_zip(zip_file):
    """
        Opens a ZIP archive, iterates through all .csv files,
        and processes each line as a PESEL number.
    """
    logging.info(f"---ROZPOCZYNAM EKSTRAKCJĘ Z {zip_file}---")

    try:
        with zipfile.ZipFile(zip_file, 'r') as z:
            for file_name in z.namelist():
                if file_name.endswith(".csv"):
                    logging.info(f"\n---PRZETWARZAM PLIK {file_name}---")

                    with z.open(file_name) as csvfile:
                        csv_file = io.TextIOWrapper(csvfile, encoding = 'windows-1250')
                        reader = csv.DictReader(csv_file)

                        for row in reader:
                            pesel_val = row['pesel'].strip()

                            if pesel_val:
                                try:
                                    new_person = PeselReader(pesel_val)
                                    new_person.save_to_db()
                                    print(f"{new_person}")
                                except ValueError as e:
                                    PeselReader.log_rejection(pesel_val, str(e))
                                    logging.info(f"Błędny PESEL {file_name} dla {pesel_val}: {e}")
    except FileNotFoundError:
        logging.info(f"Plik nieodnaleziony.")
    except zipfile.BadZipFile:
        logging.info(f"Plik {zip_file} nie jest plikiem .zip, bądź jest jest uszkodzony.")





#Month dictionary used in the class PeselReader
MM_DICT = {1: 'styczeń', 2: 'luty', 3: 'marzec',
            4: 'kwiecień', 5: 'maj', 6: 'czerwiec',
            7: 'lipiec', 8: 'sierpień', 9: 'wrzesień',
            10: 'październik', 11: 'listopad', 12: 'grudzień'}





#T - Transforming the data and the entire PeselReader class
class PeselReader:
    def __init__(self, pesel_str):
        #The PESEL number must consist of eleven digits
        if not (pesel_str.isdigit() and len(pesel_str) == 11):
            raise ValueError("Podany numer PESEL jest nieprawidłowy.")

        self.pesel_str = pesel_str
        self.checksum()



    def checksum(self):
        #Checksum function mainly validating PESEL numbers
        weights = [1, 3, 7, 9, 1, 3, 7, 9, 1, 3]
        pesel_sum = 0

        for i in range(10):
            pesel_sum += int(self.pesel_str[i]) * weights[i]

        checking = (10 - (pesel_sum % 10)) % 10
        if checking != int(self.pesel_str[10]):
            raise ValueError("Błąd checksum.")



    def decode_date_parts(self):
        """
                Decodes raw PESEL segments into year, month, and day,
                adjusting for the century based on the month offset.
        """
        yy = int(self.pesel_str[:2])
        mm_raw = int(self.pesel_str[2:4])
        dd = int(self.pesel_str[4:6])

        if 41 <= mm_raw <= 52:
            yy, msc = 2100 + yy, mm_raw - 40
        elif 21 <= mm_raw <= 32:
            yy, msc = 2000 + yy, mm_raw - 20
        elif 1 <= mm_raw <= 12:
            yy, msc = 1900 + yy, mm_raw
        elif 81 <= mm_raw <= 92:
            yy, msc = 1800 + yy, mm_raw - 80
        else:
            raise ValueError("Błędny kod miesiąca w numerze PESEL.")

        return yy, msc, dd



    def get_birth_date(self):
        #Returns a Python date object representing the citizen's birthdate.
        yy, msc, dd = self.decode_date_parts()
        try:
            return date(yy, msc, dd)
        except ValueError:
            raise ValueError("Niepoprawna data w numerze PESEL.")



    def get_gender(self):
        #Identifies gender based on the 10th digit of the PESEL string.
        gender_number = int(self.pesel_str[9])

        return "Kobieta" if gender_number % 2 == 0 else "Mężczyzna"



    def __str__(self):
        #Returns a formatted string summary of the citizen's data in Polish.
        gender = self.get_gender()
        birth_date = self.get_birth_date()

        gender_short = "K" if gender == "Kobieta" else "M"

        return f"PRAWDZIWY, {gender_short}, {birth_date.day}, {MM_DICT[birth_date.month]}, {birth_date.year}"



    def save_to_db(self, db_name="PeselInfo.db"):
        """
            Connects to the SQLite database, ensures the table exists,
            and inserts the current PESEL record. Handles duplicate entries.
        """
        con = sqlite3.connect(db_name)
        cur = con.cursor()

        cur.execute("""CREATE TABLE IF NOT EXISTS PeselInfo(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pesel TEXT UNIQUE NOT NULL,
                    status TEXT NOT NULL,
                    gender TEXT NOT NULL,
                    birth_day TEXT NOT NULL,
                    birth_month TEXT NOT NULL,
                    birth_year TEXT NOT NULL)""")

        cur.execute("""CREATE TABLE IF NOT EXISTS RejectedPeselInfo(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    raw_data TEXT,
                    error_reason TEXT,
                    date_time DATETIME DEFAULT CURRENT_TIMESTAMP)""")

        birth_date = self.get_birth_date()
        gender_short = "K" if self.get_gender() == "Kobieta" else "M"

        try:
            cur.execute("""INSERT INTO PeselInfo(pesel, status, gender, birth_day, birth_month, birth_year)
                        VALUES (?, ?, ?, ?, ?, ?)""",
                        (self.pesel_str, "PRAWIDŁOWY", gender_short, str(birth_date.day),
                         MM_DICT[birth_date.month], str(birth_date.year)))
            con.commit()
            logging.info(f"Zapisano PESEL: {self.pesel_str}")
        except sqlite3.IntegrityError:
            pass
        finally:
            con.close()



    @classmethod
    def load_everything(cls, db_name="PeselInfo.db"):
        """
                L - Loading, fetches all stored PESEL numbers from the database and
                returns a list of PeselReader objects. Invalid records are skipped.
        """
        con = sqlite3.connect(db_name)
        cur = con.cursor()
        pesel_objects = []
        try:
            cur.execute("SELECT pesel FROM PeselInfo")
            records = cur.fetchall()
            for row in records:
                try:
                    pesel_objects.append(cls(row[0]))
                except ValueError as e:
                    logging.info(f"Pominięto błędny PESEL: {e}")
            return pesel_objects
        except sqlite3.Error as e:
            logging.info(f"Błąd odczytu: {e}")
            return []
        finally:
            con.close()



    @staticmethod
    def log_rejection(raw_data, reason, db_name="PeselInfo.db"):
        """
            Adding a method rejecting bad PESEL records.
        """
        con = sqlite3.connect(db_name)
        cur = con.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS RejectedPeselInfo(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        raw_data TEXT,
                        error_reason TEXT,
                        date_time DATETIME DEFAULT CURRENT_TIMESTAMP)""")

        cur.execute("INSERT INTO RejectedPeselInfo(raw_data, error_reason) VALUES (?, ?)",
                    (raw_data, reason))
        con.commit()
        con.close()




#Testing
if __name__ == "__main__":
    logging.info("START SYSTEMU: Inicjalizacja potoku ETL.")
    extract_from_zip("data.zip")

    logging.info("KONIEC SYSTEMU: Proces ETL zakończony pomyślnie.")
    print("Proces zakończony. Wyniki w bazie danych, błędy w pesel.log.")
