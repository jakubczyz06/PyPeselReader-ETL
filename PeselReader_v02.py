#Imports
from datetime import date
import sqlite3
import zipfile
import io



#E - Extracting files
def extract_from_zip(zip_file):
    """
        Opens a ZIP archive, iterates through all .txt files,
        and processes each line as a PESEL number.
    """
    print(f"---ROZPOCZYNAM EKSTRAKCJĘ Z {zip_file}---")

    try:
        with zipfile.ZipFile(zip_file, 'r') as z:
            for file_name in z.namelist():
                if file_name.endswith(".txt"):
                    print(f"\n---PRZETWARZAM PLIK {file_name}---")

                    with z.open(file_name) as f:
                        for line in io.TextIOWrapper(f, encoding='utf-8'):
                            pesel = line.strip()

                            if pesel:
                                try:
                                    new_person = PeselReader(pesel)
                                    new_person.save_to_db()
                                except ValueError as e1:
                                    print(f"Błąd w {file_name} dla {pesel}: {e1}")

    except FileNotFoundError:
        print(f"Plik nieodnaleziony.")
    except zipfile.BadZipFile:
        print(f"Plik {zip_file} nie jest plikiem .zip, bądź jest jest uszkodzony.")




#Month dictionary used in the class PeselReader
MSC_DICT = {1: 'stycznia', 2: 'lutego', 3: 'marca',
            4: 'kwietnia', 5: 'maja', 6: 'czerwca',
            7: 'lipca', 8: 'sierpnia', 9: 'września',
            10: 'października', 11: 'listopada', 12: 'grudnia'}



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
        verb_type = "urodzona" if gender == "Kobieta" else "urodzony"

        return f"{gender}, {verb_type} {birth_date.day} {MSC_DICT[birth_date.month]} {birth_date.year} roku"


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
                    gender TEXT NOT NULL,
                    birth_date TEXT NOT NULL)""")

        gender = self.get_gender()
        birth_date_str = str(self.get_birth_date())

        try:
            cur.execute("INSERT INTO PeselInfo(pesel, gender, birth_date) VALUES (?, ?, ?)",
                        (self.pesel_str, gender, birth_date_str))
            con.commit()
            print(f"Zapisano PESEL: {self.pesel_str}")
        except sqlite3.IntegrityError:
            print(f"Info: PESEL {self.pesel_str} już istnieje w bazie.")
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
                except ValueError as e2:
                    print(f"Pominięto błędny PESEL: {e2}")
            return pesel_objects
        except sqlite3.Error as e3:
            print(f"Błąd odczytu: {e3}")
            return []
        finally:
            con.close()



#Testing
if __name__ == "__main__":
    extract_from_zip("data.zip")

    print("\n---POBIERAM DANE Z BAZY---")
    citizens_list = PeselReader.load_everything(db_name="PeselInfo.db")

    if not citizens_list:
        print("Baza jest pusta, bądź wystąpił błąd.")
    else:
        for citizen in citizens_list:
            print(f"- {citizen}")
