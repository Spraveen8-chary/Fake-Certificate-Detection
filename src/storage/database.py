import psycopg2
from dotenv import load_dotenv
import os
import sys
import bcrypt

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.core.logging import get_logger
logger = get_logger("Database")

class SupabaseDB:

    """
    Database Tables and their structure:
    
    📌 `admins`

    | Column      | Data Type                   |
    | ----------- | --------------------------- |
    | admin_id    | uuid                        |
    | name        | text                        |
    | email       | text                        |
    | password    | text                        |
    | role        | text                        |
    | created_at  | timestamp without time zone |


    📌 `certificates`

    | Column               | Data Type                   |
    | ---------------------| --------------------------- |
    | cert_id              | uuid                        |
    | student_id           | uuid                        |
    | univ_id              | uuid                        |
    | roll_no              | text                        |
    | student_name_hash    | text                        |
    | dob_hash             | text                        |
    | gpa_hash             | text                        |
    | batch_year           | integer                     |
    | issued_date          | date                        |
    | file_url             | text                        |
    | image_hash           | text                        |
    | signature_embeddings | bytea                       |
    | photo_embeddings     | bytea                       |
    | logo_embeddings      | bytea                       |
    | qr_code_cipher       | text                        |
    | created_at           | timestamp without time zone |



    📌 `students`

    | Column            | Data Type                   |
    | ----------------- | --------------------------- |
    | student_id        | uuid                        |
    | roll_no           | text                        |
    | name              | text                        |
    | dob               | date                        |
    | email             | text                        |
    | password          | text                        |
    | univ_id           | uuid                        |
    | passed_out_year   | integer                     |
    | created_at        | timestamp without time zone |



    📌 `universities`

    | Column                | Data Type                   |
    | --------------------- | --------------------------- |
    | univ_id               | uuid                        |
    | name                  | text                        |
    | address               | text                        |
    | private_key           | text                        |
    | public_key            | text                        |
    | signature_embeddings  | bytea                       |
    | logo_embeddings       | bytea                       |
    | stamp_embeddings      | bytea                       |
    | created_at            | timestamp without time zone |


    📌 `verification_logs`

    | Column       | Data Type                   |
    | ------------ | --------------------------- |
    | log_id       | uuid                        |
    | cert_id      | uuid                        |
    | verified_by  | uuid                        |
    | status       | boolean                     |
    | reason       | text                        |
    | verified_at  | timestamp without time zone |


    """

    def __init__(self):
        """Initialize connection parameters from .env file"""
        load_dotenv()
        self.user = os.getenv("user")
        self.password = os.getenv("password")
        self.host = os.getenv("host")
        self.port = os.getenv("port")
        self.dbname = os.getenv("dbname")
        self.connection = None
        self.cursor = None

    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                dbname=self.dbname
            )
            self.cursor = self.connection.cursor()
            print("✅ Connection successful!")
        except Exception as e:
            print(f"❌ Failed to connect: {e}")

    def run_query(self, query, fetch_one=False, fetch_all=False):
        """Execute SQL queries"""
        try:
            self.cursor.execute(query)
            if fetch_one:
                return self.cursor.fetchone()
            elif fetch_all:
                return self.cursor.fetchall()
            else:
                self.connection.commit()
        except Exception as e:
            print(f"⚠️ Query failed: {e}")
            return None

    def insert_admin(self, name:str, email:str, password:str, role:str):
        """Insert a new admin into the admins table"""
        try:
            salt = bcrypt.gensalt()
            password = bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')
            query = f"""
            INSERT INTO admins (name, email, password, role)
            VALUES ('{name}', '{email}', '{password}', '{role}');
            """
            self.run_query(query)
            logger.info("✅ Admin inserted successfully!")
        except Exception as e:
            logger.error(f"❌ Failed to insert admin: {e}")
        
    def delete_admin_by_id(self, admin_id:str):
        """Delete an admin from the admins table"""
        try:
            query = f"DELETE FROM admins WHERE admin_id = '{admin_id}';"
            self.run_query(query)
            logger.info("✅ Admin deleted successfully!")
        except Exception as e:
            logger.error(f"❌ Failed to delete admin: {e}")

    def update_admin_email(self, admin_id:str, new_email:str):
        """Update an admin's email in the admins table"""
        try:
            query = f"""
            UPDATE admins
            SET email = '{new_email}'
            WHERE admin_id = '{admin_id}';
            """
            self.run_query(query)
            logger.info("✅ Admin email updated successfully!")
        except Exception as e:
            logger.error(f"❌ Failed to update admin email: {e}")
    
    def update_admin_password(self, email:str, new_password:str):
        """Update an admin's password in the admins table"""
        try:
            salt = bcrypt.gensalt()
            new_password = bcrypt.hashpw(new_password.encode('utf-8'), salt).decode('utf-8')
            query = f"""
            UPDATE admins
            SET password = '{new_password}'
            WHERE email = '{email}';
            """
            self.run_query(query)
            logger.info("✅ Admin password updated successfully!")
        except Exception as e:
            logger.error(f"❌ Failed to update admin password: {e}")


    def delete_admin_by_mail(self, email:str):
        """Delete an admin from the admins table"""
        try:
            query = f"DELETE FROM admins WHERE email = '{email}';"
            self.run_query(query)
            logger.info("✅ Admin deleted successfully!")
        except Exception as e:
            logger.error(f"❌ Failed to delete admin: {e}")


    def get_admin_by_email(self, email:str):
        """Fetch an admin's details by email"""
        try:
            query = f"SELECT admin_id, email, name, role FROM admins WHERE email = '{email}';"
            result = self.run_query(query, fetch_one=True)
            return result
        except Exception as e:
            logger.error(f"❌ Failed to fetch admin: {e}")
            return None

    def admin_exists(self, email:str) -> bool:
        """Check if an admin exists by email"""
        try:
            query = f"SELECT 1 FROM admins WHERE email = '{email}';"
            result = self.run_query(query, fetch_one=True)
            return result is not None
        except Exception as e:
            logger.error(f"❌ Failed to check admin existence: {e}")
            return False

    def get_all_admins(self):
        """Fetch all admins"""
        try:
            query = "SELECT admin_id, email, name, role, created_at FROM admins;"
            result = self.run_query(query, fetch_all=True)
            return result
        except Exception as e:
            logger.error(f"❌ Failed to fetch admins: {e}")
            return []
    
    def get_admin_count(self) -> int:
        """Get total number of admins"""
        try:
            query = "SELECT COUNT(*) FROM admins;"
            result = self.run_query(query, fetch_one=True)
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"❌ Failed to count admins: {e}")
            return 0
    
    def admin_login(self, email:str, password:str) -> bool:
        """Validate admin login credentials"""
        try:
            query = f"SELECT password FROM admins WHERE email = '{email}';"
            result = self.run_query(query, fetch_one=True)
            if result:
                stored_password = result[0]
                return bcrypt.checkpw(password.encode('utf-8'), stored_password.encode('utf-8'))
            return False
        except Exception as e:
            logger.error(f"❌ Failed to validate admin login: {e}")
            return False

    # ================================== Admin Management ===================================

    def insert_student(self, name:str, email:str, password:str, roll_no:str, dob:str, univ_id:str, passed_out_year:int):
        """Insert a new student into the students table"""
        try:
            salt = bcrypt.gensalt()
            password = bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')
            def check_dob_format(dob_str):
                try:
                    day, month, year = map(int, dob_str.split('-'))
                    if 1 <= day <= 31 and 1 <= month <= 12 and 1900 <= year <= 2100:
                        return True
                    return False
                except:
                    return False
            if not check_dob_format(dob):
                logger.error("❌ Invalid date of birth format. Please use DD-MM-YYYY.")
                return
            
            query = f"""
            INSERT INTO students (name, email, password, roll_no, dob, univ_id, passed_out_year)
            VALUES ('{name}', '{email}', '{password}', '{roll_no}', '{dob}', '{univ_id}', {passed_out_year});
            """
            self.run_query(query)
            logger.info("✅ Student inserted successfully!")
        except Exception as e:
            logger.error(f"❌ Failed to insert student: {e}")


    def close(self):
        """Close cursor and connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("🔒 Connection closed.")



if __name__ == "__main__":
    db = SupabaseDB()
    db.connect()
    
    # Example query
    result = db.run_query("SELECT NOW();", fetch_one=True)
    print("Current Time:", result)
    
    # Insert example
    # db.run_query("INSERT INTO users (name, email) VALUES ('Praveen', 'praveen@example.com');")
    
    db.close()
