import sqlite3
import os
from oasis.logic.onboarding import resolved_db_path

db_path = resolved_db_path(os.path.dirname(os.path.abspath('entrypoint.py')))
print(f"Checking DB: {db_path}")

conn = sqlite3.connect(db_path)
from oasis.logic.auth_manager import hash_password

# clear lockouts and force passwords to oasis2026
conn.execute("UPDATE OASIS_USERS SET FAILED_ATTEMPTS = 0, LOCKOUT_UNTIL = NULL, PASSWORD_HASH = ?", (hash_password('oasis2026'),))
conn.commit()

user = conn.execute("SELECT USERNAME, FAILED_ATTEMPTS, LOCKOUT_UNTIL FROM OASIS_USERS WHERE USERNAME='ops_admin'").fetchone()
print(f"User ops_admin state: {user}")

conn.close()
print("Passwords force-reset in resolved_db_path to oasis2026")
