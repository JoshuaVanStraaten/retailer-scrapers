import os
import shutil
import subprocess
from datetime import datetime

# Configuration
SCRAPE_CHECKERS_CMD = ["python", "scrape_checkers.py"]
SCRAPE_PNP_CMD = ["python", "scrape_pnp.py", "--timeout", "10", "--url", "https://www.pnp.co.za/c/pnpbase"]
PRODUCTS_FILE = "products.csv"
BACKUP_FOLDER = "backup"

def backup_products_file():
    """
    Move the current products.csv file to the backup folder with a timestamped name.
    """
    if not os.path.exists(PRODUCTS_FILE):
        print(f"{PRODUCTS_FILE} does not exist. No backup needed.")
        return

    # Ensure backup folder exists
    os.makedirs(BACKUP_FOLDER, exist_ok=True)

    # Generate a timestamped backup filename
    backup_filename = os.path.join(
        BACKUP_FOLDER,
        f"products_{(datetime.now().date()).strftime('%Y-%m-%d')}.csv"
    )

    # Move the file
    shutil.move(PRODUCTS_FILE, backup_filename)
    print(f"Moved {PRODUCTS_FILE} to {backup_filename}.")

def run_scrapers():
    """
    Run the scrape_checkers and scrape_pnp scripts simultaneously.
    """
    # Launch both scripts
    processes = [
        subprocess.Popen(SCRAPE_CHECKERS_CMD),
        subprocess.Popen(SCRAPE_PNP_CMD),
    ]

    # Wait for both to complete
    for process in processes:
        process.wait()

    print("Both scrapers have completed.")

if __name__ == "__main__":
    print("Starting daily scrape process...")
    backup_products_file()
    run_scrapers()
    print("Daily scrape process completed.")
