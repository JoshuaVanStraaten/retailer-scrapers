import os
import shutil
import subprocess
from datetime import datetime
import pandas as pd  # Ensure pandas is installed: pip install pandas
import logging

# Configure logging
LOG_FILE = f"scrape_log_{datetime.now().strftime('%Y-%m-%d')}.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Commands
SCRAPE_CHECKERS_CMD = ["python", "scrape_checkers.py"]
SCRAPE_SHOPRITE_CMD = ["python", "scrape_shoprite.py"]
SCRAPE_PNP_CMD = ["python", "scrape_pnp.py", "--timeout", "10", "--url", "https://www.pnp.co.za/c/pnpbase"]
SCRAPE_WOOLWORTHS_CMD = ["python", "scrape_woolworths.py"]

# File Paths
PRODUCTS_FILE = "products.csv"
BACKUP_FOLDER = "backup"
SCRAPER_OUTPUT_FILES = [
    "products_checkers.csv",
    "products_shoprite.csv",
    "products_pnp.csv",
    "products_woolies.csv",
]

def backup_products_file():
    """Move the current products.csv file to the backup folder with a timestamped name."""
    if not os.path.exists(PRODUCTS_FILE):
        logging.warning(f"{PRODUCTS_FILE} does not exist. No backup needed.")
        return

    os.makedirs(BACKUP_FOLDER, exist_ok=True)
    backup_filename = os.path.join(
        BACKUP_FOLDER,
        f"products_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    )
    shutil.move(PRODUCTS_FILE, backup_filename)
    logging.info(f"Moved {PRODUCTS_FILE} to {backup_filename}.")

def run_all_scrapers():
    """Run all scrapers simultaneously."""
    logging.info("Starting all scrapers in parallel...")
    processes = []
    commands = [
        SCRAPE_CHECKERS_CMD,
        SCRAPE_PNP_CMD,
        SCRAPE_SHOPRITE_CMD,
        SCRAPE_WOOLWORTHS_CMD,
    ]
    for cmd in commands:
        try:
            process = subprocess.Popen(cmd)
            processes.append((cmd, process))
            logging.info(f"Started {cmd[1]} scraper.")
        except Exception as e:
            logging.error(f"Failed to start {cmd[1]} scraper: {e}")

    # Wait for all processes to complete
    for cmd, process in processes:
        try:
            process.wait()
            if process.returncode == 0:
                logging.info(f"{cmd[1]} scraper completed successfully.")
            else:
                logging.error(f"{cmd[1]} scraper failed with return code {process.returncode}.")
        except Exception as e:
            logging.error(f"Error waiting for {cmd[1]} scraper to complete: {e}")

def combine_csv_files():
    """Combine all scraper output files into a single products.csv and delete the individual files."""
    logging.info("Combining scraper output files...")
    combined_data = []
    for file in SCRAPER_OUTPUT_FILES:
        if os.path.exists(file):
            try:
                data = pd.read_csv(file)
                combined_data.append(data)
                logging.info(f"Loaded {file} successfully.")
            except Exception as e:
                logging.error(f"Failed to load {file}: {e}")
        else:
            logging.warning(f"{file} not found. Skipping.")

    if combined_data:
        try:
            combined_df = pd.concat(combined_data, ignore_index=True)
            combined_df.to_csv(PRODUCTS_FILE, index=False)
            logging.info(f"Combined data saved to {PRODUCTS_FILE}.")
        except Exception as e:
            logging.error(f"Failed to combine and save data: {e}")

    # Delete individual scraper files
    for file in SCRAPER_OUTPUT_FILES:
        if os.path.exists(file):
            try:
                os.remove(file)
                logging.info(f"Deleted {file}.")
            except Exception as e:
                logging.error(f"Failed to delete {file}: {e}")

if __name__ == "__main__":
    logging.info("Starting daily scrape process...")
    backup_products_file()
    run_all_scrapers()
    combine_csv_files()
    logging.info("Daily scrape process completed.")
