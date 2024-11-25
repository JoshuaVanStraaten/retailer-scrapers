import argparse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import random
import time
import csv
from datetime import datetime
import pandas as pd

def get_random_user_agent():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    ]
    return random.choice(user_agents)

def is_within_scrape_window():
    now = datetime.utcnow()
    start_time = now.replace(hour=4, minute=0, second=0, microsecond=0)
    end_time = now.replace(hour=8, minute=45, second=0, microsecond=0)
    return start_time <= now <= end_time

def scrape_checkers(base_url, start_page=0, end_page=9):
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--enable-unsafe-swiftshader")  # Enable fallback for software WebGL rendering
    options.add_argument(f"user-agent={get_random_user_agent()}")

    # Enable GPU acceleration
    options.add_argument("--enable-gpu")
    options.add_argument("--enable-webgl")
    options.add_argument("--ignore-gpu-blocklist")  # Force GPU acceleration
    
    # Reduce memory usage and improve performance
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-infobars")
    
    # Optimize image loading
    options.add_argument("--blink-settings=imagesEnabled=false")  # Only if you don't need page images
    options.add_experimental_option("prefs", {
        "profile.managed_default_content_settings.images": 2,  # 2 = block images, 1 = allow images
    })
    
    # Memory management
    options.add_argument("--disable-application-cache")
    options.add_argument("--aggressive-cache-discard")
    options.add_argument("--disable-browser-side-navigation")

    driver_path = r"C:\Users\joshu\OneDrive\Documents\coding\web_scraping\chromedriver.exe"  # Replace with actual ChromeDriver path
    service = Service(driver_path)
    driver = webdriver.Chrome(service=service, options=options)

    all_data = []

    try:
        for page in range(start_page, end_page + 1):
            print(f"Scraping Page: {page}")
            if not is_within_scrape_window():
                print("Current time is outside allowed scrape window (04:00-08:45 UTC). Exiting.")
                break

            url = f"{base_url}&page={page}"
            driver.get(url)
            time.sleep(10)  # Respect crawl-delay and request-rate

            specials = driver.find_elements(By.CLASS_NAME, 'item-product')  # Update as needed
            for item in specials:
                product_name = item.find_element(By.CLASS_NAME, 'item-product__name').text.strip()
                images = item.find_elements(By.CSS_SELECTOR, 'img')
                product_image = next(
                    (img.get_attribute('src') for img in images if "discovery-vitality" not in img.get_attribute('src')), None
                )
                try:
                    price_old = item.find_element(By.CLASS_NAME, 'before').text.strip()
                except:
                    price_old = None
                price_current = item.find_element(By.CLASS_NAME, 'now').text.strip()

                all_data.append({
                    'name': product_name,
                    'price': price_old if price_old else price_current,
                    'Promotion Price': price_current if price_old else "No promo",
                    'retailer': "Checkers",
                    'image_url': product_image
                })

    finally:
        driver.quit()

    return all_data

import pandas as pd

def save_to_csv(product_list, filename='products.csv'):
    """
    Save products to a CSV file with an incremental index.
    
    Args:
    product_list (list): List of dictionaries containing product information.
    filename (str): The name of the CSV file (default: 'products.csv').
    """
    if not product_list:
        print("No data to save.")
        return

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(product_list)

    try:
        # Check if the file already exists
        existing_df = pd.read_csv(filename, index_col=0)
        next_index = existing_df.index.max() + 1
    except FileNotFoundError:
        # File does not exist, start index from 0
        next_index = 0

    # Set the DataFrame index starting from the next available index
    df.index = range(next_index, next_index + len(df))
    df.index.name = 'index'

    # Append data to the CSV file
    df.to_csv(filename, mode='a', header=not pd.io.common.file_exists(filename))
    print(f"Data has been saved to {filename}.")

if __name__ == "__main__":
    base_url = "https://www.checkers.co.za/c-2256/All-Departments?q=%3Arelevance%3AallCategories%3Afood%3AbrowseAllStoresFacetOff%3AbrowseAllStoresFacetOff"
    data = scrape_checkers(base_url, start_page=0, end_page=100)  # Needs update

    if data:
        save_to_csv(data)
    else:
        print("Failed to scrape data.")
