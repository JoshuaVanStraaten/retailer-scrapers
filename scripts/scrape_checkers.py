from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import random
import time
import math
import psutil
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from supabase import create_client
import pandas as pd
import requests
from datetime import datetime
import unicodedata
import mimetypes

# Constants
SUPABASE_URL = "<supabase_url>"
SUPABASE_KEY = "<supabase_key>"
LOCAL_FOLDER_PATH = os.path.join('.', 'checkers_images')
BUCKET_NAME = 'product_images'
REMOTE_FOLDER_PATH = 'checkers/'

def get_random_user_agent():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    ]
    return random.choice(user_agents)

def download_image(url, save_path):
    """
    Downloads an image or SVG from a URL and saves it locally.
    Uses Pillow for image processing.

    Args:
        url (str): The URL of the image.
        save_path (str): The local path to save the image.

    Returns:
        bool: True if download and conversion (if applicable) were successful, False otherwise.
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "")
        extension = mimetypes.guess_extension(content_type)

        if content_type == "image/svg+xml" or extension == ".svg":
            # Handle SVG
            print(f"Detected SVG content: {url}")
            svg_path = save_path.replace(".jpg", ".svg")
            png_path = save_path.replace(".jpg", ".png")

            # Save original SVG
            with open(svg_path, "wb") as file:
                file.write(response.content)

            # Convert SVG to PNG using urllib and Pillow
            try:
                import svglib.svglib
                from reportlab.graphics import renderPM
                svg_drawing = svglib.svglib.svg2rlg(svg_path)
                renderPM.drawToFile(svg_drawing, png_path, fmt='PNG')
                print(f"Converted SVG to PNG: {png_path}")
            except ImportError:
                print("svglib or reportlab not installed. Skipping SVG to PNG conversion.")

            return True
        else:
            # Save as regular image (JPEG, PNG, etc.)
            with open(save_path, "wb") as file:
                file.write(response.content)
            return True

    except Exception as e:
        print(f"Failed to download or process {url}: {e}")
        return False

def verify_file_in_supabase(bucket_name, remote_path):
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    try:
        files = supabase.storage.from_(bucket_name).list(
            REMOTE_FOLDER_PATH,
            {"limit": 100, "offset": 0, "sortBy": {"column": "name", "order": "desc"}},
        )
        for file in files:
            if file['name'] == remote_path.removeprefix(REMOTE_FOLDER_PATH):
                return True
        return False
    except Exception as e:
        print(f"Verification error for {remote_path}: {e}")
        return False

def upload_file_to_supabase(local_path, bucket_name, remote_path, retries=5, backoff_factor=2):
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    attempt = 0

    while attempt < retries:
        try:
            with open(local_path, 'rb') as f:
                # First check if file is in supabase
                if verify_file_in_supabase(bucket_name, remote_path):
                    print(f"File {remote_path} already exists, skipping upload ...")
                    return supabase.storage.from_(bucket_name).get_public_url(remote_path)
                else:
                    response = supabase.storage.from_(bucket_name).upload(remote_path, f)
                    print(f"Supabase upload response: {response}")

            if verify_file_in_supabase(bucket_name, remote_path):
                print(f"Upload and verification successful for {remote_path}")
                return supabase.storage.from_(bucket_name).get_public_url(remote_path)
            else:
                print(f"Verification failed for {remote_path}. Retrying...")

        except Exception as e:
            try:
                # Attempt to parse the error message as a dictionary
                error_content = eval(str(e))
                if isinstance(error_content, dict) and error_content.get('message') == 'The resource already exists':
                    print(f"Resource already exists. Using existing URL for {remote_path}")
                    return supabase.storage.from_(bucket_name).get_public_url(remote_path)
            except (SyntaxError, ValueError):
                # Handle cases where the error content is not a valid dictionary
                pass
                print(f"Upload error: {e}")

        attempt += 1
        if attempt < retries:
            sleep_time = backoff_factor ** attempt
            print(f"Retrying upload in {sleep_time} seconds...")
            time.sleep(sleep_time)

    print(f"Failed to upload {local_path} after {retries} attempts.")
    return None

def get_last_index(csv_file):
    try:
        df = pd.read_csv(csv_file)
        if 'index' in df.columns and not df['index'].isnull().all():
            last_index = df['index'].max()
            return int(last_index) + 1 if pd.notna(last_index) else 0
        else:
            return 0
    except FileNotFoundError:
        return 0

def get_last_index_from_scraped_data(scraped_data):
    """
    Get the last index from scraped data, which is a list of dictionaries.

    Args:
        scraped_data (list): List of dictionaries containing product data.

    Returns:
        int: The next index to use based on the last index in the scraped data.
    """
    if not scraped_data:
        return 0  # If the list is empty, start from 0

    # Extract the 'index' values and find the maximum
    indices = [item.get('index', -1) for item in scraped_data]
    max_index = max(indices, default=-1)  # Use -1 as the default for empty lists or missing 'index'
    return max_index + 1

def get_price(price_old, price_current):
    def extract_numeric_price(price_str):
        # If price is None or empty, return None
        if not price_str:
            return None

        # Try to convert to float while preserving prefix/suffix
        try:
            # Find the first set of numeric characters (including decimal)
            numeric_part = ''.join(char for char in price_str if char.isdigit() or char == '.')
            float_value = float(numeric_part)

            # Preserve the original string if a valid float is found
            return {
                'value': float_value,
                'original': price_str
            }
        except ValueError:
            return None

    # Process both prices
    processed_old = extract_numeric_price(price_old)
    processed_current = extract_numeric_price(price_current)

    # Prioritize old price, then current price
    if processed_old is not None:
        return processed_old['original']
    elif processed_current is not None:
        return processed_current['original']
    else:
        return "no price available"

def scrape_page(base_url, page, existing_data, current_index, save_filename='products.csv', max_retries=3):
    """
    Scrape a specific page and retry if an error occurs.

    Args:
        base_url (str): The base URL for scraping.
        page (int): The page number to scrape.
        existing_data (dict): Existing data to check for duplicates.
        current_index (int): The current index for products.
        save_filename (str): The file to save scraped data.
        max_retries (int): Maximum number of retry attempts.

    Returns:
        tuple: (scraped data as a list, updated current index)
    """
    retries = 0
    while retries <= max_retries:
        try:
            # Configure Selenium options
            options = Options()
            options.add_argument("--headless=new")
            options.add_argument("--enable-unsafe-swiftshader")
            options.add_argument(f"user-agent={get_random_user_agent()}")
            options.add_argument("--enable-gpu")
            options.add_argument("--enable-webgl")
            options.add_argument("--ignore-gpu-blocklist")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-notifications")
            options.add_argument("--disable-infobars")
            options.add_argument("--blink-settings=imagesEnabled=false")
            options.add_experimental_option("prefs", {
                "profile.managed_default_content_settings.images": 2,
            })
            options.add_argument("--disable-application-cache")
            options.add_argument("--aggressive-cache-discard")
            options.add_argument("--disable-browser-side-navigation")

            driver_path = r"path\to\chromedriver.exe"
            service = Service(driver_path)
            driver = webdriver.Chrome(service=service, options=options)

            scraped_data = []
            try:
                print(f"Scraping page {page}")
                url = f"{base_url}&page={page}"
                driver.get(url)
                time.sleep(5)  # Adjust delay if necessary

                specials = driver.find_elements(By.CLASS_NAME, 'item-product')
                for index, item in enumerate(specials):
                    current_index += 1
                    product_name = item.find_element(By.CLASS_NAME, 'item-product__name').text.strip()
                    try:
                        price_old = item.find_element(By.CLASS_NAME, 'before').text.strip()
                    except:
                        price_old = None
                    price_current = item.find_element(By.CLASS_NAME, 'now').text.strip()

                    existing_product = existing_data.get(product_name)
                    if existing_product:
                        product_image_url = existing_product['image_url']
                    else:
                        images = item.find_elements(By.CSS_SELECTOR, 'img')
                        product_image = next(
                            (img.get_attribute('src') for img in images if "discovery-vitality" not in img.get_attribute('src')),
                            None
                        )

                        product_image_url = None
                        if product_image:
                            normalized = unicodedata.normalize('NFKD', product_name.replace(" ", "_")).encode('ascii', 'ignore').decode('ascii')
                            sanitized = re.sub(r'[^\w\.-]', '_', normalized)
                            file_name = f"checkers_image_{sanitized}.jpg"
                            save_path = os.path.join(LOCAL_FOLDER_PATH, file_name)
                            os.makedirs(LOCAL_FOLDER_PATH, exist_ok=True)
                            if download_image(product_image, save_path):
                                remote_path = f"{REMOTE_FOLDER_PATH}{file_name}"
                                product_image_url = upload_file_to_supabase(save_path, BUCKET_NAME, remote_path)

                    scraped_data.append({
                        'index': str((page * 20) - 1 + current_index),
                        'name': product_name,
                        'price': get_price(price_old, price_current),
                        'promotion_price': price_current if price_old else "No promo",
                        'retailer': "Checkers",
                        'image_url': product_image_url,
                    })
                # Save data incrementally
                save_to_csv(scraped_data, filename=save_filename)
                return scraped_data, current_index

            finally:
                driver.quit()

        except Exception as e:
            retries += 1
            print(f"Error scraping page {page}: {e}")
            if retries <= max_retries:
                print(f"Retrying... Attempt {retries} of {max_retries}")
                time.sleep(2 ** retries)  # Exponential backoff
            else:
                print(f"Failed to scrape page {page} after {max_retries} retries.")
                return [], current_index

def get_optimal_threads():
    # Get the number of logical processors
    cpu_count = os.cpu_count()

    # For I/O-bound tasks, using 2x-4x the CPU count is often optimal
    optimal_threads = cpu_count  # Adjust as needed

    # Limit based on system memory (1GB per thread as an example threshold)
    total_memory = psutil.virtual_memory().total // (1024 * 1024 * 1024)  # in GB
    memory_limited_threads = total_memory * 2  # Adjust memory scaling as needed

    # Use the minimum of CPU-based and memory-based limits
    return min(optimal_threads, memory_limited_threads)

def scrape_checkers_concurrently(base_url, start_page, end_page, existing_data, starting_index):
    optimal_threads = 6  # For now just set to 6
    print(f"Using {optimal_threads} threads based on system specs.")

    all_results = []
    current_index = starting_index

    with ThreadPoolExecutor(optimal_threads) as executor:
        futures = {
            executor.submit(scrape_page, base_url, page, existing_data, current_index): page
            for page in range(start_page, end_page + 1)
        }

        for future in as_completed(futures):
            try:
                result, current_index = future.result()
                # Debugging the type of current_index
                if not isinstance(current_index, (int, float)):
                    print(f"Error: current_index is not a real number (type: {type(current_index)})")
                all_results.extend(result)
            except Exception as e:
                print(f"Error scraping page: {e}")
    return all_results

def load_existing_data(csv_file):
    try:
        # Try reading the CSV file with UTF-8 encoding first
        df = pd.read_csv(csv_file, encoding='utf-8')
    except UnicodeDecodeError:
        # If UTF-8 fails, try an alternative encoding (e.g., 'latin1')
        print(f"Warning: Failed to read {csv_file} with UTF-8 encoding. Trying 'latin1'.")
        df = pd.read_csv(csv_file, encoding='latin1')
    except FileNotFoundError:
        print(f"Error: File {csv_file} not found.")
        return {}

    # Check for rows with NaN values and log them
    rows_with_nan = df[df.isna().any(axis=1)]
    if not rows_with_nan.empty:
        print(f"Warning: Found rows with NaN values:\n{rows_with_nan}")

    # Convert the DataFrame to a dictionary
    return {
        row['name']: row.to_dict() for _, row in df.iterrows()
    }

def upsert_to_supabase(data, batch_size=500):
    """
    Upserts data to Supabase in batches.

    Args:
        data (list): A list of dictionaries containing the rows to be upserted.
        batch_size (int): The number of rows to upsert in each batch (default: 500).

    Returns:
        None
    """
    from supabase import create_client

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    try:
        total_rows = len(data)
        print(f"Total rows to upsert: {total_rows}")

        for start in range(0, total_rows, batch_size):
            end = start + batch_size
            batch = data[start:end]
            print(f"Upserting batch: {start + 1} to {end}")

            response = supabase.table('Products').upsert(batch).execute()
            print(f"Batch upsert response: {response}")

    except Exception as e:
        print(f"Error upserting to Supabase: {e}")


def save_to_csv(product_list, filename='products.csv'):
    """
    Save products to a CSV file, appending data if the file exists, otherwise creating a new file.

    Args:
    product_list (list): List of dictionaries containing product information.
    filename (str): The name of the CSV file (default: 'products.csv').
    """
    if not product_list:
        print("No data to save.")
        return

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(product_list)

    if 'index' in df.columns:
        df['index'] = pd.to_numeric(df['index'], errors='coerce')  # Convert to numeric, coerce errors to NaN
        df.set_index('index', inplace=True)
    else:
        df.index = range(0, len(df))
        df.index.name = 'index'

    # Check if the file already exists to determine header inclusion
    if not os.path.exists(filename):
        # Save data to CSV with header if the file does not exist
        df.to_csv(filename, mode='w', header=True)
        print(f"Data has been saved to {filename}.")
    else:
        # Append data to CSV without header if the file exists
        df.to_csv(filename, mode='a', header=False)
        print(f"Data has been appended to {filename}.")

if __name__ == "__main__":
    base_url = "https://www.checkers.co.za/c-2256/All-Departments?q=%3Arelevance"
    existing_data = load_existing_data('products_old.csv')
    starting_index = get_last_index('products.csv')
    scraped_data = scrape_checkers_concurrently(base_url, start_page=0, end_page=375,
                                                existing_data=existing_data, starting_index=starting_index)

    if scraped_data:
        new_data = load_existing_data('products.csv')
        upsert_to_supabase(list(new_data.values()))
        print("Data saved and updated.")

    else:
        print("No new data scraped.")

