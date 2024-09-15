import argparse
from requests_html import HTMLSession
from bs4 import BeautifulSoup
import csv
import time
import random
from datetime import datetime, time as dtime
import pytz
from urllib.parse import urlparse, urljoin

def get_random_user_agent():
    """
    Returns a random user agent string from a predefined list.
    This helps to mimic different browsers and avoid detection as a bot.
    """
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    ]
    return random.choice(user_agents)

def is_allowed_time():
    """
    Check if the current time is within the allowed visit time (04:00-08:45 UTC).
    """
    utc_now = datetime.now(pytz.UTC).time()
    start_time = dtime(3, 0)
    end_time = dtime(8, 45)
    return start_time <= utc_now <= end_time

def is_allowed_url(url):
    """
    Check if the URL is allowed according to the robots.txt rules.
    """
    parsed_url = urlparse(url)
    path = parsed_url.path
    query = parsed_url.query

    disallowed_paths = ['/cart', '/checkout', '/my-account']
    disallowed_queries = ['allSpecials', 'q=%3Anovelty%3A', 'q=%3Aname-asc%3A']

    if any(path.startswith(disallowed) for disallowed in disallowed_paths):
        return False
    if any(disallowed in query for disallowed in disallowed_queries):
        return False
    return True

def scrape_shoprite(url):
    """
    Scrapes product information from the given Shoprite URL.
    
    Args:
    url (str): The URL of the Shoprite page to scrape.
    
    Returns:
    list: A list of dictionaries containing product information (name and price).
    """
    if not is_allowed_time():
        print("Current time is outside the allowed visit time (04:00-08:45 UTC). Exiting.")
        return None

    if not is_allowed_url(url):
        print(f"Access to {url} is disallowed by robots.txt. Skipping.")
        return None

    session = HTMLSession()
    
    headers = {
        "User-Agent": get_random_user_agent(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.shoprite.co.za/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    
    try:
        # Send a GET request to the URL
        response = session.get(url, headers=headers)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Parse the HTML content
            soup = BeautifulSoup(response.html.html, 'html.parser')
            
            # Find all product items
            products = soup.find_all('div', class_='item-product')
            
            product_list = []
            for product in products:
                # Extract product name and price
                name = product.find('h3', class_='item-product__name').text.strip()
                price = product.find('span', class_='now').text.strip()
                
                product_list.append({
                    'name': name,
                    'price': price
                })
            
            print(f"Successfully scraped {len(product_list)} products.")
            return product_list
        else:
            print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
            print(f"Response content: {response.text[:500]}...")  # Print first 500 characters of the response
            return None
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None
    
    finally:
        session.close()

def save_to_csv(product_list, filename='shoprite_products.csv'):
    """
    Saves the list of products to a CSV file.
    
    Args:
    product_list (list): List of dictionaries containing product information.
    filename (str): Name of the CSV file to save (default: 'shoprite_products.csv').
    """
    if product_list:
        keys = product_list[0].keys()
        with open(filename, 'w', newline='', encoding='utf-8') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(product_list)
        print(f"Data has been saved to {filename}")
    else:
        print("No data to save.")

def main(url):
    """
    Main function to orchestrate the scraping process.
    
    Args:
    url (str): The URL of the Shoprite page to scrape.
    """
    # Scrape the website
    product_data = scrape_shoprite(url)

    # Save the data to a CSV file
    if product_data:
        save_to_csv(product_data)
    else:
        print("Failed to scrape data.")

    # Respect the crawl delay
    print("Waiting for 10 seconds before next request...")
    time.sleep(10)

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Scrape product information from Shoprite website.")
    parser.add_argument("url", help="URL of the Shoprite page to scrape")
    args = parser.parse_args()

    # Run the main function with the provided URL
    main(args.url)
