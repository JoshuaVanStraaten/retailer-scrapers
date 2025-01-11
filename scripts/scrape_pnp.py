import argparse
import requests
import json
import pandas as pd
from datetime import datetime, time
import pytz
from time import sleep
from urllib.parse import urlparse


SUPABASE_URL = "<supabase_url>"
SUPABASE_KEY = "<supabase_key>"


class Scraper:
    """
    A class to scrape product information from PnP website, complying with robots.txt rules.
    """

    def __init__(self, timeout, referer_url):
        """
        Initialize the Scraper with a timeout value and referer URL.

        Args:
        timeout (int): The number of seconds to wait between requests.
        referer_url (str): The referer URL to use in the request headers.
        """
        self.timeout = max(timeout, 10)  # Ensure we respect the 10-second crawl delay
        self.referer_url = referer_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'CustomBot/1.0 (+http://www.example.com/bot.html)'
        })

    def is_allowed_time(self):
        """
        Check if the current time is within the allowed visit time (04:00-08:45 UTC).

        Returns:
        bool: True if current time is within allowed visit time, False otherwise.
        """
        utc_now = datetime.now(pytz.UTC).time()
        start_time = time(4, 0)
        end_time = time(8, 45)
        return start_time <= utc_now <= end_time

    def request(self, page_number, max_retries=3):
        """
        Send a POST request to the website and return the JSON response.

        Args:
        page_number (int): The page number to request.

        Returns:
        dict: The JSON response from the server.
        """
        if not self.is_allowed_time():
            print("Current time is outside the allowed visit time (04:00-08:45 UTC). Exiting.")
            return None

        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'origin': 'https://www.pnp.co.za',
            'referer': self.referer_url,
            'sec-ch-ua': '"Not/A)Brand";v="99", "Google Chrome";v="126", "Chromium";v="126"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'x-anonymous-consents': '%5B%5D',
            'x-dtpc': '3$80169274_85h27vCHFAAUGAQAMTKHEKHUPPHFOMRGESLQAH-0e0',
            'x-dtreferer': 'https://www.pnp.co.za/c/pnpbase',
            'x-pnp-search-client-id': '74f9a705-b4b4-4d25-bfaa-89c0702c3e5e',
            'x-pnp-search-session-id': '14'
        }

        base_url = 'https://www.pnp.co.za/pnphybris/v2/pnp-spa/products/search'

        product_fields = [
            'sponsoredProduct', 'onlineSalesAdId', 'onlineSalesExtendedAdId', 'code', 'name',
            'brandSellerId', 'averageWeight', 'summary', 'price(FULL)', 'images(DEFAULT)',
            'stock(FULL)', 'averageRating', 'numberOfReviews', 'variantOptions', 'maxOrderQuantity',
            'productDisplayBadges(DEFAULT)', 'allowedQuantities(DEFAULT)', 'available',
            'defaultQuantityOfUom', 'inStockIndicator', 'defaultUnitOfMeasure',
            'potentialPromotions(FULL)', 'categoryNames'
        ]

        other_fields = [
            'facets', 'breadcrumbs', 'pagination(DEFAULT)', 'sorts(DEFAULT)', 'freeTextSearch',
            'currentQuery', 'responseJson', 'seoCategoryContent', 'seoCategoryTitle',
            'refinedContent', 'categoryDescription', 'keywordRedirectUrl'
        ]

        fields = f"products({','.join(product_fields)}),{','.join(other_fields)}"

        params = {
            'fields': fields,
            'query': ':relevance:allCategories:pnpbase',
            'pageSize': '72',
            'currentPage': f'{page_number}',
            'storeCode': 'WC44',
            'lang': 'en',
            'curr': 'ZAR'
        }

        print(f"Requesting page {page_number} of Pnp")
        retry_count = 0

        while retry_count < max_retries:
            try:
                response = self.session.post(base_url, params=params, headers=headers)

                if response.ok:
                    print(f"Response received. Status code: {response.status_code}")
                    return json.loads(response.text)
                else:
                    print(f"Request failed. Status code: {response.status_code}")
                    return None

            except Exception as e:
                retry_count += 1
                print(f"An error occurred: {str(e)}. Retry {retry_count}/{max_retries}")

                if retry_count >= max_retries:
                    print("Max retries reached. Request failed.")
                    return None

            finally:
                print(f"Waiting for {self.timeout} seconds before the next attempt...")
                sleep(self.timeout)


    def process(self, response):
        """
        Process the JSON response and extract relevant product information.

        Args:
        response (dict): The JSON response from the server.

        Returns:
        pandas.DataFrame: A DataFrame containing the extracted product information.
        """
        if not response:
            return pd.DataFrame()

        results = response.get('products', [])

        prod_lst = []

        for result in results:
            prod_dict = {
                'name': result.get('name'),
                'price': result.get('price', {}).get('formattedValue'),
                'promotion_price': self.get_promotion_message(result.get('potentialPromotions', [])),
                'retailer': "Pick n Pay",
                'image_url': next((item['url'] for item in result.get('images') if item['format'] == 'carousel'), None)
            }

            prod_lst.append(prod_dict)

        return pd.DataFrame(prod_lst)

    def get_promotion_message(self, promotions):
        """
        Extract the promotion message from the promotions data.

        Args:
        promotions (list or dict): The promotions data for a product.

        Returns:
        str: The promotion message or a default message if no promotion is available.
        """
        if not promotions:
            return 'No promotion available'

        if isinstance(promotions, list):
            promotion = promotions[0] if promotions else None
        else:
            promotion = promotions

        return promotion.get('promotionTextMessage', 'Promotion details not available') if promotion else 'No promo'


    def load_existing_data(self, csv_file):
        """
        Load existing product data from a CSV file and return it as a dictionary.

        This function attempts to read the specified CSV file using UTF-8 encoding.
        If UTF-8 fails, it falls back to 'latin1' encoding.
        It logs warnings for any rows with missing (NaN) values in the file and
        skips them in the returned data.

        Args:
            csv_file (str): The path to the CSV file to load.

        Returns:
            dict: A dictionary where keys are product names and values are dictionaries of
                  product details.
                Returns an empty dictionary if the file is not found or unreadable.
        """
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


    def upsert_to_supabase(self, data, batch_size=500):
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


    def run(self, filename='products_pnp.csv'):
        """
        Run the scraping process, collecting data from multiple pages.

        Args:
        filename (str): The name of the CSV file to save the results to.
        """
        page_number = 0

        # Load existing file or initialize from scratch
        try:
            existing_df = pd.read_csv(filename, index_col=0, encoding='utf-8')
            next_index = max(7500, existing_df.index.max() + 1)  # Start from 7500 if file exists
        except FileNotFoundError:
            print(f"Warning: File {filename} not found. Starting a new file.")
            next_index = 7500
        except UnicodeDecodeError:
            print(f"Warning: Failed to read {filename} with UTF-8 encoding. Trying 'latin1'.")
            try:
                existing_df = pd.read_csv(filename, index_col=0, encoding='latin1')
                next_index = max(7500, existing_df.index.max() + 1)
            except Exception as e:
                print(f"Error: Unable to read {filename} with fallback encoding. {e}")
                next_index = 7500

        while True:
            response = self.request(page_number)
            if not response:
                break

            response_df = self.process(response)
            if response_df.empty:
                break

            # Set the index for the new data
            response_df.index = range(next_index, next_index + len(response_df))
            response_df.index.name = 'index'

            # Save the processed data to the CSV file
            try:
                response_df.to_csv(
                    filename,
                    mode='a',
                    header=not pd.io.common.file_exists(filename),
                    encoding='utf-8'
                )
                print(f"Page {page_number} data successfully saved to {filename}.")
            except UnicodeEncodeError:
                print(f"Warning: Failed to save {filename} with UTF-8 encoding. Trying 'latin1'.")
                response_df.to_csv(
                    filename,
                    mode='a',
                    header=not pd.io.common.file_exists(filename),
                    encoding='latin1'
                )

            next_index += len(response_df)
            page_number += 1

            if page_number == 138:
                break

            # Uncomment the following block for production use
            # if page_number > 1:
            #     if response_df.equals(dfs[-2]):
            #         break

        # Load data from the updated CSV
        new_data = self.load_existing_data('products_pnp.csv')

        # Upsert the data to Supabase
        try:
            # Filter new_data to include only rows where 'retailer' == 'Pick n Pay'
            filtered_data = {name: details for name, details in new_data.items() if details.get('retailer') == 'Pick n Pay'}
            self.upsert_to_supabase(list(filtered_data.values()))
            print(f"Scraping complete. {len(filtered_data.values())} products scraped and saved to '{filename}'.")
        except Exception as e:
            print(f"Error during Supabase upsert: {e}")
        print("Scraping process complete.")


def main(timeout, referer_url):
    """
    Main function to create a Scraper instance and run the scraping process.

    Args:
    timeout (int): The timeout value to be passed to the Scraper.
    referer_url (str): The referer URL to use in the request headers.
    """
    scraper = Scraper(timeout, referer_url)
    scraper.run()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape product information from PnP website.")
    parser.add_argument("--timeout", type=int, default=10, help="Timeout between requests in seconds (default: 10 seconds)")
    parser.add_argument("--url", type=str, required=True, help="Referer URL to use in the request headers")
    args = parser.parse_args()

    main(args.timeout, args.url)
