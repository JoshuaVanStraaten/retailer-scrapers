import argparse
import requests
import json
import pandas as pd
from datetime import datetime, time
import pytz
from time import sleep
from urllib.parse import urlparse

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

    def request(self, page_number):
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

        print(f"Requesting page {page_number}")
        while True:
            try:
                response = self.session.post(base_url, params=params, headers=headers)

                if response.ok:
                    print(f"Response received. Status code: {response.status_code}")
                    return json.loads(response.text)
                else:
                    print(f"Request failed. Status code: {response.status_code}")
                    return None

            except Exception as e:
                print(f"An error occurred: {str(e)}")
                return None

            finally:
                print(f"Waiting for {self.timeout} seconds before next request...")
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
                'Promotion Price': self.get_promotion_message(result.get('potentialPromotions', [])),
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


    def run(self, filename='products.csv'):
        """
        Run the scraping process, collecting data from multiple pages.

        Args:
        filename (str): The name of the CSV file to save the results to.
        """
        page_number = 0
        dfs = []

        while True:
            response = self.request(page_number)
            if not response:
                break

            response_df = self.process(response)
            if response_df.empty:
                break

            dfs.append(response_df)
            page_number += 1

            # if page_number == 101:
            #     break

            # Uncomment the following block for production use
            # if len(dfs) > 2:
            #     if response_df.equals(dfs[-2]):
            #         break

        if dfs:
            df = pd.concat(dfs, ignore_index=True)

            # Load existing file or start from 0
            try:
                existing_df = pd.read_csv(filename, index_col=0)
                next_index = existing_df.index.max() + 1
            except FileNotFoundError:
                next_index = 0

            # Set index for new DataFrame
            df.index = range(next_index, next_index + len(df))
            df.index.name = 'index'

            # Save to the shared file
            df.to_csv(filename, mode='a', header=not pd.io.common.file_exists(filename))
            print(f"Scraping complete. {len(df)} products scraped and saved to '{filename}'.")
        else:
            print("No data was scraped.")


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
