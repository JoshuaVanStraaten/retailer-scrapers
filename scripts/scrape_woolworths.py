import requests
import json
import pandas as pd
from datetime import datetime
from time import sleep as sleep
import os
from supabase import create_client


SUPABASE_URL = "<supabase_url>"
SUPABASE_KEY = "<supabase_key>"


# Create a scraper class
class Scraper:

    # Define the initialization function
    def __init__(self, params, category, code, last_index=0):
        self.timeout = params.get('timeout')
        self.category = category
        self.code = code
        self.last_index = last_index

    # Request data from a page number
    def request(self, page_number):

        # To iterate through categeories, change the URL:
        # - https://www.woolworths.co.za/cat/Food/<catgetory>/<code>

        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Referer': f'https://www.woolworths.co.za/cat/Food/{category}/_/N-{code}?No=48&Nrpp=24',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-GPC': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'X-Frame-Options': 'SAMEORIGIN',
            'X-Requested-By': 'Woolworths Online',
            'sec-ch-ua': '"Brave";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'x-dtpc': '9$201666412_28h16vVREQKDMKAHRSFAUHGCWIVNVFSJFRSVNG-0e0',
            'x-dtreferer': f'https://www.woolworths.co.za/cat/Food/{category}/_/N-{code}?No=24&Nrpp=24',
        }

        params = {
            'pageURL': f'/cat/Food/{category}/_/N-{code}',
            'No': f'{page_number * 24}',
            'Nrpp': '24',
        }

        # Keep the user updated with terminal window print outs
        print_update = lambda x: print(f'>>>> Time {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - Response Code: {x}', end='\r')
        print(f"Requesting page {page_number} of Woolies")

        # Add a while True loop that will break on a successful response
        while True:

            # Use a try except block to catch any exceptions
            try:
                response = requests.get('https://www.woolworths.co.za/server/searchCategory', params=params, headers=headers)

                # Response.ok is set to true if the response code is 200
                if response.ok:

                    # Print the response code to the terminal
                    print_update(response.status_code)

                    # Convert the response text to python dictionary
                    response = json.loads(response.text)

                    # Return the response will break the while loop
                    return response

                else:

                    # Print the response code to the terminal
                    print_update(response.status_code)

                    # Issue a sleep command for x seconds, settings in params
                    sleep(self.timeout)
            except:

                # Print the response code to the terminal
                print_update("ERROR")

                # Issue a sleep command for x seconds, settings in params
                sleep(self.timeout)

    # Process the response data from the server
    def process(self, response):

        results = response.get('contents')[0].get('mainContent')[0].get('contents')[0].get('records')

        count = response.get('contents')[0].get("secondaryContent")[0].get("categoryDimensions")[0].get("count")

        # Get the number of pages to scrape
        page_end = count // 24
        if count % 24 == 0:
            page_end -= 1

        products = []

        # Iterate through the results
        for result in results:

            # Extract the product details
            product = {
                'name': result.get('attributes').get('p_displayName'),
                'price': f'R{result.get('startingPrice').get('p_pl10')}',
                'promotion_price': result.get('attributes', {}).get('PROMOTION', 'No promo'),
                'retailer': "Woolworths",
                'image_url': result.get('attributes').get('p_externalImageReference'),
            }

            # Ensure there are no Nan values in the product details
            if product['name'] == 'FFF_Water_Content_Card_Wk43':
                continue

            # Append the product to the products list
            products.append(product)

        # Create a pandas dataframe from the products list
        df = pd.DataFrame(products)

        return df, page_end

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


    # Run function loops page numbers and calls request and process functions
    def run(self):

        # Set the starting page number
        page_number = 0

        # Create an empty list to store the data
        dfs = []

        # Increment through all the pages
        while True:

            # Get response from the server
            response = self.request(page_number)

            # Process the response and determine the total number of pages
            current_df, page_end = self.process(response)

            # Append the current page's DataFrame to the list of all pages
            dfs.append(current_df)

            # Concatenate all pages' data into a single DataFrame
            df = pd.concat(dfs, ignore_index=True)

            # Set the starting index to 29000 for the concatenated DataFrame
            self.last_index = self.last_index if self.last_index else 29000
            df.index = df.index + self.last_index
            df.index.name = 'index'

            # Apply the same index logic to the current DataFrame (`current_df`)
            # Calculate its starting index based on the overall last index
            current_start_index = self.last_index
            current_df.index = range(current_start_index, current_start_index + len(current_df))
            current_df.index.name = 'index'

            # Remove rows with NaN values - This can be removed once error handling logic is added
            current_df.dropna(inplace=True)

            # Check if the file exists
            file_exists = os.path.isfile('products_woolies.csv')

            # Save the current page's DataFrame (with the correct index) to the CSV file
            try:
                current_df.to_csv(
                    'products_woolies.csv',
                    mode='a' if file_exists else 'w',
                    header=not file_exists,  # Write header only if the file doesn't exist
                    encoding='utf-8'
                )
                print(f"Page {page_number} data successfully saved to products_woolies.csv.")
            except UnicodeEncodeError:
                print("UTF-8 encoding failed. Retrying with 'latin1'.")
                current_df.to_csv(
                    'products_woolies.csv',
                    mode='a' if file_exists else 'w',
                    header=not file_exists,
                    encoding='latin1'
                )

            # Increment the page number
            # Update the last index for the next run
            self.last_index += len(current_df)
            page_number += 1
            sleep(5)  # Optional: Add delay to avoid rate-limiting

            # Break the loop if all pages have been scraped
            if page_number > page_end:
                break

        # Load data from the updated CSV
        new_data = self.load_existing_data('products_woolies.csv')

        # Filter new_data to include only rows where 'retailer' == 'Woolworths'
        filtered_data = {name: details for name, details in new_data.items() if details.get('retailer') == 'Woolworths'}

        # Upsert the data to Supabase
        try:
            self.upsert_to_supabase(list(filtered_data.values()))
            print(f"Scraping complete. {len(filtered_data.values())} products scraped and saved to products_woolies.csv.")
        except Exception as e:
            print(f"Error during Supabase upsert: {e}")
        print("Scraping process complete.")


# Define a parameters dictionary to be passed to the class on construction
params = {'timeout': 60}

# Define a dictionary of product categories and related codes to scrape
categories = {
    'Fruit-Vegetables-Salads': 'lllnam',
    'Meat-Poultry-Fish': 'd87rb7',
    'Milk-Dairy-Eggs': '1sqo44p',
    'Ready-Meals': 's2csbp',
    'Deli-Entertaining': '13b8g51',
    'Food-To-Go': '11buko0',
    'Bakery': '1bm2new',
    'Frozen-Food': 'j8pkwq',
    'Pantry': '1lw4dzx',
    'Chocolates-Sweets-Snacks': '1yz1i0m',
    'Beverages-Juices': 'mnxddc',
    'Household': 'vvikef',
    'Cleaning': 'o1v4pe',
    'Toiletries-Health': '1q1wl1r',
    'Flowers-Plants': '1z13rv1',
    'Kids': 'ymaf0z',
    'Baby': '1rij75n',
    'Pets': 'l1demz',
}

# Create a new instance of the Scraper class
last_index = 0
for category, code in categories.items():
    scraper = Scraper(params, category, code, last_index)

    # Call the run function
    scraper.run()
    last_index = scraper.last_index


# TO-DO: Add image uploads to Supabase
# TO-DO: Add error handling logic for Nan values after load csv prior to upsert