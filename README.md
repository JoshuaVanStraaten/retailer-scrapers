# Retailer Scraping Scripts

This repository contains Python scripts for web scraping product information from different e-commerce websites:
- Shoprite
- Pick n Pay (PnP).

## Scripts

1. `scrape_shoprite.py`: Scrapes product information from Shoprite's website.
2. `scrape_pnp.py`: Scrapes product information from Pick n Pay's website using JSON responses.

## Requirements

- Python 3.6+
- Required Python packages:
  - requests
  - requests-html
  - beautifulsoup4
  - pandas

You can install the required packages using pip:

```
pip install requests requests-html beautifulsoup4 pandas
```

## Usage

### scrape_shoprite.py

This script scrapes product information from a given Shoprite URL.

To run the script:

```
python scrape_shoprite.py <URL>
```

Replace `<URL>` with the Shoprite URL you want to scrape.

Example:
```
python scrape_shoprite.py "https://www.shoprite.co.za/c-2256/All-Departments?q=%3Arelevance%3AbrowseAllStoresFacetOff%3AbrowseAllStoresFacetOff&page=1"
```

#### How it works

1. The script sends a GET request to the provided URL.
2. It then uses BeautifulSoup to parse the HTML content.
3. Product information (name and price) is extracted from the parsed HTML.
4. The extracted data is saved to a CSV file named `shoprite_products.csv`.

#### Maintenance

- The CSS selectors used to find product elements may need to be updated if Shoprite changes their website structure.
- The list of user agents in the `get_random_user_agent()` function should be periodically updated to include newer browser versions.

#### Compliance

The script has been setup to comply with the website's `robots.txt` file. This is done by:
- Respecting the crawl delay of 10 seconds
- Only scraping during the allowed visit time (04:00-08:45 UTC)
- Not accessing disallowed paths or URLs with specific query parameters
- Using a custom User-Agent

The `robots.txt` file can be accessed [here](https://www.shoprite.co.za/robots.txt).

### scrape_pnp.py

This script scrapes product information from Pick n Pay's website using their JSON API.

To run the script:

```
python scrape_pnp.py [--timeout TIMEOUT]
```

The `--timeout` argument is optional and sets the time to wait between requests in case of errors. The default is 60 seconds.

Example:
```
python scrape_pnp.py --timeout 30
```

#### How it works

1. The script sends POST requests to PnP's API endpoint, simulating browser behavior.
2. It processes the JSON responses to extract product information.
3. The script iterates through multiple pages of results until it reaches the end or encounters duplicate data.
4. The extracted data is saved to a CSV file named `pnp_products.csv`.

#### Maintenance

- The headers and API endpoint URL may need to be updated if PnP changes their API structure.
- The `fields` parameter in the `request()` method may need adjustment if PnP modifies the available data fields.
- The logic for detecting the end of available products (currently commented out) should be reviewed and adjusted as needed.

#### Compliance

The script has been setup to comply with the website's `robots.txt` file. This is done by:
- Respecting the crawl delay of 10 seconds
- Only scraping during the allowed visit time
- Using a custom User-Agent

The `robots.txt` file can be accessed [here](https://www.pnp.co.za/robots.txt).

## Output

Both scripts generate CSV files containing the scraped product information:

- `scrape_shoprite.py` produces `shoprite_products.csv`
- `scrape_pnp.py` produces `pnp_products.csv`

These files will be created in the same directory as the scripts.

## Disclaimer

Web scraping may be against the terms of service of some websites. Ensure you have permission to scrape data from the target websites and use the scraped data responsibly. These scripts are for educational purposes only.

## Contributing

Feel free to fork this repository and submit pull requests with improvements or bug fixes. For major changes, please open an issue first to discuss what you would like to change.
