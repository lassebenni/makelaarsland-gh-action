from datetime import datetime
import os
import re
import time

from requests.adapters import HTTPAdapter
from retrying import retry
import requests

import boto3
from bs4 import BeautifulSoup
from makelaarsland.makelaarsland import Makelaarsland, Listing

BUCKET_NAME = os.environ.get('S3_BUCKET')
s3 = boto3.client('s3')

BASE_URL = "https://mijn.makelaarsland.nl"
FIRST_PAGE_LISTING_URL = f"{BASE_URL}/aanbod/kaart?id="
LOGIN_URL = f"{BASE_URL}/Inloggen"
SLEEP_TIMEOUT = 1

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:81.0) Gecko/20100101 Firefox/81.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'nl,en-US;q=0.7,en;q=0.3',
    'Origin': 'https://mijn.makelaarsland.nl',
    'Connection': 'keep-alive',
    'Referer': 'https://mijn.makelaarsland.nl/Inloggen',
    'Upgrade-Insecure-Requests': '1',
    'Content-Type': 'application/x-www-form-urlencoded',
}


def login():
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    response = session.get(LOGIN_URL, headers=HEADERS)
    soup = BeautifulSoup(response.content, "html.parser")
    # get '__RequestVerificationToken' value
    req_verif_token = soup.find_all(
        "input", {"name": "__RequestVerificationToken"})[0]['value']
    username = os.getenv('USERNAME', '')
    password = os.getenv('PASSWORD', '')

    payload = f'__RequestVerificationToken={req_verif_token}&MyAccount.Username={username}&MyAccount.Password={password}&RememberMe=false'

    # login
    session.post(LOGIN_URL, data=payload, headers=HEADERS)

    return session


@retry
def get_listings(session, listings_url):
    print(f'Scraping {listings_url}')
    listings_response = session.get(listings_url)
    listings_soup = BeautifulSoup(listings_response.content)
    return listings_soup


def get_house_urls(listing):
    all_houses = listing.find_all(class_='house-content')
    description_urls = [x.a['href'] for x in all_houses]
    return description_urls


@retry
def get_description(session, details_url):
    description_response = session.get(
        f"{BASE_URL}{details_url}")
    description_soup = BeautifulSoup(description_response.content)

    description_keys = description_soup.find_all(
        'div', class_='col-12 col-sm-4 grey')
    description_values = description_soup.find_all(
        'div', class_='col-12 col-sm-8 darkgrey')
    description_kv_pairs = {key.text.strip(): val.text.strip()
                            for key, val in zip(description_keys, description_values)}

    description_kv_pairs['streetname'] = description_soup.find_all(
        'div', class_='row justify-content-start mb-3')[0].text.strip()

    postal_code_str = description_soup.select(
        '.house-sale > div:nth-child(3) > div:nth-child(1) > p:nth-child(1)')[0].text.strip()
    description_kv_pairs['postal_code'] = re.search(
        '((\d{4} \w{2}) (\w.*))', postal_code_str, re.IGNORECASE)[2]
    description_kv_pairs['city'] = re.search(
        '((\d{4} \w{2}) (\w.*))', postal_code_str, re.IGNORECASE)[3]

    description_kv_pairs['url'] = f"{BASE_URL}{details_url}"
    description_kv_pairs['date'] = f"{datetime.today().strftime('%d-%m-%y')}"

    return description_kv_pairs


def extract_all_urls(search_result):
    def extract_last_page_number():
        # Find all pagination links in the search result to find other pages
        all_links = search_result.find('ul', 'pagination').find_all('li')
        # Keep only links with a 'href' attribute
        actual_links = [link.a['href']
                        for link in all_links if 'href' in link.a.attrs]
        # Regex-extract the page numbers from the links
        page_numbers = [int(re.search('(page=(\d{1,3}))', link)[
                            2]) for link in actual_links]
        # Last page number
        last_page_number = max(page_numbers)
        # Use this to create all urls

    extract_last_page_number()


def get_paginated_listing_urls(first_listing):
    pagination_list = first_listing.find_all('ul', {'class': 'pagination'})
    listing_urls = []
    for pagination in pagination_list:
        pagination_items = pagination.find_all('li')
        for page in pagination_items:
            listings_url = page.a.get('href')
            listing_urls.append(f"{BASE_URL}{listings_url}")

    unique_urls = list(set(listing_urls))
    page_urls = list(filter(lambda url: "page" in url, unique_urls))
    return page_urls


def get_house_descriptions():
    # login
    session = login()

    # get the urls for the first listing page
    listing_id = os.getenv('LISTING_ID', '')
    first_page_listing = get_listings(
        session, FIRST_PAGE_LISTING_URL + listing_id)

    not_logged_in = first_page_listing.find_all(text='Inloggen')
    if not_logged_in:
        print(f'Scrape failed. User not logged in.')
        exit(1)
    else:
        print("Successfully logged in.")

    # get the urls for the other listing pages
    house_description_urls = get_house_urls(first_page_listing)
    print(f'Retrieved {len(house_description_urls)} urls.')
    # get descriptions for all house_urls
    paginated_listing_urls = get_paginated_listing_urls(first_page_listing)

    # get house description URLS for all paginated listings
    for paginated_listing in paginated_listing_urls:
        listing = get_listings(session, paginated_listing)
        paginated_house_urls = get_house_urls(listing)
        # add URLS from first page to the URLS in the paginated other pages
        for url in paginated_house_urls:
            house_description_urls.append(url)

        print(f'Crawled listing {paginated_listing}')

    listings = []
    # now get descriptions for all URLS
    makelaarsland = Makelaarsland()
    house_description_urls = house_description_urls
    count = 0
    for url in house_description_urls:
        listing_dict = get_description(session, url)
        listing: Listing = makelaarsland.create_listing(
            listing=listing_dict, to_dict=True)
        if listing:
            listings.append(listing)
        time.sleep(SLEEP_TIMEOUT)
        print(
            f'Crawled description number {count}/{len(house_description_urls)}: {BASE_URL}{url}')
        count += 1

    return listings

if __name__ == '__main__':
    descriptions = get_house_descriptions()
    print("Finished crawling. Uploading to S3...")