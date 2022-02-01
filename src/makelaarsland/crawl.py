from datetime import datetime
from typing import List
from src.lib.utils import pandas_read
import re
import time
import json

import requests

from bs4 import BeautifulSoup
from src.makelaarsland.makelaarsland import Makelaarsland

CITY_REGEX = "(\d{4})\s(\w{2}\s)?(.*)"


class MakelaarslandCrawler:
    BASE_URL = "https://mijn.makelaarsland.nl"
    FIRST_PAGE_LISTING_URL = f"{BASE_URL}/aanbod/kaart?id="
    LOGIN_URL = f"{BASE_URL}/Inloggen"
    SLEEP_TIMEOUT = 1

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:81.0) Gecko/20100101 Firefox/81.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "nl,en-US;q=0.7,en;q=0.3",
        "Origin": "https://mijn.makelaarsland.nl",
        "Connection": "keep-alive",
        "Referer": "https://mijn.makelaarsland.nl/Inloggen",
        "Upgrade-Insecure-Requests": "1",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    def __init__(self, username: str, password: str, listing_id: str):
        self.listing_id = listing_id
        self.session = self.login(username, password)
        self.makelaarsland = Makelaarsland()

    def login(self, username, password) -> requests.Session:
        """Login to makelaarsland.nl and return a Session object.

        Args:
            username (str)
            password (str)

        Returns:
            requests.Session: [Request Session used for connecting to the website.]
        """
        session = requests.Session()

        response = session.get(self.LOGIN_URL, headers=self.HEADERS)
        soup = BeautifulSoup(response.content, "html.parser")
        # get '__RequestVerificationToken' value
        req_verif_token = soup.find_all(
            "input", {"name": "__RequestVerificationToken"}
        )[0]["value"]

        payload = f"__RequestVerificationToken={req_verif_token}&MyAccount.Username={username}&MyAccount.Password={password}&RememberMe=false"

        # login
        session.post(self.LOGIN_URL, data=payload, headers=self.HEADERS)

        return session

    def _get_listings_soup(self, listings_url) -> BeautifulSoup:
        """Return a BeautifulSoup object of the makelaarsland listings page.

        Args:
            listings_url ([type]): [description]

        Returns:
            BeautifulSoup: [description]
        """
        print(f"Scraping {listings_url}")
        listings_response = self.session.get(listings_url)
        if listings_response and listings_response.content:
            listings_soup = BeautifulSoup(
                listings_response.content, features="html.parser"
            )
            return listings_soup
        else:
            raise Exception("Could not get listings soup")

    def _get_house_urls(self, listing: BeautifulSoup) -> List[str]:
        """Return all urls for the description of the house in the BeautifulSoup object.

        Args:
            listing ([Beautifulsoup]): [Contains the listings of the page]

        Returns:
            List[str]: [URLS of the houses]
        """
        all_houses = listing.find_all(class_="house-content")
        description_urls = [x.a["href"] for x in all_houses]
        return description_urls

    def _get_description(self, details_url) -> dict:
        """Extract the description from the house details URL.

        Args:
            details_url ([str]): [House description URL]

        Returns:
            dict: [Dictionary containing the description of the house]
        """
        description_response = self.session.get(f"{self.BASE_URL}{details_url}")
        description_soup = BeautifulSoup(description_response.content)

        if "Er is iets fout gegaan" in description_soup.text:
            print(f"Scrape failed for {details_url}")
            return {}

        description_keys = description_soup.find_all(
            "div", class_="col-12 col-sm-4 grey"
        )
        description_values = description_soup.find_all(
            "div", class_="col-12 col-sm-8 darkgrey"
        )
        description_kv_pairs = {
            key.text.strip(): val.text.strip()
            for key, val in zip(description_keys, description_values)
        }

        description_kv_pairs["streetname"] = description_soup.find_all(
            "div", class_="row justify-content-start mb-3"
        )[0].text.strip()

        postal_code_str = description_soup.select(
            ".house-sale > div:nth-child(4) > div:nth-child(1) > p:nth-child(1)"
        )[0].text.strip()

        postal_code_match = re.search(CITY_REGEX, postal_code_str, re.IGNORECASE)
        if postal_code_match:
            postal_code_digits = postal_code_match[1]
            postal_code_letters = postal_code_match[2]

            description_kv_pairs[
                "postal_code"
            ] = f"{postal_code_digits} {postal_code_letters}"

            description_kv_pairs["city"] = postal_code_match[3]

        description_kv_pairs["url"] = f"{self.BASE_URL}{details_url}"
        description_kv_pairs["date"] = f"{datetime.today().strftime('%d-%m-%y')}"

        return description_kv_pairs

    def _get_paginated_listing_urls(self, first_listing) -> List[str]:
        """Using the first listing page, find total number of listing pages, including paginated ones.

        Args:
            first_listing ([Beautifulsoup]): [BeautifulSoup object of the first listing page]

        Returns:
            List[str]: [List of all page URLs]
        """
        pagination_list = first_listing.find_all("ul", {"class": "pagination"})
        listing_urls = []
        for pagination in pagination_list:
            pagination_items = pagination.find_all("li")
            for page in pagination_items:
                listings_url = page.a.get("href")
                listing_urls.append(f"{self.BASE_URL}{listings_url}")

        unique_urls = list(set(listing_urls))
        page_urls = list(filter(lambda url: "page" in url, unique_urls))
        return page_urls

    def _check_logged_in(self, first_page_listing: BeautifulSoup):
        not_logged_in = first_page_listing.find_all(text="Inloggen")
        if not_logged_in:
            print(f"Scrape failed. User not logged in.")
            exit(1)
        else:
            print("Successfully logged in.")

    def _get_house_descriptions(self, limit: int = 0):
        """Retrieve the descriptions for the houses in the listing ID.

        Args:
            limit ([int]): [Limit the number of crawled houses]

        Returns:
            [type]: [description]
        """
        first_page_listing = self._get_listings_soup(
            self.FIRST_PAGE_LISTING_URL + self.listing_id
        )

        self._check_logged_in(first_page_listing)

        # only take the limit of house urls to crawl
        print(f"limit is={limit}")
        if limit > 0:
            house_urls = self._get_house_urls(first_page_listing)
            if house_urls:
                house_urls = house_urls[:limit]
        else:
            house_urls = self._get_paginated_urls(first_page_listing)

        print(f"Crawled listing {house_urls}")

        return self._crawl_house_descriptions(house_urls)

    def _get_paginated_urls(self, first_page_listing: List[str]) -> List[str]:
        urls = []

        # add the house description urls from the first page
        for url in self._get_house_urls(first_page_listing):
            urls.append(url)

        paginated_listing_urls = self._get_paginated_listing_urls(first_page_listing)

        # add house description urls for the other pages
        for paginated_listing in paginated_listing_urls:
            listings_soup = self._get_listings_soup(paginated_listing)
            for url in self._get_house_urls(listings_soup):
                urls.append(url)

        return urls

    def _crawl_house_descriptions(self, urls: List[str]) -> List[str]:
        """Crawl the descriptions of the houses and create Listing objects.

        Args:
            urls List[str]: List of URLs for the houses to crawl

        Returns:
            List[str]: List of MakelaarslandListing objects
        """
        listings = []
        count = 0

        for url in urls:
            if "woning-details" not in url:
                print(f"Unknown URL {url}.. skipping.")
                continue

            listing_dict = self._get_description(url)
            listing = self.makelaarsland.create_listing(
                listing=listing_dict, to_dict=True
            )
            if listing:
                listings.append(listing)
            time.sleep(self.SLEEP_TIMEOUT)
            print(
                f"Crawled description number {count}/{len(urls)}: {self.BASE_URL}{url}"
            )
            count += 1

        return listings

    def _combine_results(self, input_dir: str, output_dir: str):
        """Combine the files in the input_dir to a single result in the output_dir.

        Args:
            input_dir (str): [Directory with the input files]
            output_dir (str): [Directory for the output file]
        """
        df = pandas_read(input_dir, "json")
        df = df.drop_duplicates(subset=["streetname"])
        df.reset_index(drop=True, inplace=True)

        df.to_json(f"{output_dir}/makelaarsland.json", orient="records")
        df.to_csv(f"{output_dir}/makelaarsland.csv")

    def crawl_listings(
        self,
        input_dir: str = "data/scraped",
        output_dir: str = "data/output",
        limit: int = 0,
    ):
        """Crawls the makelaarsland.nl website and creates a json file with the house descriptions.

        Args:
            input_dir (str, optional): Location for the scraped files. Defaults to 'data/scraped'.
            output_dir (str, optional): Location for the output file. Defaults to 'data/output'.
            limit (int, optional): Throttling of the amount of houses to crawl. Defaults to 0.
        """
        descriptions = self._get_house_descriptions(limit)

        print(f"Finished crawling. Storing as JSON into dir: {input_dir}.")
        current_date = datetime.today().strftime("%d-%m-%y")

        with open(f"{input_dir}/{current_date}.json", "w") as f:
            json.dump(descriptions, f)

        self._combine_results(input_dir, output_dir)
