import time
from typing import Dict, List
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
from src.lib.aws import write_to_parquet
from src.makelaarsland.models.house_listing import HouseListing


class MakelaarslandCrawlerV2:
    BASE_URL = "https://www.makelaarsland.nl/huis-kopen/?_p="
    SUFFIX_URL = "&_q&min&max&verkocht=ja"
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

    def __init__(self):
        pass

    def crawl(self) -> List[HouseListing]:
        listings: List[Dict] = []
        first_page_soup = self.get_soup(self.BASE_URL + str(1) + self.SUFFIX_URL)
        last_page = self.get_last_page(first_page_soup)
        urls_for_first_page = self.fetch_listing_urls(first_page_soup)
        first_page_listings = self.scrape_listings(urls_for_first_page)
        listings.extend(first_page_listings)

        for page in range(2, int(last_page) + 1):
            print(f"Scraping page {page} of {last_page}")
            soup = self.get_soup(self.BASE_URL + str(page) + self.SUFFIX_URL)
            time.sleep(1)
            urls_for_page = self.fetch_listing_urls(soup)
            listings_for_page = self.scrape_listings(urls_for_page)
            listings.extend(listings_for_page)

        house_listings = [HouseListing(**listing) for listing in listings]
        return house_listings

    def get_soup(self, url: str) -> BeautifulSoup:
        html = requests.get(url).content
        return BeautifulSoup(html, "html.parser")

    def scrape_listing(self, url: str) -> Dict[str, str]:
        mapping = {}

        house_html = requests.get(url).content
        house_soup = BeautifulSoup(house_html, "html.parser")

        # details = house_soup.select('section.m-house-detail-specifications__section > div > h3')
        address = house_soup.select(
            "div.m-house-detail-data:nth-child(3) > h1:nth-child(1)"
        )[0].text

        # regex extract the last word in the string
        match = re.search(r"(.*)\b(\w+)\b(?=\s*$)", address)
        if match:
            streetname = match.groups()[0]
            mapping["streetname"] = streetname
            city = match.groups()[1]
            mapping["city"] = city

        mapping["brochure_url"] = url + "/brochure"

        # house_text = house_soup.find("div", {"id": "houseDetailDescription"}).text
        # mapping["house_text"] = house_text

        specs = house_soup.find("div", {"id": "houseDetailSpecifications"})
        specifications = specs.select("div.m-house-detail-specifications__row")
        for specification in specifications:
            label = specification.select("h3.m-house-detail-specifications__label")[
                0
            ].text.lower()
            value = specification.select("h4.m-house-detail-specifications__title")[
                0
            ].text

            mapping[label] = value

        details = house_soup.select(".m-house-detail-header__label-container")[
            0
        ].select("a")
        for detail in details:

            # check if detail contains the href attribute
            if detail.has_attr("href"):
                label = detail.text.strip().lower()
                value = detail["href"]

                mapping[label] = value

        mapping["url"] = url

        return mapping

    def get_last_page(self, soup: BeautifulSoup) -> int:
        pages = soup.select("a.page-numbers")
        return pages[-1].text.strip()

    def fetch_listing_urls(self, soup: BeautifulSoup) -> List[str]:
        listings_selector = soup.select("div.m-search-result > a")
        urls = [listing["href"] for listing in listings_selector]
        return urls

    def scrape_listings(self, urls: List[str]) -> List[Dict[str, str]]:
        listing_mappings = []

        for url in urls:
            listing_mapping = self.scrape_listing(url)
            listing_mappings.append(listing_mapping)

        return listing_mappings

    def store_house_listings(
        self, listings: List[HouseListing], bucket: str, path: str
    ):
        df = pd.DataFrame([listing.dict() for listing in listings])
        write_to_parquet(df, bucket, path, ["date"])

    # def write_mappings_to_file(self, listings, file_path: str):
    #     # Open the file for appending
    #     with open(file_path, "w") as f:
    #         # Create a CSV writer object
    #         keys = set()
    #         for d in listings:
    #             keys.update(d.keys())
    #         writer = csv.DictWriter(f, fieldnames=keys)
    #         # Write the headers
    #         writer.writeheader()

    #         for mapping in listings:
    #             # Write the values to the rows
    #             writer.writerow(mapping)
