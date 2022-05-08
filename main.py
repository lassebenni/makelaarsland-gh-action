import os
from typing import List
import fire
from src.lib.utils import write_to_json

from src.makelaarsland.crawl import MakelaarslandCrawler
from src.walter.walterliving import WalterLiving


def crawl_makelaarsland(limit: int = 0, store: bool = True):
    username = os.getenv("MAKELAARSLAND_USERNAME")
    password = os.getenv("MAKELAARSLAND_PASSWORD")
    bucket = os.getenv("MAKELAARSLAND_BUCKET")

    crawler = MakelaarslandCrawler(username, password)
    listings: List[str] = crawler.crawl_listings(limit=limit)
    if store:
        crawler.store_listings_locally(listings)
        crawler.store_listings_in_s3(listings, bucket, "scraped/parquet")


def crawl_walterliving():
    proxy_url = os.getenv("PROXY_URL", "")
    walter = WalterLiving(proxy_url)
    walter.add_woz_values(csv_path="data/output/makelaarsland.csv")


if __name__ == "__main__":
    fire.Fire(
        {
            "makelaarsland": crawl_makelaarsland,
            "walterliving": crawl_walterliving,
        }
    )
