import os
from typing import List
import fire
from src.makelaarsland.models.house_listing import HouseListing
from src.makelaarsland.crawl_v2 import MakelaarslandCrawlerV2


def crawl_makelaarsland(full_run: bool = False):
    bucket = os.getenv("MAKELAARSLAND_BUCKET", "")

    crawler = MakelaarslandCrawlerV2(full_run)
    listings: List[HouseListing] = crawler.crawl()
    print(f"Found {len(listings)} listings")

    if full_run:
        crawler.store_house_listings(listings, bucket, "makelaarsland")


if __name__ == "__main__":
    fire.Fire(
        {
            "makelaarsland": crawl_makelaarsland,
        }
    )
