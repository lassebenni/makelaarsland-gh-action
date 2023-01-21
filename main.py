import os
from typing import List
import fire
from src.makelaarsland.models.house_listing import HouseListing
from src.makelaarsland.crawl_v2 import MakelaarslandCrawlerV2


def crawl_makelaarsland(full_run: bool = False):
    bucket = os.getenv("MAKELAARSLAND_BUCKET", "")

    crawler = MakelaarslandCrawlerV2(full_run)
    listings: List[HouseListing] = crawler.crawl()

    if full_run:
        crawler.store_house_listings(listings, bucket, "makelaarsland")


# def crawl_walterliving():
#     proxy_url = os.getenv("PROXY_URL", "")
#     walter = WalterLiving(proxy_url)
#     walter.add_woz_values(csv_path="data/output/makelaarsland.csv")


if __name__ == "__main__":
    fire.Fire(
        {
            "makelaarsland": crawl_makelaarsland,
            # "walterliving": crawl_walterliving,
        }
    )
