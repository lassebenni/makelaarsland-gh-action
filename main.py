import os
import fire

from src.makelaarsland.crawl import MakelaarslandCrawler
from src.walter.walterliving import WalterLiving


def crawl_makelaarsland(limit: int = 0):
    username = os.getenv("MAKELAARSLAND_USERNAME", "")
    password = os.getenv("MAKELAARSLAND_PASSWORD", "")
    listing_id = os.getenv("MAKELAARSLAND_LISTING_ID", "")

    crawler = MakelaarslandCrawler(username, password, listing_id)
    crawler.crawl_listings(limit=limit)


def crawl_walterliving():
    proxy_url = os.getenv("PROXY_URL", "")
    walter = WalterLiving(proxy_url)
    walter.add_woz_values(csv_path="data/output/makelaarsland.csv")


if __name__ == "__main__":
    fire.Fire(
        {"makelaarsland": crawl_makelaarsland, "walterliving": crawl_walterliving,}
    )
