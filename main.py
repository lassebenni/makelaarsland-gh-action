import os
import fire

from makelaarsland.crawl import MakelaarslandCrawler
from walter.walterliving import WalterLiving


def crawl_makelaarsland():
    username = os.getenv('MAKELAARSLAND_USERNAME', '')
    password = os.getenv('MAKELAARSLAND_PASSWORD', '')
    listing_id = os.getenv('MAKELAARSLAND_LISTING_ID', '')

    crawler = MakelaarslandCrawler(username, password, listing_id)
    crawler.crawl_listings()


def crawl_walterliving():
    proxy_url = os.getenv('PROXY_URL', '')
    walter = WalterLiving(proxy_url)
    walter.add_woz_values(csv_path='output/makelaarsland.csv')


if __name__ == "__main__":
    fire.Fire({
        'makelaarsland': crawl_makelaarsland,
        'walterliving': crawl_walterliving,
    })
