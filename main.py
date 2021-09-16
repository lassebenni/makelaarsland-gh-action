import os
import fire

from makelaarsland.crawl import MakelaarslandCrawler


def crawl_makelaarsland():
    username = os.getenv('MAKELAARSLAND_USERNAME', '')
    password = os.getenv('MAKELAARSLAND_PASSWORD', '')
    listing_id = os.getenv('MAKELAARSLAND_LISTING_ID', '')

    crawler = MakelaarslandCrawler(username, password, listing_id)
    crawler.crawl_listings()


def crawl_walterliving():
    pass


if __name__ == 'main':
    fire.Fire({
        'makelaarsland': crawl_makelaarsland,
        'walterliving': crawl_walterliving,
    })
