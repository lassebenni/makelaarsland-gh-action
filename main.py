import os

from makelaarsland.crawl import MakelaarslandCrawler

if __name__ == '__main__':
    username = os.getenv('MAKELAARSLAND_USERNAME', '')
    password = os.getenv('MAKELAARSLAND_PASSWORD', '')
    listing_id = os.getenv('MAKELAARSLAND_LISTING_ID', '')

    crawler = MakelaarslandCrawler(username, password, listing_id)
    crawler.crawl_listings()
