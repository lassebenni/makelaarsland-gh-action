from time import sleep
import json
import re
import requests
from retrying import retry

import pandas as pd
from bs4 import BeautifulSoup as bs

FUNDA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "nl,en-US;q=0.7,en;q=0.3",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
}


class WalterLiving:

    proxies = None

    def __init__(self, proxy_url):
        self.proxies = {
            "http": proxy_url,
            "https": proxy_url,
        }

    @retry(stop_max_attempt_number=3)
    def _fetch_funda_link(self, street: str, status: str = "available"):
        result = ""

        street_name = re.sub("\d.*", "", street).strip()
        street_name = re.sub(" ", "-", street_name).strip()

        if status == "available":
            url = f"https://www.funda.nl/koop/utrecht/straat-{street_name}/"
        elif status == "sold":
            url = f"https://www.funda.nl/koop/utrecht/straat-{street_name}/verkocht/sorteer-afmelddatum-af/"
        print(url)
        try:
            response = requests.request(
                "GET", url, headers=FUNDA_HEADERS, proxies=self.proxies
            )
            soup = bs(response.content, "html.parser")
            results = soup.find_all(class_="search-result")
            if results:
                url = results[0].find("a", href=True).get("href", "")

                street_lowercase = street.lower().replace(" ", "-")
                if "pijperlaan" not in url and street_lowercase in url:
                    result = f"funda.nl{url}"
        except Exception as e:
            print(e)

        return result

    @retry(stop_max_attempt_number=3)
    def _get_latest_woz(self, funda_url: str):
        url = "https://api.walterliving.com/hunter/lookup?"

        payload = json.dumps({"url": funda_url})
        headers = {"Content-Type": "application/json", "User-Agent": "Chrome"}

        try:
            response = requests.request(
                "POST", url, headers=headers, data=payload, proxies=self.proxies
            )
        except Exception as e:
            print(e)

        if response.status_code == 200:
            values = json.loads(response.text)["changes"]
            woz_values = list(filter(lambda x: x["status"] == "WOZ", values))

            if woz_values:
                return woz_values[0]
            else:
                return {}
        else:
            return {}

    def add_woz_values(self, csv_path: str) -> pd.DataFrame:
        df = pd.read_csv(csv_path)

        if len(df) == 0 or "streetname" not in df.columns:
            print("No streetnames found")
            return ""

        # only take rows that do not have a WOZ value yet
        if "woz_value" in df.columns:
            df = df[df["woz_value"] == ""]

        streets = list(df["streetname"])

        results = []
        for street in streets:
            url = self._fetch_funda_link(street, status="available")
            if url == "":
                sleep(1)
                url = self._fetch_funda_link(street, status="sold")
            if url != "":
                latest_woz = self._get_latest_woz(url)
                results.append(
                    dict(
                        streetname=street,
                        woz_value=str(latest_woz.get("price", "")),
                        woz_date=latest_woz.get("timestamp", ""),
                        funda_url=url,
                    )
                )

        if results:
            res = df.merge(pd.DataFrame.from_dict(results), on="streetname", how="left")
            return res
        else:
            df["woz_value"] = ""
            df["woz_date"] = ""
            df["funda_url"] = ""
            return df
