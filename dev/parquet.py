from datetime import datetime
import awswrangler as wr
from src.makelaarsland.makelaarsland import DUTCH_ENGLISH_ATTRIBUTES

COLS = [
    "date",
    "delivery",
    "price",
    "streetname",
    "url",
    "kvk_reserve_funds",
    "build_year",
    "location",
    "isolation",
    "kvk_monthly_payment",
    "appartment_type",
    "outside_size",
    "parking",
    "floors_amount",
    "warm_water",
    "floor",
    "living_size",
    "volume",
    "kvk_yearly_meeting",
    "heating",
    "amenities",
    "heating_type",
    "rooms_amount",
    "kvk_repair_plan",
    "city",
    "external_storage",
    "kvk_registration",
    "balcony_terrace",
    "bathroom_amenities",
    "roof",
    "status",
    "bathrooms_amount",
    "construction_type",
    "kvk_insurance",
    "postal_code",
]


# wr.s3.to_parquet(
#     df=df,
#     path="s3://makelaarsland-listings/scraped/parquet/2021.parquet",
#     compression="snappy",
# )


def json_to_parquet():
    files = wr.s3.list_objects("s3://makelaarsland-listings/scraped/parquet")

    for file in files:

        df = wr.s3.read_json(file)

        df = df.rename(DUTCH_ENGLISH_ATTRIBUTES, axis=1)
        df["price"] = df["price"].str.extract("(\d+)").astype(float)

        for col in [
            "other_inhoouse_spaces",
            "kvk_monthly_payment",
            "outside_size",
            "external_storage",
            "bathrooms_amount",
        ]:
            df[col] = df[col].str.extract("(\d+)").fillna(0).astype(int)

        df["postal_code"] = ""
        df["streetname"] = ""
        df["city"] = ""

        datestr = "-".join(file.split("/")[-1].split("_houses")[0].split("_")[:-1])
        df["date"] = datetime.strptime(datestr, "%Y-%m-%d-%H-%M-%S")

        df = df[COLS]

        df.to_parquet(
            path="s3://makelaarsland-listings/scraped/parquet/older2",
            compression="snappy",
            index=False,
            partition_cols=["date"],
        )


def fix_nans():
    files = wr.s3.list_objects("s3://makelaarsland-listings/scraped/parquet")

    for file in files:

        df = wr.s3.read_parquet(file)
        for col in df.columns:
            df[col] = df[col].fillna("").astype(str)
        df.to_parquet(file)


if __name__ == "__main__":
    fix_nans()
