from collections import namedtuple
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import Dict, Optional
import json
import logging
import re

import desert
import marshmallow

LOGGER_BASENAME = '''makelaarsland'''
LOGGER = logging.getLogger(LOGGER_BASENAME)
LOGGER.addHandler(logging.NullHandler())

DUTCH_ENGLISH_ATTRIBUTES = {
    'Aantal badkamers': 'bathrooms_amount',
    'Aantal kamers': 'rooms_amount',
    'Aantal woonlagen': 'floors_amount',
    'Aanvaarding': 'delivery',
    'Achtertuin': 'back_yard',
    'Badkamer voorzieningen': 'bathroom_amenities',
    'Balkon/dakterras': 'balcony_terrace',
    'Bouwjaar': 'build_year',
    'Bouwperiode': 'build_period',
    'Capaciteit': 'capacity',
    'city': 'city',
    'Cv-ketel': 'heating_type',
    'date': 'date',
    'Energieklasse': 'energy_class',
    'Externe bergruimte': 'external_storage',
    'Gebouwgebonden buitenruimte': 'outside_size',
    'Gelegen op': 'floor',
    'Inhoud': 'volume',
    'Inschrijving KvK': 'kvk_registration',
    'Isolatie': 'isolation',
    'Jaarlijkse vergadering': 'kvk_yearly_meeting',
    'Keurmerken': 'brands',
    'Ligging tuin': 'garden_location',
    'Ligging': 'location',
    'Onderhoudsplan': 'kvk_repair_plan',
    'Opstalverzekering': 'kvk_insurance',
    'Overige inpandige ruimte': 'other_inhoouse_spaces',
    'Perceeloppervlakte': 'plot_area',
    'Periodieke bijdrage': 'kvk_monthly_payment',
    'postal_code': 'postal_code',
    'Reservefonds aanwezig': 'kvk_reserve_funds',
    'Schuur/berging': 'storage',
    'Soort appartement': 'appartment_type',
    'Soort bouw': 'construction_type',
    'Soort dak': 'roof',
    'Soort garage': 'garage_type',
    'Soort parkeergelegenheid': 'parking',
    'Soort woonhuis': 'house_type',
    'Status': 'status',
    'streetname': 'streetname',
    'Tuin': 'garden',
    'url': 'url',
    'Verwarming': 'heating',
    'Voortuin': 'front_garden',
    'Voorzieningen': 'amenities',
    'Vraagprijs': 'price',
    'Warm water': 'warm_water',
    'Woonoppervlakte': 'living_size',
    'Zonneterras': 'sun_terrace',
}

TranslatedPair = namedtuple('TranslatedPair', 'dutch english')
TRANSLATED_PAIRS = [TranslatedPair(*pair)
                    for pair in list(DUTCH_ENGLISH_ATTRIBUTES.items())]


@dataclass_json
@dataclass
class Listing:
    """Models a Makelaarsland Listing."""
    city: str
    date: str
    postal_code: str
    price: str
    streetname: str
    url: str

    amenities: Optional[str]
    appartment_type: Optional[str]
    balcony_terrace: Optional[str]
    bathroom_amenities: Optional[str]
    bathrooms_amount: Optional[str]
    build_year: Optional[str]
    construction_type: Optional[str]
    delivery: Optional[str]
    external_storage: Optional[str]
    floor: Optional[str]
    floors_amount: Optional[str]
    heating_type: Optional[str]
    heating: Optional[str]
    isolation: Optional[str]
    kvk_insurance: Optional[str]
    kvk_monthly_payment: Optional[str]
    kvk_registration: Optional[str]
    kvk_repair_plan: Optional[str]
    kvk_reserve_funds: Optional[str]
    kvk_yearly_meeting: Optional[str]
    living_size: Optional[str]
    location: Optional[str]
    outside_size: Optional[str]
    parking: Optional[str]
    roof: Optional[str]
    rooms_amount: Optional[str]
    status: Optional[str]
    volume: Optional[str]
    warm_water: Optional[str]

    def __post_init__(self):
        self.convert_attributes()

    def to_json(self):
        """Returns the Listing as a json object.
        """
        return json.dumps(self.dict())

    def convert_attributes(self):
        if (match := re.search(r"(\d{1,9}).(\d{1,9})", self.price)):
            self.price = match[1] + match[2]

        def extract_number(attribute: str):
            if attribute:
                if (match := re.search('\d+', attribute, re.IGNORECASE)):
                    return match.group(0)
            else:
                return ''

        self.outside_size = extract_number(self.outside_size)
        self.living_size = extract_number(self.living_size)
        self.volume = extract_number(self.volume)
        self.floors_amount = extract_number(self.floors_amount)
        self.bathrooms_amount = extract_number(self.bathrooms_amount)
        self.kvk_monthly_payment = extract_number(self.kvk_monthly_payment)
        self.rooms_amount = extract_number(self.rooms_amount)
        self.external_storage = extract_number(self.external_storage)


class Makelaarsland:
    schema = desert.schema(Listing, meta={"unknown": marshmallow.EXCLUDE})

    def __init__(self):
        logger_name = u'{base}.{suffix}'.format(
            base=LOGGER_BASENAME, suffix=self.__class__.__name__)
        self._logger = logging.getLogger(logger_name)

    def create_listing(self, listing: Dict, to_dict: bool = True) -> Optional[Listing]:
        translated_listing = {}
        for pair in TRANSLATED_PAIRS:
            translated_listing[pair.english] = listing.get(pair.dutch)

        if not translated_listing.get('appartment_type', None):
            return None

        result: Listing = self.schema.load(translated_listing)
        self._logger.info('Listing created.')

        if to_dict:
            result = result.to_dict()

        return result
