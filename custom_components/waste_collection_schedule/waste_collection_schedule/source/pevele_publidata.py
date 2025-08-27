# -*- coding: utf-8 -*-
"""
Source: Pévèle Carembault via Publidata (WasteCollection)
Flux: (city_name|citycode) -> geocoder -> events
"""
from __future__ import annotations

import datetime as dt
from typing import Dict, List, Optional, Any, Sequence, Union

import requests
from waste_collection_schedule import Collection  # type: ignore[attr-defined]

TITLE = "Pévèle Carembault (Publidata)"
DESCRIPTION = "Collectes via API Publidata (recherche ville -> geocoder -> calendrier)."
URL = "https://www.pevelecarembault.fr/mon-quotidien/mes-dechets/calendrier-de-collecte"

# Endpoints Publidata génériques
CITY_SEARCH_URL = "https://api.publidata.io/v2/search"
GEOCODER_URL = "https://api.publidata.io/v2/geocoder"

# par défaut on devine l'endpoint events le plus commun; tu peux le surcharger
DEFAULT_EVENTS_URL = "https://api.publidata.io/v2/waste_collection/events"

# Chemins/Clés JSON (ajuste si besoin)
CITY_ARRAY_PATH: Sequence[Union[str, int]] = ["items"]
CITY_CODE_FIELD = "insee_code"

ADDR_ARRAY_PATH: Sequence[Union[str, int]] = ["features"]  # geocoder type=FeatureCollection
ADDR_ID_PATH: Sequence[Union[str, int]] = ["properties", "id"]

EVENTS_ARRAY_PATH: Sequence[Union[str, int]] = ["items"]
EVENT_DATE_FIELD = "date"
EVENT_TYPE_FIELD = "stream"

# Liste INSEE autorisée (depuis ta réponse next.publidata.io/api/instances/Ad7D6tp4LB/)
INSEE_WHITELIST_DEFAULT = [
    "59004","59022","59029","59034","59042","59071","59080","59096","59105","59123","59124",
    "59129","59145","59150","59158","59168","59197","59258","59266","59304","59330","59364",
    "59398","59408","59411","59419","59427","59435","59449","59452","59462","59466","59551",
    "59586","59592","59600","59630","59638"
]

TEST_CASES: Dict[str, Dict[str, str]] = {
    # Beuvry-la-Forêt (59080) – à adapter côté q / rue
    "BeuvryLaForet": {
        "citycode": "59080",
        "q": "965 Rue",  # ta requête geocoder
        # "events_url": "https://api.publidata.io/v2/…/events",  # à coller si différent
    },
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; WasteCollectionSchedule/1.0; +https://github.com/mampfes/hacs_waste_collection_schedule)"
}


def _dig(obj: Any, path: Sequence[Union[str, int]]) -> Any:
    cur = obj
    for key in path:
        if isinstance(key, int):
            cur = cur[key]
        else:
            if not isinstance(cur, dict):
                raise KeyError(f"JSON: niveau non-dict avant '{key}'")
            cur = cur.get(key)
        if cur is None:
            raise KeyError(f"JSON: clé/index manquant '{key}' pour chemin {path}")
    return cur


class Source:
    """
    Args YAML supportés:
      - citycode: str (ex "59080") OU
      - city_name: str (ex "Beuvry")  # si tu ne connais pas l'INSEE
      - insee_whitelist: list[str]    # sinon celle par défaut est utilisée
      - q: str                        # requête geocoder (ex "965 Rue")
      - events_url: str               # pour surcharger l'URL des events si différente du défaut
      - events_extra_params: dict     # pour ajouter dataset/from/to/instance/etc.
      - type_map: dict                # remap libellés bruts -> finaux
    """
    def __init__(
        self,
        citycode: Optional[str] = None,
        city_name: Optional[str] = None,
        insee_whitelist: Optional[List[str]] = None,
        q: Optional[str] = None,
        events_url: Optional[str] = None,
        events_extra_params: Optional[Dict[str, str]] = None,
        type_map: Optional[Dict[str, str]] = None,
        session: Optional[requests.Session] = None,
        tz: str = "Europe/Paris",
    ):
        self._citycode = citycode
        self._city_name = city_name
        self._insee_whitelist = insee_whitelist or INSEE_WHITELIST_DEFAULT
        self._q = (q or "").strip()
        self._events_url = (events_url or DEFAULT_EVENTS_URL).rstrip("/")
        self._events_extra_params = events_extra_params or {}
        self._type_map = type_map or {}
        self._tz = tz
        self._s = session or requests.Session()

    # --- étape 1: citycode (si non fourni)
    def _resolve_citycode(self) -> str:
        if self._citycode:
            return self._citycode
        if not self._city_name:
            raise ValueError("Fournis 'citycode' ou 'city_name'")

        params = {
            "size": 1000,
            "types[]": "city",
            "select[]": ["full_name", "postal_codes", "insee_code", "address_count"],
            "q": self._city_name,
        }
        # applique la whitelist INSEE (répétition de la même clé)
        for code in self._insee_whitelist:
            params.setdefault("insee_codes[]", [])
            params["insee_codes[]"].append(code)

        resp = self._s.get(CITY_SEARCH_URL, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        items = _dig(resp.json(), CITY_ARRAY_PATH)
        if not items:
            raise ValueError("Aucune ville trouvée")
        citycode = items[0].get(CITY_CODE_FIELD)
        if not citycode:
            raise ValueError("Champ 'insee_code' absent")
        return str(citycode)

    # --- étape 2: geocoder -> id adresse
    def _geocode(self, citycode: str) -> str:
        if not self._q:
            raise ValueError("Paramètre 'q' manquant pour le geocoder (ex: '965 Rue')")

        params = {
            "q": self._q,
            "limit": 10000,
            "lookup": "publidata",
            "citycode": citycode,
        }
        resp = self._s.get(GEOCODER_URL, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        arr = _dig(resp.json(), ADDR_ARRAY_PATH)
        if not arr:
            raise ValueError("Aucune adresse trouvée via le geocoder")

        # essaie features[0].properties.id, sinon items[0].id
        addr = arr[0]
        try:
            address_id = _dig(addr, ADDR_ID_PATH)
        except Exception:
            address_id = addr.get("id")

        if not address_id:
            raise ValueError("Identifiant d'adresse introuvable")
        return str(address_id)

    # --- étape 3: events
    def _fetch_events(self, address_id: str) -> List[dict]:
        params = {"address_id": address_id}
        params.update(self._events_extra_params)

        resp = self._s.get(self._events_url, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        events = _dig(resp.json(), EVENTS_ARRAY_PATH)
        if not isinstance(events, list):
            raise ValueError("Réponse 'events' inattendue (pas une liste)")
        return events

    def fetch(self) -> List[Collection]:
        citycode = self._resolve_citycode()
        address_id = self._geocode(citycode)
        events = self._fetch_events(address_id)

        out: List[Collection] = []
        for ev in events:
            raw_type = (ev.get(EVENT_TYPE_FIELD) or "").strip()
            t = self._type_map.get(raw_type, raw_type)

            date_str = ev.get(EVENT_DATE_FIELD)
            if not date_str:
                continue
            # normalise en date
            try:
                d = dt.date.fromisoformat(date_str[:10])
            except Exception:
                try:
                    d = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
                except Exception:
                    continue

            out.append(Collection(date=d, t=t))
        return out
