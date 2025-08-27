import json
import re
from datetime import datetime
from urllib.parse import urlencode

import requests
from waste_collection_schedule import Collection  # type: ignore[attr-defined]

TITLE = "Pévèle Carembault (Publidata)"
DESCRIPTION = "Scrape du widget Publidata calendrier (fallback sans ICS ni API events)"
URL = "https://www.pevelecarembault.fr"
TEST_CASES = {
    # Exemple avec ID d’adresse direct (celui que tu as donné)
    "Adresse par ID": {"address_id": "59080_0360_00965"},
    # Exemple via texte + code INSEE
    "Texte + INSEE": {"q": "965 Rue", "citycode": "59080"},
}

WIDGET_INSTANCE_SLUG = "Ad7D6tp4LB"  # instance Widgets “Mes déchets”
WIDGET_BASE = f"https://widgets.publidata.io/{WIDGET_INSTANCE_SLUG}"

GEOCODER_BASE = "https://api.publidata.io/v2/geocoder"
SEARCH_BASE = "https://api.publidata.io/v2/search"


class Source:
    def __init__(self, address_id=None, q=None, citycode=None, timeout=20):
        """
        address_id: ex '59080_0360_00965' (recommandé si disponible)
        q: texte de recherche d'adresse (ex: '965 Rue')
        citycode: code INSEE de la commune (ex: '59080')
        """
        self.address_id = address_id
        self.q = q
        self.citycode = citycode
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": "waste_collection_schedule/pevele_publidata (+https://github.com/mampfes/hacs_waste_collection_schedule)",
                "Accept": "text/html,application/json",
            }
        )
        self.timeout = timeout

    # ------------- utils HTTP
    def _get(self, url, **kwargs):
        resp = self._session.get(url, timeout=self.timeout, **kwargs)
        resp.raise_for_status()
        return resp

    # ------------- résolution adresse
    def _resolve_address_id(self) -> str:
        if self.address_id:
            return self.address_id

        if not (self.q and self.citycode):
            raise ValueError("Fournir address_id OU (q + citycode)")

        # Géocoder par texte dans une commune
        params = {"q": self.q, "limit": 100, "lookup": "publidata", "citycode": self.citycode}
        r = self._get(f"{GEOCODER_BASE}", params=params).json()
        feats = r.get("features") or []
        if not feats:
            raise ValueError("Adresse introuvable avec le géocoder Publidata")
        # on prend le 1er résultat certifié si possible
        best = next((f for f in feats if f["properties"].get("certification")), feats[0])
        addr_id = best["properties"]["id"]
        if not addr_id:
            raise ValueError("Pas d'ID d'adresse dans la réponse Publidata")
        return addr_id

    # ------------- extraction du JSON Next.js
    def _extract_next_data(self, html: str) -> dict:
        # Cherche le script __NEXT_DATA__
        m = re.search(r"<script[^>]+id=\"__NEXT_DATA__\"[^>]*>(.*?)</script>", html, re.DOTALL | re.IGNORECASE)
        if not m:
            raise ValueError("JSON __NEXT_DATA__ introuvable dans la page calendrier")
        raw = m.group(1)
        return json.loads(raw)

    # ------------- détection des événements dans le state
    def _iter_events_from_state(self, data: dict):
        """
        Les structures peuvent varier; on balaie tout le JSON et on collecte
        les objets qui ressemblent à des événements (date + libellé).
        """
        iso_date = re.compile(r"^\d{4}-\d{2}-\d{2}")
        stack = [data]
        while stack:
            cur = stack.pop()
            if isinstance(cur, dict):
                # heuristique: {date: "YYYY-MM-DD", type/libelle/...}
                if "date" in cur and isinstance(cur["date"], str) and iso_date.match(cur["date"]):
                    # champ "types" / "label" / "name" selon les installations
                    label = (
                        cur.get("label")
                        or cur.get("name")
                        or cur.get("type")
                        or cur.get("wasteType")
                        or "Collecte"
                    )
                    yield cur["date"], str(label)
                stack.extend(cur.values())
            elif isinstance(cur, list):
                stack.extend(cur)

    def fetch(self):
        addr_id = self._resolve_address_id()

        # Appel page calendrier hydratée
        params = {"address_id": addr_id}
        html = self._get(f"{WIDGET_BASE}/calendar", params=params).text

        data = self._extract_next_data(html)

        # Itère et agrège par date -> [types]
        buckets = {}
        for d, label in self._iter_events_from_state(data):
            buckets.setdefault(d, set()).add(label)

        # Construit les Collection[]
        out = []
        for d, labels in sorted(buckets.items()):
            dt = datetime.strptime(d, "%Y-%m-%d").date()
            # titre compact: "Verre, Emballages"
            title = ", ".join(sorted(labels))
            out.append(Collection(date=dt, t=title))
        return out
