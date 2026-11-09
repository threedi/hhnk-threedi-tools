from enum import Enum
from pathlib import Path


# %%
class metadata_type(Enum):
    PRIMAIRE = "primaire_kering"
    REGIONAAL = "regionale_kering"
    STRESSTEST = "stresstest"


metadata_template: dict[str, Path] = {
    metadata_type.PRIMAIRE.value: Path(__file__).parent
    / "metadata_template"
    / f"metadata_template_{metadata_type.PRIMAIRE.value}.xlsx",
    metadata_type.REGIONAAL.value: Path(__file__).parent
    / "metadata_template"
    / f"metadata_template_{metadata_type.REGIONAAL.value}.xlsx",
    metadata_type.STRESSTEST.value: Path(__file__).parent
    / "metadata_template"
    / f"metadata_template_{metadata_type.STRESSTEST.value}.xlsx",
}

columns_metadata_primaire: dict[str, str | int] = {
    # "Naam buitenwater": "Boezemwater",
    "Scenariotype": "B",
    "Projectnaam": "Overstromingsberekeningen primaire doorbraken 2024.",
    "Versie resultaat": 1,
    "Varianttype": "Bres",
    "Motivatie rekenmethode": "Actualisatie maaiveldmodel, berekening mogelijk op hoge resolutie. Boezemsysteem in 1D gemodelleerd t.b.v. verspreiding regionaal systeem."
    "Boezemsysteem in 1D gemodelleerd t.b.v. verspreiding regionaal systeem.",
    # "Overschrijdingsfrequentie": f"{{variable}}",
    "Doel": "Actualisatie aanlevering ROR.",
    "Beschrijving scenario": "Doorbraak primaire waterkering.",
    "Compartimentering van de boezem": "nee",
    "Gebiedsnaam": "gebieden beschermd door genormeerde regionale keringen, langs rivieren, meren, kanalen en boezemwateren",
}


columns_metadata_regionaal: dict[str, str | int] = {
    "Naam buitenwater": "Boezemwater",
    "Scenariotype": "C",
    "Projectnaam": "Normering regionale keringen IPO voor toetsronde 2024.",
    "Versie resultaat": 1,
    "Varianttype": "Bres",
    "Motivatie rekenmethode": "Actualisatie maaiveldmodel, berekening mogelijk op hoge resolutie. "
    "Boezemsysteem in 1D gemodelleerd t.b.v. verspreiding regionaal systeem.",
    "Overschrijdingsfrequentie": 1000,
    "Doel": "Berekenen van schade en risico bij regionale keringen",
    "Beschrijving scenario": "Referentie berekening doorbraak boezemkade.",
    "Compartimentering van de boezem": "nee",
    "Gebiedsnaam": "gebieden beschermd door genormeerde regionale keringen, langs rivieren, meren, kanalen en boezemwateren",
}

COLUMNS_NAMES: dict[metadata_type, dict[str, str | int]] = {
    metadata_type.REGIONAAL: columns_metadata_regionaal,
    metadata_type.PRIMAIRE: columns_metadata_primaire,
}


# %%
