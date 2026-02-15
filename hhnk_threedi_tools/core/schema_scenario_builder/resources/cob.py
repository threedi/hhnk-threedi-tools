# %%
import csv
import json
from pathlib import Path

INPUT_CSV = Path("schematisation_settings.csv")
OUTPUT_JSON = Path("scenarios.json")


def convert_value(value: str):
    """Convert CSV string values to proper Python types."""
    if value == "":
        return None

    # Bool detection (only 0/1)
    if value in {"0", "1"}:
        return bool(int(value))

    # Int
    if value.isdigit():
        return int(value)

    # Float
    try:
        return float(value)
    except ValueError:
        pass

    return value


def csv_to_json():
    scenarios = []

    with INPUT_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")

        for row in reader:
            clean_row = {key: convert_value(value.strip()) for key, value in row.items()}
            scenarios.append(clean_row)

    output = {"scenarios": scenarios}

    with OUTPUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(output, f, indent=4)

    print(f"Written {OUTPUT_JSON}")


if __name__ == "__main__":
    csv_to_json()
