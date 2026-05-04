# validationrules.json — Reference Guide

This document describes the structure and usage of `validationrules.json`, the central
configuration file that drives all validation and fix behaviour in the HyDAMO fixer pipeline.

---

## Table of contents

1. [File location and schema](#1-file-location-and-schema)
2. [Top-level structure](#2-top-level-structure)
3. [Object entry structure](#3-object-entry-structure)
4. [general_rules](#4-general_rules)
5. [validation_rules](#5-validation_rules)
6. [fix_rules](#6-fix_rules)
7. [Filters](#7-filters)
8. [Function reference](#8-function-reference)
9. [How inputs are interpreted](#9-how-inputs-are-interpreted)
10. [custom_hydamo — calling custom functions](#10-custom_hydamo--calling-custom-functions)
11. [CSV overviews](#11-csv-overviews)

---

## 1. File location and schema

| Item | Path |
|---|---|
| Configuration file | `hhnk_threedi_tools/resources/schematisation_builder/validationrules.json` |
| Schema (installed package) | `hydamo_validation/schemas/rules/rules_1.5_fixes.json` |

The file is validated against `rules_1.5_fixes.json` at runtime (loaded from the installed
`hydamo_validation` package). The current schema version is `"1.5_fixes"`. Breaking this
schema causes the fixer pipeline to reject the file on load.

Note: the `hhnk_threedi_tools/resources/schematisation_builder/schemas/` folder contains
local copies of older schema files that are **not used at runtime** and can be ignored.

---

## 2. Top-level structure

```json
{
    "schema": "1.5_fixes",
    "hydamo_version": "2.4",
    "status_object": ["planvorming", "gerealiseerd"],
    "objects": [ ... ]
}
```

| Key | Type | Required | Description |
|---|---|---|---|
| `schema` | string | yes | Schema version. Current version with fixes is `"1.5_fixes"`. |
| `hydamo_version` | string | yes | HyDAMO data model version. One of `"2.2"`, `"2.3"`, `"2.4"`. |
| `status_object` | array of strings | no | Status values from the HyDAMO data model that are considered *in scope* for validation. Objects with a `status` not in this list are skipped. |
| `objects` | array | yes | List of per-layer rule sets. Each entry covers one HyDAMO object layer. |

---

## 3. Object entry structure

Each element of `objects` has the following shape:

```json
{
    "object": "duikersifonhevel",
    "general_rules": [ ... ],
    "general_rules_temp_excluded": [ ... ],
    "validation_rules": [ ... ],
    "fix_rules": [ ... ]
}
```

| Key | Required | Description |
|---|---|---|
| `object` | yes | Name of the HyDAMO layer. Must be one of the allowed values listed below. |
| `general_rules` | no | Pre-processing rules that derive helper columns used by validation rules. |

### Allowed values for `object`

The following HyDAMO object names are accepted (defined in the schema enum):

`admingrenswaterschap`, `afsluitmiddel`, `afvoeraanvoergebied`, `afvoergebiedaanvoergebied`,
`aquaduct`, `beheergrenswaterschap`, `bijzonderhydraulischobject`, `bodemval`, `brug`,
`doorstroomopening`, `duikersifonhevel`, `gemaal`, `grondwaterinfolijn`, `grondwaterinfopunt`,
`grondwaterkoppellijn`, `grondwaterkoppelpunt`, `hydrologischerandvoorwaarde`, `hydroobject`,
`hydroobject_normgp`, `imwa_geoobject`, `kunstwerkopening`, `lateraleknoop`,
`leggerwatersysteem`, `leggerwaterveiligheid`, `meetlocatie`, `meetwaardeactiewaarde`,
`normgeparamprofiel`, `normgeparamprofielwaarde`, `peilafwijkinggebied`, `peilbesluitgebied`,
`peilgebiedpraktijk`, `peilgebiedvigerend`, `pomp`, `profielgroep`, `profiellijn`,
`profielpunt`, `regelmiddel`, `reglementgrenswaterschap`, `streefpeil`, `sturing`, `stuw`,
`vispassage`, `vispassagevlak`, `vuilvang`, `waterbeheergebied`, `zandvang`
| `general_rules_temp_excluded` | no | Same format as `general_rules`; rules here are silently skipped. Used to disable rules temporarily without deleting them. |
| `validation_rules` | yes | Rules that evaluate correctness of data and flag invalid rows. |
| `fix_rules` | no | Rules that describe how to correct rows flagged as invalid. |

---

## 4. general_rules

General rules compute *intermediate attributes* (helper columns) on a layer's GeoDataFrame
before validation checks run. They do not produce pass/fail results; they produce numeric or
categorical Series that validation rules can reference.

### Required fields

| Field | Type | Description |
|---|---|---|
| `id` | integer | Unique identifier within the object. IDs 0–99 are treated as `hwh` (base rule set); IDs ≥ 100 are treated as `hhnk` (custom rule set). |
| `result_variable` | string | Name of the column that is added to the GeoDataFrame. Must match `^[A-Za-z_][A-Za-z0-9_]*$`. |
| `function` | object | The function to apply (see [Function reference](#8-function-reference)). |

### Optional fields

| Field | Type | Description |
|---|---|---|
| `description` | string | Human-readable explanation of what the rule computes. |

### Example

```json
{
    "id": 0,
    "result_variable": "bodemhoogte",
    "function": {
        "object_relation": {
            "related_object": "profielpunt",
            "statistic": "min",
            "related_parameter": "geometry.z"
        }
    },
    "description": "Minimale hoogte van profielpunten behorend bij de profiellijn"
}
```

**Source:** General rules are executed by `_process_general_function` in
`hydamo_validation.logical_validation`. The functions themselves live in
`hydamo_validation.functions.general`.

---

## 5. validation_rules

Validation rules check whether attribute values satisfy a condition and flag rows that do not.
Each rule produces a boolean Series stored in the layer's validation result.

### Required fields

| Field | Type | Description |
|---|---|---|
| `id` | integer | Unique identifier within the object. IDs ≥ 100 are treated as `hhnk`-specific rules. |
| `name` | string | Short descriptive name for the rule, shown in reports. |
| `type` | string | `"logic"` for attribute-level checks; `"topologic"` for geometry/topology checks. |
| `error_type` | string | `"critical"` or `"non-critical"`. Critical errors are treated more severely in summaries. |
| `result_variable` | string | Column name written to the validation result GeoDataFrame. |
| `error_message` | string | Human-readable message shown when a row fails this rule. |
| `active` | boolean | Set to `false` to disable a rule without removing it. Only `active: true` rules appear in the generated CSV overview. |
| `function` | object | The check function to apply (see [Function reference](#8-function-reference)). |

### Optional fields

| Field | Type | Description |
|---|---|---|
| `description` | string | Longer explanation of the rule. |
| `validation_rule_set` | string | Arbitrary label (e.g. `"basic"`, `"hhnk"`). Rules with `validation_rule_set == "hhnk"` or `id >= 100` are tagged as `bron = hhnk` in the CSV overview. |
| `filter` | object | A filter expression (see [Filters](#7-filters)). The rule is only applied to rows that satisfy the filter; other rows pass automatically. |
| `exceptions` | array of strings | `globalid` values that are always excluded from this rule regardless of their attribute values. |
| `penalty` | integer (0–9) | Optional severity weighting used in scoring. |
| `tags` | array of strings | Free-form tags for grouping or filtering rules externally. |

### Example

```json
{
    "id": 102,
    "name": "DiepteNatProfiel is plausibel (0.2m - 20m)",
    "type": "logic",
    "validation_rule_set": "hhnk",
    "error_type": "critical",
    "result_variable": "dieptenatprofiel_plausibel",
    "error_message": "diepteNatProfiel niet plausibel (0.2m - 20m)",
    "active": true,
    "function": {
        "BE": {
            "parameter": "dieptenatprofiel",
            "min": 0.2,
            "max": 20,
            "inclusive": false
        }
    }
}
```

**Source:** Logic functions are dispatched by `_process_logic_function` and topologic functions
by `_process_topologic_function`, both in `hydamo_validation.logical_validation`.
The logic functions themselves live in `hydamo_validation.functions.logic` and the topologic
functions live in `hydamo_validation.functions.topologic`.

---

## 6. fix_rules

Fix rules describe how to correct rows that have been flagged as invalid by one or more
validation rules. The fixer executes fix rules in an order determined by their dependency
structure (see the fixer `README.md`).

### Required fields

| Field | Type | Description |
|---|---|---|
| `attribute_name` | string | The attribute (column) on the layer that this fix writes to. |
| `validation_ids` | array of integers | The validation rule IDs whose failures trigger this fix. The fix is only applied to rows where at least one of these rules is currently failing. |
| `fix_id` | integer | Unique identifier for the fix rule within the object. |
| `fix_action` | string | Semantic label for the type of correction. Allowed values: `"Omit"`, `"Derived assumption"`, `"Manual adjustment"`, `"Replace"`, `"Modify"`, `"Flag"`. |
| `fix_type` | string | TODO: Implement distinction between `"automatic"` (applied without user interaction) or `"manual"` (pauses for review) in code. |
| `fix_method` | object | The function used to compute the corrected value (see [Function reference](#8-function-reference)). May be an empty object `{}` for `"manual"` fixes that have no computable correction. |
| `fix_description` | string | Human-readable description of what the fix does. |

### Optional fields

| Field | Type | Description |
|---|---|---|
| `filter` | object | A filter expression (see [Filters](#7-filters)). The fix is only applied to rows that satisfy the filter; other invalid rows remain unfixed by this rule. |

### Example

```json
{
    "attribute_name": "breedteopening",
    "validation_ids": [5],
    "fix_id": 5,
    "fix_action": "Derived assumption",
    "fix_type": "automatic",
    "fix_method": {
        "custom_hydamo": {
            "custom_function_name": "if_else",
            "logic": { "ISIN": { "parameter": "categorieinwatersysteem", "array": ["primair"] } },
            "true": { "equal": { "to": 0.8 } },
            "false": { "equal": { "to": 0.5 } }
        }
    },
    "fix_description": "breedteopening is 0.8 als primair, anders 0.5",
}
```

Fix rules with a `filter` only apply to the subset of rows matching the filter:

```json
{
    "attribute_name": "breedteopening",
    "validation_ids": [5],
    "fix_id": 5,
    "fix_action": "Derived assumption",
    "fix_type": "automatic",
    "fix_method": {
        "custom_hydamo": {
            "custom_function_name": "if_else",
            "logic": { "ISIN": { "parameter": "categorieinwatersysteem", "array": ["primair"] } },
            "true": { "equal": { "to": 0.8 } },
            "false": { "equal": { "to": 0.5 } }
        }
    },
    "fix_description": "breedteopening is 0.8 als primair, anders 0.5 (rond)",
    "filter": {
        "ISIN": {
            "parameter": "vormkoker",
            "array": ["Rond"]
        }
    }
}
```

**Source:** Fix rule functions use the same `hydamo_validation.functions.general` module as
general rules. Custom fix functions are implemented in `hydamo_validation.functions.custom`.

---

## 7. Function reference

### Functions available in general_rules and fix_rules (fix_method)

Source: `hydamo_validation.functions.general`

| Function key | Parameters | Description |
|---|---|---|
| `buffer` | `radius` (str/num), `percentile` (int), `coverage` (`"AHN"`), `fill_value` (num, optional) | Extracts a percentile of the AHN raster within a circular buffer around each feature. |
| `difference` | `left` (str/num), `right` (str/num), `absolute` (bool, optional) | Computes `left - right`, optionally taking the absolute value. |
| `divide` | `left` (str/num), `right` (str/num) | Computes `left / right`. |
| `equal` | `to` (str/num) | Sets the attribute to the value `to`. If `to` is a column name it copies that column; otherwise it assigns the literal value. |
| `join_parameter` | `join_object` (HyDAMO object name), `join_parameter` (str), `fill_value` (num, optional) | Looks up `join_parameter` from a related HyDAMO layer by matching on `{join_object}id`. |
| `multiply` | `left` (str/num), `right` (str/num) | Computes `left * right`. |
| `object_relation` | `related_object` (HyDAMO object name), `statistic` (`min`/`max`/`sum`/`count`/`majority`), `related_parameter` (str, optional), `fill_value` (num, optional) | Aggregates values of `related_parameter` from a related layer by `globalid`. |
| `sum` | `array` (list of str/num) | Sums all columns and/or literals in `array`. |
| `custom_hydamo` | `custom_function_name` (str), + any extra kwargs | Calls a function by name from `hydamo_validation.functions.custom`. See [section 10](#10-custom_hydamo--calling-custom-functions). |

### Functions available in validation_rules (function)

**Logic functions** — Source: `hydamo_validation.functions.logic`

| Function key | Parameters | Description |
|---|---|---|
| `BE` | `parameter` (str), `min` (num), `max` (num), `inclusive` (bool, optional) | True if `parameter` is between `min` and `max`. |
| `EQ` | `left` (str/num), `right` (str/num) | True if `left == right`. |
| `GE` | `left` (str/num), `right` (str/num) | True if `left >= right`. |
| `GT` | `left` (str/num), `right` (str/num) | True if `left > right`. |
| `ISIN` | `parameter` (str), `array` (list) | True if value of `parameter` is in `array`. |
| `LE` | `left` (str/num), `right` (str/num) | True if `left <= right`. |
| `LT` | `left` (str/num), `right` (str/num) | True if `left < right`. |
| `NOTIN` | `parameter` (str), `array` (list) | True if value of `parameter` is not in `array`. |
| `NOTNA` | `parameter` (str) | True if value of `parameter` is not null/NaN. |
| `join_object_exists` | `join_object` (str) | True if the referenced foreign key exists in the related layer's `globalid` column. |
| `consistent_period` | `max_gap` (int, optional), `groupers` (list, optional), `start_date` (str, optional), `end_date` (str, optional) | True if periodic entries (e.g. pump schedules) have no overlaps or excessive gaps. |

**Topologic functions** — Source: `hydamo_validation.functions.topologic`

| Function key | Description |
|---|---|
| `snaps_to_hydroobject` | Feature geometry lies on or snaps to a hydroobject line. |
| `no_dangling_node` | Node is connected to at least one other object. |
| `splitted_at_junction` | Hydroobject lines are split at every junction. |
| `not_overlapping` | Features do not overlap each other. |
| `structures_at_boundaries` | Structures are placed at the boundary of hydroobject segments. |
| `structures_at_intersections` | Structures are placed at intersections of hydroobject lines. |
| `structures_at_nodes` | Structures are placed at network nodes. |
| `compare_longitudinal` | Longitudinal comparison of values along a network. |
| `distant_to_others` | Feature is at a minimum distance from all other features of the same type. |
| `geometry_length` | Geometry length satisfies a length constraint. |

---

## 8. Filters

Filters narrow the set of rows a validation rule or fix rule operates on. A row that does
*not* satisfy the filter is treated as if it automatically passes the validation check, or is
skipped by the fix.

A filter is expressed using the same logic functions from `hydamo_validation.functions.logic`
that are available to validation rules. Only the comparison subset is permitted in a filter:

| Function key | Parameters | Description |
|---|---|---|
| `LE` | `left` (str/num), `right` (str/num) | `left <= right` |
| `LT` | `left` (str/num), `right` (str/num) | `left < right` |
| `GE` | `left` (str/num), `right` (str/num) | `left >= right` |
| `GT` | `left` (str/num), `right` (str/num) | `left > right` |
| `EQ` | `left` (str/num), `right` (str/num) | `left == right` |
| `ISIN` | `parameter` (str), `array` (list) | Value of `parameter` is in `array` |
| `NOTIN` | `parameter` (str), `array` (list) | Value of `parameter` is not in `array` |
| `NOTNA` | `parameter` (str) | Value of `parameter` is not null/NaN |

Input conventions are identical to their use in validation rules:
a string value refers to a column in the GeoDataFrame; a number is a literal constant.
See [How inputs are interpreted](#9-how-inputs-are-interpreted).

---

## 9. How inputs are interpreted

Wherever a function parameter accepts `"string or number"` (as shown in the schema), the
following interpretation applies:

- **String value** → treated as a **column name** in the layer's GeoDataFrame. The function
  reads the values from that column at runtime.
- **Numeric value** → treated as a **literal constant**. All rows receive the same value.

This applies to `left`, `right`, `parameter`, `radius`, `to`, and similar fields across all
functions. For example:

```json
{ "difference": { "left": "hoogtebinnenonderkantbov", "right": 5 } }
```

Here `"hoogtebinnenonderkantbov"` is a column name and `5` is a literal — the result is
`column_values - 5` for every row.

```json
{ "difference": { "left": "hoogtebinnenonderkantbov", "right": "bodemhoogte" } }
```

Here both are column references — the result is `column_a - column_b` per row.

---

## 10. custom_hydamo — calling custom functions

The `custom_hydamo` function type provides an escape hatch for logic that cannot be expressed
with the built-in functions. It calls a named Python function from
`hydamo_validation.functions.custom` at runtime.

### JSON structure

```json
{
    "custom_hydamo": {
        "custom_function_name": "<function_name>",
        "<kwarg_1>": <value_1>,
        "<kwarg_2>": <value_2>
    }
}
```

The `custom_function_name` key specifies which function to call. All other keys in the
`custom_hydamo` object are passed as keyword arguments (`**kwargs`) to that function.

### Expected function signature in custom.py

```python
def my_function(gdf: GeoDataFrame, hydamo: HyDAMO, param_a, param_b, ...) -> pd.Series:
    ...
```

`general.py` calls the function by unpacking the extra keys from the `custom_hydamo` object
(everything except `custom_function_name`) as keyword arguments. For example, a JSON key
`"parameter"` must appear as `parameter` in the function signature.

`gdf` and `hydamo` are injected automatically by the fixer at runtime — they do not need to
be (and cannot be) specified in the JSON.

### Example — fix_relative_to_level

The JSON:

```json
{
    "custom_hydamo": {
        "custom_function_name": "fix_relative_to_level",
        "parameter": "hoogteopening",
        "compare_parameter": "peilgebiedpraktijk_laagste_streefpeil",
        "operator": "-"
    }
}
```

Translates to the call:

```python
fix_relative_to_level(
    gdf,
    hydamo,
    parameter="hoogteopening",
    compare_parameter="peilgebiedpraktijk_laagste_streefpeil",
    operator="-",
)
```

### Example — if_else (conditional assignment)

```json
{
    "custom_hydamo": {
        "custom_function_name": "if_else",
        "logic": { "ISIN": { "parameter": "categorieinwatersysteem", "array": ["primair"] } },
        "true": { "equal": { "to": 0.8 } },
        "false": { "equal": { "to": 0.5 } }
    }
}
```

This sets the attribute to `0.8` where `categorieinwatersysteem` is `"primair"` and to `0.5`
otherwise.

### Available functions in custom.py

Source: `hydamo_validation.functions.custom`

Functions prefixed with `on_<object>_` are intended as general rules for that object (they
derive a computed column). Functions prefixed with `_` are helpers.

| Function name | Parameters (beyond `gdf`, `hydamo`) | Description |
|---|---|---|
| `on_profiellijn_compute_wet_profile_distance` | — | Computes `afstandNatProfiel` from `profielpunt.afstand` for wet-profile points (`typeprofielpunt == 22`). |
| `on_profiellijn_compute_wet_profile_depth` | — | Computes `dieptenatprofiel` as the height range within the wet profile points of each `profiellijn`. |
| `on_profiellijn_compute_max_cross_product` | — | Computes the maximum cross-product of successive segments; used to check whether a `profiellijn` geometry is straight. |
| `on_profiellijn_compute_jaarinwinning` | — | Extracts the year from `datuminwinning` into a `jaarinwinning` column. |
| `on_profiellijn_add_breedte_value_from_hydroobject` | — | Joins the `breedte` value from the related `hydroobject` (via `profielgroep`) to each `profiellijn`. |
| `on_profiellijn_compute_if_ascending` | — | Returns `1` if the cross-section profile is V-shaped (descending to a minimum then ascending by `afstand`); `0` otherwise. |
| `fix_relative_to_level` | `parameter` (str), `compare_parameter` (str), `operator` (str) | Computes `parameter - compare_parameter` (or other operator). Both inputs resolve to column values if the name exists as a column, otherwise treated as literals. |
| `if_else` | `logic` (dict), `true` (value), `false` (value), `attribute` (str) | Sets `attribute` to `true` for rows where `logic` evaluates to True, and to `false` otherwise. |

### Adding a new custom function

1. Open `hydamo_validation/functions/custom.py`.
2. Add a new function with the signature `def my_function(gdf, hydamo, param_a, ...)`.
3. Reference it by name in `validationrules.json` under `custom_function_name`.
4. Any parameters it needs can be passed as extra keys in the `custom_hydamo` object — or can
   be hardcoded inside the function itself.

**Tradeoff:** Passing parameters as JSON kwargs makes the function reusable and its
configuration visible in the rules file, but requires the custom.py function to declare those
exact parameter names. Hardcoding values inside the function is simpler to write but makes
the rules file less transparent.

---

## 11. CSV overviews

Three CSV files in this folder are automatically generated from `validationrules.json`
whenever `main` is updated:

| File | Contents |
|---|---|
| `validation_rules.csv` | All active (`active: true`) validation rules across all objects. |
| `general_rules.csv` | All general rules across all objects. |
| `fix_rules.csv` | All fix rules across all objects. |

### Columns in each CSV

| Column | Description |
|---|---|
| `id` | Rule id as defined in the JSON. |
| `bron` | Rule origin: `hwh` (id < 100 and no hhnk rule set) or `hhnk` (id ≥ 100 or `validation_rule_set == "hhnk"`). |
| `laag` | HyDAMO object (layer) name. |
| `type_functie` | `"validation rules"`, `"general rules"`, or `"fix rules"`. |
| `naam` | Rule name (validation/fix) or `result_variable` (general). |
| `beschrijving` | Description text from the JSON entry, or empty string if absent. |

### How to regenerate

**Automatically:** On every push or pull request to `main`, the GitHub Actions workflow
`.github/workflows/update_validation_rules_csv.yml` runs the export script and commits the
updated CSVs back to the branch.

**Manually (local):**

```bash
pixi run python -m hhnk_threedi_tools.core.schematisation_builder.utils.export_validation_rules_overview
```

The script is located at:
`hhnk_threedi_tools/core/schematisation_builder/utils/export_validation_rules_overview.py`

The workflow includes a bot-commit guard: if the last commit was made by `github-actions[bot]`
the workflow exits early, preventing infinite loops caused by the auto-commit step.
