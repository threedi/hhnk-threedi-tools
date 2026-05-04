# HyDAMO fixer

This package implements a structured, semi-automated pipeline for correcting invalid
attribute values in a HyDAMO dataset that has already been validated by `hydamo_validation`.

---

## Table of contents

1. [Pipeline overview](#1-pipeline-overview)
2. [File overview](#2-file-overview)
3. [Inputs from hydamo_validation](#3-inputs-from-hydamo_validation)
4. [Fix execution order: how it is computed](#4-fix-execution-order-how-it-is-computed)
5. [Decision tree: order in which fix rules are applied](#5-decision-tree-order-in-which-fix-rules-are-applied)
6. [Rechecking invalid indices after each fix](#6-rechecking-invalid-indices-after-each-fix)
7. [How the files connect](#7-how-the-files-connect)
8. [Full function reference](#8-full-function-reference)

---

## 1. Pipeline overview

The entry point for external callers is `HyDAMO_fixer.py` in the parent
`schematisation_builder` folder, which copies input files into the working directory and
delegates to `fixer.py`. The four stages of the pipeline are:

1. **Fix preparation** — build per-layer review GeoDataFrames with fix suggestions and
   validation summaries (based on the `hydamo_validation` output).
2. **User review** — pause execution so a reviewer can inspect and optionally override
   suggested fixes in the review GeoPackage.
3. **Fix execution** — apply staged fixes (and any manual overwrites) back into the
   datamodel, layer by layer, in dependency order.
4. **Export** — write the corrected `HyDAMO_fixed.gpkg` and a `fix_result.json` summary.

---

## 2. File overview

| File | Responsibility |
|---|---|
| `fixer.py` | Orchestration — validates the working directory, loads `hydamo_validation` outputs, runs review and execute, exports results. |
| `hydamo_fixes.py` | Fix logic — `review()` stages fix suggestions; `execute()` applies them in order, rechecking validity after each step. |
| `mapping.py` | Dependency analysis — statically analyses validation rules to compute a safe execution order for fix rules across and within layers. |
| `data.py` | Data classes — extended versions of `hydamo_validation`'s `HyDAMO`, `LayersSummary`, and `ResultSummary` with fix-pipeline metadata. |

---

## 3. Inputs from hydamo_validation

The fixer is designed as a post-processing layer on top of `hydamo_validation`. A typical
workflow is:

1. Run `hydamo_validation` → produces `HyDAMO_validated.gpkg` and `results.gpkg`.
2. Run the fixer on those outputs → produces `HyDAMO_fixed.gpkg`.

The validation results from step 1 drive nearly every decision in the fixer:

- **`results.gpkg`** — contains a per-layer table of which rows failed which validation
  rules. The fixer reads this as `validation_results` on `ExtendedHyDAMO` and uses it
  throughout `hydamo_fixes.py` to identify which rows need fixing.
- **`validationrules.json`** — parsed by `read_validation_rules` and stored on
  `ExtendedHyDAMO`. The fixer's `mapping.py` statically analyses these rules to determine
  execution order; `hydamo_fixes.py` re-evaluates individual rules after each fix step.
- **`HyDAMO_validated.gpkg`** — the datamodel itself, loaded into `ExtendedHyDAMO` via
  `from_geopackage`. All layer GeoDataFrames come from this file.

Key integration points with `hydamo_validation` internals:

| Component | Module | Purpose |
|---|---|---|
| `HyDAMO` | `data.py` | Base datamodel; `ExtendedHyDAMO` inherits from it |
| `LayersSummary`, `ResultSummary` | `data.py` | Base summary classes extended with fix-specific fields |
| `DataSets` | `fixer.py` | Enumerates available layers in the dataset directory |
| `read_validation_rules` | `fixer.py` | Parses `validationrules.json` into rule sets |
| `datamodel_layers`, `missing_layers` | `fixer.py` | Checks layer presence against schema |
| `_process_general_function` | `hydamo_fixes.py` | Evaluates general/fix rule functions on a GeoDataFrame slice |
| `_process_logic_function` | `hydamo_fixes.py` | Evaluates logic and filter expressions (boolean Series) |
| `_process_topologic_function` | `hydamo_fixes.py` | Evaluates topologic rules against the full datamodel |
| `_add_related_gdf`, `_add_join_gdf` | `hydamo_fixes.py` | Resolves cross-layer rule inputs |
| `_notna_indices`, `_nan_message` | `hydamo_fixes.py` | Handles NaN rows consistently |
| `normalize_fiona_schema`, `read_geopackage` | `data.py` | Reads layers from GeoPackage |
| `Timer` | `fixer.py` | Measures pipeline duration |

---

## 4. Fix execution order: how it is computed

Fix execution order is determined by two orthogonal concerns resolved in sequence:

1. **Layer order** — which layers are processed before others (cross-layer dependency).
2. **Fix-rule order within a layer** — which fix rules run first (attribute priority and
   dependency count).

### Step 1 — build the validation mapping

`_validation_mapping` (in `mapping.py`) inspects every validation rule in
`validationrules.json` and calls `_extract_inputs_from_function` to resolve all
`(object, attribute)` pairs the rule reads. The result is a nested dict:

```
validation_mapping = {
    "layer_A": {
        rule_id_1: [{"object": "layer_A", "attribute": "col_x"}, ...],
        rule_id_2: [{"object": "layer_B", "attribute": "col_y"}, ...],  # cross-layer ref
    },
    "layer_B": { ... },
}
```

### Step 2 — assign layer execution rounds

`_validation_iterations` builds a dependency graph from cross-layer references and assigns
each layer a round number:

```
round[layer] = max(round[dep] for dep in dependencies[layer]) + 1
```

Layers with no cross-layer dependencies start at round 1. This propagates iteratively until
stable (Bellman-Ford style). `_iterate_by_rounds` in `hydamo_fixes.py` uses these rounds to
process layers in safe order.

### Step 3 — derive attribute-to-validation-ID mapping

`_validation_ids` inverts `validation_mapping` to answer: *"which validation rule IDs care
about attribute X on layer Y?"*. This bridges fix rules (which reference attribute names) to
the validation results from `hydamo_validation` (which reference rule IDs):

```
validation_ids = {
    "duikersifonhevel": {
        "hoogteopening":            [10, 11],
        "breedteopening":           [12],
        "hoogtebinnenonderkantbov": [13],
    }
}
```

### Step 4 — group fix rules into iteration buckets

Fix rules within a layer are grouped and ordered using two inputs:

**`LAYER_MAPPING`** — manually maintained; assigns each fixable attribute to a named
iteration bucket. Attributes in lower-numbered buckets are fixed first because others may
depend on them.

```python
LAYER_MAPPING = {
    "duikersifonhevel": {
        1: [],                          # reserved / geometry fixes
        2: ["hoogteopening"],           # fix height first
        3: ["breedteopening",
            "hoogtebinnenonderkantbov",
            "hoogtebinnenonderkantbene"],
    },
}
```

**Dependency count (`dep_count`)** — within a bucket, fix rules are sorted by the number of
unique `(object, attribute)` input pairs across their associated validation rules. Using
unique pairs prevents the same input referenced by multiple rule IDs from inflating the count
and mis-ordering the rules. Rules with fewer unique inputs run first.

---

## 5. Decision tree: order in which fix rules are applied

```
For each layer (in layer round order from _validation_iterations):
|
+-- Does the layer appear in LAYER_MAPPING?
|   |
|   +-- NO  -> all fix rules placed in a single default bucket (overflow = 1)
|   |         All fix rules still run; there is no explicit attribute priority.
|   |         |
|   |         +-- Sort bucket by dep_count:
|   |             +-- dep_count == 0  -> no tracked inputs -> runs first
|   |             +-- dep_count  > 0  -> sorted ascending (simpler rules first)
|   |             +-- dep_count equal -> tie-break by fix_id ASC (determinism)
|   |
|   +-- YES -> attribute grouping applies
|             |
|             +-- For each fix rule on this layer:
|                 |
|                 +-- Is attribute_name in any bucket of LAYER_MAPPING[layer]?
|                 |   +-- YES -> assign to that bucket's iteration number
|                 |   +-- NO  -> assign to overflow bucket
|                 |             (max(LAYER_MAPPING keys) + 1)
|                 |             -> runs after all explicitly mapped attributes
|                 |
|                 +-- Compute dep_count:
|                     count of unique (object, attribute) pairs across all
|                     validation rules whose IDs appear in
|                     validation_ids[layer][attribute_name]
|
+-- Sort and emit fix rules:
    |
    +-- Outer sort: by iteration bucket number (ascending)
    |   -> respects LAYER_MAPPING priority (e.g. hoogteopening before breedteopening)
    |   -> overflow attributes always run last within the layer
    |
    +-- Inner sort within each bucket: by dep_count
        +-- dep_count == 0  -> runs first (least constrained)
        +-- dep_count  > 0  -> sorted ascending; simpler rules before complex ones
        +-- dep_count equal -> tie-break by fix_id ASC (determinism)
```

When `_iterate_by_steps` emits a `(step, rule)` pair, the step label encodes position:

- Single fix in a bucket → step label is the bucket number (e.g. `"2"`)
- Multiple fixes in a bucket → step label is `"<bucket>.<pos>"` (e.g. `"3.1"`, `"3.2"`)

This step label appears in log messages, making it straightforward to trace which fix ran
at which point in the execution sequence.

---

## 6. Rechecking invalid indices after each fix

Before each fix rule is applied, `execute` calls `_invalid_indices` to determine which rows
need fixing. The indices come from `object_validation_result` — the per-layer boolean table
produced by `hydamo_validation` and stored in `ExtendedHyDAMO.validation_results`.

After each fix rule completes, `execute` calls `_update_validation_result` to re-run the
relevant validation rules against the current (partially fixed) `object_gdf` and refresh
`object_validation_result`. Rule IDs are removed from a row's invalid column when the row
now passes, and retained when it still fails.

This means that when the *next* fix rule calls `_invalid_indices`, it reads an up-to-date
snapshot rather than the original `hydamo_validation` results. Rows corrected by an earlier
fix step are no longer flagged and will not be targeted again unnecessarily.

An optional `filter` in the fix rule definition can narrow the indices further. If present,
a logic expression is evaluated against the current `object_gdf` (which reflects previous
fixes) and only rows that satisfy the filter *and* appear in the invalid indices are
processed.

In summary:
- `_update_validation_result` keeps the snapshot current after each fix.
- `_invalid_indices` reads that snapshot to determine eligible rows.
- The optional `filter` narrows which of those rows are in scope.
- The fix function is only applied to the intersection of the two.

---

## 7. How the files connect

```
HyDAMO_fixer.py  (schematisation_builder/)
    +-- fixer.py            # orchestrates the full pipeline
            +-- hydamo_fixes.py     # fix logic (review + execute)
            |       +-- data.py     # ExtendedHyDAMO, summaries
            |       +-- mapping.py  # (via data.py) dependency analysis
            +-- data.py             # datamodel + summary classes
            +-- mapping.py          # dependency/ordering analysis
```

`data.py` depends on `mapping.py` — it calls `_validation_mapping`, `_validation_ids`,
`_validation_iterations`, and `_fix_iterations` during `post_process_datamodel()`.

`hydamo_fixes.py` depends on `data.py` for the extended classes and on `mapping.py`
indirectly through the pre-computed attributes on `ExtendedHyDAMO`.

`fixer.py` depends on `hydamo_fixes.py` (calls `review` and `execute`) and on `data.py`
(instantiates `ExtendedLayersSummary`, `ExtendedResultSummary`, `ExtendedHyDAMO`).

`HyDAMO_fixer.py` is a thin wrapper that copies input files into the working directory and
delegates to `fixer.fixer()`.

---

## 8. Full function reference

### `fixer.py`

| Function | Description |
|---|---|
| `fixer(output_types, log_level, coverages)` | Factory — returns a pre-configured `functools.partial` of `_fixer`. |
| `_fixer(directory, ...)` | Main pipeline function. Validates the working directory, loads the datamodel and validation rules, calls `review`, pauses for user review, calls `execute`, and exports results. |
| `pause_for_review(file_path, logger, result_summary)` | Halts the process at the review stage, prompting the user to confirm before fix execution proceeds. |
| `_continue(file_path, logger)` | Checks whether the user has confirmed that review is complete. |
| `_read_schema(directory)` | Locates and validates the `validationrules.json` file against the schema. |
| `_check_attributes(attributes)` | Validates that the expected attributes are present and of the correct type. |
| `_init_logger(log_level)` | Creates and configures the pipeline logger. |
| `_add_log_file(logger, log_path)` | Adds a file handler to the logger. |
| `_close_log_file(logger)` | Removes and flushes all file handlers on the logger. |
| `_log_to_results(result_summary, logger)` | Copies log output into the result summary object. |

---

### `hydamo_fixes.py`

| Function | Description |
|---|---|
| `FixColumns` | Frozen dataclass that derives consistent column names (`val_errors_*`, `fixes_*`, `fix_checks_*`, `manual_overwrite_*`) for a given attribute name. |
| `review(datamodel, layers_summary, result_summary, logger, raise_error)` | Iterates over all layers and fix rules to produce annotated review GeoDataFrames. Populates `layers_summary` with fix suggestions, validation summaries, fix-check results, and empty `manual_overwrite` columns. Does **not** modify the datamodel. |
| `execute(datamodel, layers_summary, result_summary, logger, raise_error)` | Applies fix rules to the datamodel in-place. For each fix rule: identifies still-invalid rows, applies the fix, applies manual overwrites, recomputes general rules, then calls `_update_validation_result` to refresh the validity snapshot. |
| `_apply_general_rules(gdf, layer, rules, overwrite, ...)` | Applies `general_rules` from the validation rule set to derive computed attributes. Called with `overwrite=False` up front and with `overwrite=True` after each fix rule. |
| `_apply_manual_overwrites(object_gdf, layers_summary, ...)` | Reads values from `manual_overwrite_*` columns in the review layer and writes them into the datamodel. |
| `_invalid_indices(gdf, validation_result, validation_ids)` | Identifies rows where any of the given validation rule IDs appear in the invalid columns of the validation result snapshot. |
| `_update_validation_result(object_gdf, object_validation_result, validation_rules, datamodel)` | Re-runs a set of validation rules against the current `object_gdf` and updates the `;`-separated rule ID strings in `object_validation_result`. Rule IDs are removed when the row now passes; retained or added when it still fails. |
| `_manual_indices(gdf, review_gdf, manual_column)` | Identifies rows where a reviewer has filled in a manual overwrite value. |
| `_iterate_by_rounds(data, execution_dict)` | Yields `(round, key, value)` in layer-dependency order using the round assignments from `_validation_iterations`. |
| `_iterate_by_steps(fix_rules, fix_iterations)` | Yields `(step, rule)` from a list of fix rules in the execution order defined by `_fix_iterations`. |
| `pre_run_logic(fix_method, gdf, datamodel)` | Pre-evaluates `if_else` custom function inputs before dispatch. |
| `_run_true_false(value, gdf, datamodel)` | Evaluates the `true` or `false` branch of an `if_else` expression. |
| `_add_custom_kwargs(fix_method, gdf, datamodel)` | Resolves and injects additional keyword arguments required by a `custom_hydamo` function call. |

---

### `mapping.py`

| Function | Description |
|---|---|
| `_build_general_rules_lookup_table(validation_rules)` | Builds a fast lookup from `layer -> result_variable -> function_dict` for all `general_rules`. Used as a pre-built index by `_extract_inputs_from_function`. |
| `_extract_inputs_from_function(func, current_layer, layers, columns_by_layer, ...)` | Recursively resolves all concrete `(object, attribute)` input references for a single function definition. Handles nested functions, derived variables, cross-layer references, geometry references, and whole-object references. |
| `map_general_rule_inputs(datamodel, layers)` | Builds the full input dependency map for all general rules across all layers. |
| `_validation_mapping(datamodel, layers, include_topologic, omit_topologic_as_none)` | Builds the full input dependency map for all validation rules. Central mapping consumed by all downstream ordering functions. |
| `_validation_iterations(mapping)` | Assigns an execution round to each layer based on inter-layer dependencies. Computed iteratively until stable (Bellman-Ford style). |
| `_validation_ids(validation_mapping)` | Inverts `validation_mapping` to produce `layer -> {attribute_name: [validation_ids]}`. |
| `_fix_iterations(validation_rules, validation_mapping, validation_ids, layer_mapping)` | Combines `LAYER_MAPPING` attribute priorities with dependency counts to produce `layer -> {iteration_num: [fix_ids]}`, the final ordered execution plan. |
| `_get_validation_ids_for_attribute(mapping, object_layer, attribute_name)` | Helper that looks up validation IDs for a specific layer + attribute pair. |

---

### `data.py`

| Class / method | Description |
|---|---|
| `ExtendedResultSummary(ResultSummary)` | Extends `hydamo_validation.ResultSummary` with `prep_result`, `fix_result`, and `fix_layers` fields. Adds `to_json()` to write the summary as a JSON file. |
| `ExtendedLayersSummary(LayersSummary)` | Extends `hydamo_validation.LayersSummary` with `empty`, `data_layers`, `export()`, and `from_geopackage()`. |
| `ExtendedLayersSummary.export(results_path, gpkg_name, output_types)` | Writes all GeoDataFrame attributes to the specified output formats (`geopackage`, `geojson`, `csv`). |
| `ExtendedLayersSummary.from_geopackage(file_path)` | Classmethod to reconstruct an instance from a GeoPackage (used to reload the review layer after user edits). |
| `ExtendedHyDAMO(HyDAMO)` | Extends `hydamo_validation.HyDAMO` with fix-pipeline metadata: `validation_results`, `validation_rules`, `validation_mapping`, `validation_ids`, `validation_iterations`, and `fix_iterations`. |
| `ExtendedHyDAMO.read_layer(layer, result_summary, status_object)` | Reads a layer from the GeoPackage with optional status filtering. |
| `ExtendedHyDAMO.from_geopackage(hydamo_path, results_path, rules_objects, ...)` | Classmethod that loads the full datamodel from a GeoPackage and initialises all fix-pipeline metadata by calling `post_process_datamodel()`. |
| `ExtendedHyDAMO.post_process_datamodel()` | Calls all four mapping functions (`_validation_mapping`, `_validation_ids`, `_validation_iterations`, `_fix_iterations`) and stores their results as instance attributes. |
