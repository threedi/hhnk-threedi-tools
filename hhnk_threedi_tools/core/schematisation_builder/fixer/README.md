# HyDAMO fixer functions

This package implements the HyDAMO fix pipeline: a structured, semi-automated process for
correcting invalid attribute values in a validated HyDAMO dataset. The pipeline stages are:

1. **Fix-preparation** - build per-layer review GeoDataFrames with fix suggestions and validation summaries.
2. **User review** - pause execution so a reviewer can inspect and optionally override suggested fixes.
3. **Fix execution** - apply staged fixes (and any manual overwrites) back into the datamodel.
4. **Export** - write the corrected `HyDAMO_fixed.gpkg` and a `fix_result.json` summary.

The entry point for external callers is `HyDAMO_fixer.py` in the parent `schematisation_builder`
folder, which wraps `fixer.py` with file-copy bookkeeping.

---

## File overview

### `fixer.py`

The orchestration layer. Contains:

- `fixer(output_types, log_level, coverages)` - factory function that returns a pre-configured
  callable (a `functools.partial` of `_fixer`).
- `_fixer(directory, ...)` - the main pipeline function. Validates the working directory,
  loads the datamodel and validation rules, then calls `hydamo_fixes.review()`,
  pauses for user review, calls `hydamo_fixes.execute()`, and exports results.
- `pause_for_review(file_path, logger, result_summary)` - halts the process at the review
  stage, prompting the user to confirm before fix execution proceeds.
- Several private helpers for logging setup (`_init_logger`, `_add_log_file`, `_close_log_file`,
  `_log_to_results`) and input validation (`_check_attributes`, `_read_schema`).

**Uses from `hydamo_validation`:**
- `DataSets` - reads the dataset directory and enumerates available layers.
- `read_validation_rules` - parses `validationrules.json` into rule sets.
- `datamodel_layers`, `missing_layers` - syntax-level checks on layer availability.
- `logical_validation.general_functions._set_coverage` - registers coverage rasters for
  topologic/general rule evaluation.
- `Timer` - measures total pipeline duration.
- `HyDAMO` (via `ExtendedHyDAMO`) - the schema-validated datamodel.

---

### `hydamo_fixes.py`

The fix logic layer. Contains all functions that directly read from or write to GeoDataFrames.

- `FixColumns` - frozen dataclass that derives consistent column names (`val_errors_*`,
  `fixes_*`, `fix_checks_*`, `manual_overwrite_*`) for a given attribute name.
- `review(datamodel, layers_summary, result_summary, logger, raise_error)` - iterates over
  all layers and fix rules to produce annotated review GeoDataFrames. Populates
  `layers_summary` with one GeoDataFrame per layer containing fix suggestions, validation
  summaries, fix-check results, and empty `manual_overwrite` columns for the reviewer to fill.
  Does **not** modify the datamodel.
- `execute(datamodel, layers_summary, result_summary, logger, raise_error)` - applies fix
  rules to the datamodel in-place. For each fix rule it: identifies still-invalid rows via
  `_invalid_indices`, applies the fix, applies manual overwrites, recomputes general rules,
  then calls `_update_validation_result` to refresh the validity snapshot so the next fix
  rule only targets rows that remain invalid after the previous fix.
- `_apply_general_rules(gdf, layer, rules, overwrite, ...)` - applies `general_rules` from
  the validation rule set to derive computed attributes. Called once up front
  (`overwrite=False`) to ensure derived columns exist, and again after each fix rule
  (`overwrite=True`) to recompute any columns that depend on the just-fixed attribute.
- `_apply_manual_overwrites(object_gdf, layers_summary, ...)` - reads values from
  `manual_overwrite_*` columns in the review layer and writes them into the datamodel.
- `_invalid_indices(gdf, validation_result, validation_ids)` - identifies rows where any of
  the given validation rule IDs appear in the invalid columns of the validation results.
  Reads from whichever `validation_result` snapshot is passed in; after each fix step,
  `execute` passes the freshly updated snapshot from `_update_validation_result`.
- `_update_validation_result(object_gdf, object_validation_result, validation_rules, datamodel)` -
  re-runs a set of validation rules against the current (partially fixed) `object_gdf` and
  updates the `;`-separated rule ID strings in `object_validation_result` accordingly.
  Rule IDs are removed from a row's invalid column when the row now passes, and retained
  (or added) when it still fails. If a rule cannot be re-evaluated, its existing entry is
  left unchanged so the row stays in scope. Called by `execute` after each fix step.
- `_manual_indices(gdf, review_gdf, manual_column)` - identifies rows where a reviewer has
  filled in a manual overwrite value.
- `_iterate_by_rounds(data, execution_dict)` - yields `(round, key, value)` in dependency
  order, using the round assignments from `mapping._validation_iterations`.
- `_iterate_by_steps(fix_rules, fix_iterations)` - yields `(step, rule)` from a list of fix
  rules in the execution order defined by `mapping._fix_iterations`.
- `pre_run_logic`, `_run_true_false`, `_add_custom_kwargs` - helpers that pre-evaluate
  `if_else` custom function inputs before dispatch.

**Uses from `hydamo_validation`:**
- `_process_general_function` - evaluates a general/fix function on a GeoDataFrame slice.
- `_process_logic_function` - evaluates a logic expression, returning a boolean Series.
- `_process_topologic_function` - evaluates a topologic rule against the full datamodel.
- `_add_related_gdf`, `_add_join_gdf` - resolve related/join object references in rule inputs.
- `_notna_indices` - returns the indices of rows that are not NaN for the required inputs.
- `_nan_message` - formats a warning message for rows dropped due to NaN inputs.

---

### `data.py`

The data classes layer. Defines extended versions of the `hydamo_validation` summary and
datamodel classes.

- `ExtendedResultSummary(ResultSummary)` - extends the base result summary with
  `prep_result`, `fix_result`, and `fix_layers` fields. Adds `to_json()` to write the
  summary as a JSON file.
- `ExtendedLayersSummary(LayersSummary)` - extends the base layers summary with:
  - `empty` property - checks whether any GeoDataFrame attributes are present.
  - `data_layers` property - returns names of non-empty GeoDataFrame attributes.
  - `export(results_path, gpkg_name, output_types)` - writes all GeoDataFrame attributes
    to the specified output formats (`geopackage`, `geojson`, `csv`).
  - `from_geopackage(file_path)` - classmethod to reconstruct an instance from a GeoPackage,
    used to reload the review layer after user edits.
- `ExtendedHyDAMO(HyDAMO)` - extends the base HyDAMO datamodel with fix-pipeline metadata:
  - Holds `validation_results`, `validation_rules`, `validation_mapping`, `validation_ids`,
    `validation_iterations`, and `fix_iterations` as instance attributes populated during
    `post_process_datamodel()`.
  - `read_layer(layer, result_summary, status_object)` - reads a layer from the GeoPackage
    with optional status filtering.
  - `from_geopackage(hydamo_path, results_path, rules_objects, ...)` - classmethod to load
    the full datamodel from a GeoPackage and initialise all fix-pipeline metadata.

**Uses from `hydamo_validation`:**
- `HyDAMO` - base datamodel class providing schema-validated layer objects and `layers` list.
- `LayersSummary`, `ResultSummary` - base summary classes (from `hydamo_validation.summaries`).
- `normalize_fiona_schema`, `read_geopackage` - utility functions for reading GeoPackages.

---

### `mapping.py`

The dependency analysis layer. Statically analyses validation rules to determine which
attributes and layers each rule depends on, and uses that to compute a safe execution order
for fix rules.

#### Functions

- `_build_general_rules_lookup_table(validation_rules)` - builds a fast lookup from
  `layer -> result_variable_name -> function_dict` for all `general_rules` entries.
  Used as a pre-built index so that `_extract_inputs_from_function` can resolve derived
  variables without rescanning the full rule set on every call.
- `_extract_inputs_from_function(func, current_layer, layers, columns_by_layer, ...)` -
  recursively resolves all concrete `(object, attribute)` input references for a single
  function definition. Handles nested functions, derived variables, cross-layer references,
  geometry references, and whole-object references. See the decision tree below.
- `map_general_rule_inputs(datamodel, layers)` - builds the full input dependency map for
  all general rules across all layers (same shape as `_validation_mapping` but scoped to
  `general_rules` only).
- `_validation_mapping(datamodel, layers, include_topologic, omit_topologic_as_none)` -
  builds the full input dependency map for all validation rules. This is the central mapping
  that all downstream ordering functions consume.
- `_validation_iterations(mapping)` - assigns an execution round (integer) to each layer
  based on inter-layer dependencies. Layers in round N may depend on layers in round N-1.
  Computed iteratively until stable (Bellman-Ford style propagation).
- `_validation_ids(validation_mapping)` - inverts the mapping to produce
  `layer -> {attribute_name: [validation_ids]}`, bridging validation rule IDs to fix rules.
- `_fix_iterations(validation_rules, validation_mapping, validation_ids, layer_mapping)` -
  combines `LAYER_MAPPING` attribute priorities with dependency counts to produce
  `layer -> {iteration_num: [fix_ids]}`, the final ordered execution plan for fix rules.
  Within each bucket, rules are sorted by the number of **unique** `(object, attribute)`
  input pairs across their associated validation rules (fewest unique dependencies first).
- `_get_validation_ids_for_attribute(mapping, object_layer, attribute_name)` - helper that
  looks up validation IDs for a specific layer + attribute pair.

**Uses from `hydamo_validation`:** None directly. Operates purely on the validation rules
dict and the GeoDataFrame columns present on each layer.

---

#### Fix execution order: how it is computed

Fix execution order is determined by two orthogonal concerns that are resolved in sequence:

1. **Layer order** — which layers are processed before others (cross-layer dependency).
2. **Fix-rule order within a layer** — which fix rules run first within a single layer
   (attribute dependency and manual priority grouping).

##### Step 1: build the validation mapping

`_validation_mapping` inspects every validation rule in `validationrules.json` and calls
`_extract_inputs_from_function` to resolve all `(object, attribute)` pairs the rule reads.
The result is a nested dict:

```
validation_mapping = {
    "layer_A": {
        rule_id_1: [{"object": "layer_A", "attribute": "col_x"}, ...],
        rule_id_2: [{"object": "layer_B", "attribute": "col_y"}, ...],  # cross-layer ref
        ...
    },
    "layer_B": { ... },
    ...
}
```

##### Step 2: assign layer execution rounds (`_validation_iterations`)

Each entry `{"object": "layer_B", ...}` in `layer_A`'s rules means `layer_A` depends on
`layer_B`. `_validation_iterations` builds a dependency graph from these cross-layer
references, then iteratively assigns rounds:

```
round[layer] = max(round[dep] for dep in dependencies[layer]) + 1
```

Layers with no cross-layer dependencies start at round 1. This is repeated until no round
values change. The result is used by `_iterate_by_rounds` in `hydamo_fixes.py` to process
layers in safe order.

##### Step 3: derive attribute-to-validation-ID mapping (`_validation_ids`)

`_validation_ids` inverts `validation_mapping` to answer: "which validation rule IDs care
about attribute X on layer Y?" The result bridges fix rules (which reference attribute names)
to validation results (which reference rule IDs):

```
validation_ids = {
    "duikersifonhevel": {
        "hoogteopening":            [10, 11],
        "breedteopening":           [12],
        "hoogtebinnenonderkantbov": [13],
        ...
    }
}
```

##### Step 4: group fix rules into iterations (`_fix_iterations`)

Fix rules within a layer are grouped and ordered using two inputs:

- **`LAYER_MAPPING`** — a manually maintained configuration that assigns each fixable
  attribute to a named iteration bucket. Attributes in lower-numbered buckets are fixed
  first because others may depend on them.
- **Dependency count** — within a bucket, fix rules are sorted by the number of unique
  `(object, attribute)` input pairs across their associated validation rules (fewest first).
  Using unique pairs rather than the raw total prevents the same input referenced by
  multiple rule IDs from inflating the count and mis-ordering the rules.

```python
LAYER_MAPPING = {
    "duikersifonhevel": {
        1: [],                          # reserved / geometry fixes (empty)
        2: ["hoogteopening"],           # fix height first
        3: ["breedteopening",           # fix width and invert levels after
            "hoogtebinnenonderkantbov",
            "hoogtebinnenonderkantbene"],
    },
}
```

Attributes not listed in `LAYER_MAPPING` are placed in an overflow bucket numbered
`max(iteration_keys) + 1`, so they always run last.

The final result consumed by `_iterate_by_steps` is:

```
fix_iterations = {
    "duikersifonhevel": {
        1: [],        # no fix rules in this bucket
        2: [10],      # fix hoogteopening (fix_id 10)
        3: [11, 12],  # fix breedteopening, then hoogtebinnenonderkant*
    }
}
```

---

#### Decision tree: order in which fix rules are applied for a single layer

```
For each layer (in layer round order from _validation_iterations):
│
├── Does the layer appear in LAYER_MAPPING?
│   │
│   ├── NO  → all fix rules for this layer are placed in a single default
│   │         bucket (overflow_iteration = 1). All fix rules still run;
│   │         they just have no explicit attribute priority ordering.
│   │         │
│   │         └── Sort bucket contents by dep_count:
│   │             │
│   │             ├── Compute dep_count for each fix rule:
│   │             │   count of unique (object, attribute) pairs across all
│   │             │   validation rules whose IDs appear in
│   │             │   validation_ids[layer][attribute_name]
│   │             │   (duplicates that appear in multiple rule IDs are counted once)
│   │             │
│   │             ├── dep_count == 0  → rule reads no tracked inputs
│   │             │                     → runs first (least constrained)
│   │             │
│   │             ├── dep_count  > 0  → rule reads N tracked inputs
│   │             │                     → sorted ascending by dep_count
│   │             │                     → simpler rules run before complex ones
│   │             │
│   │             └── dep_count equal → tie-break by fix_id ASC (determinism)
│   │
│   └── YES → proceed with attribute grouping
│             │
│             └── For each fix rule on this layer:
│                 │
│                 ├── Is attribute_name listed in any bucket of LAYER_MAPPING[layer]?
│                 │   │
│                 │   ├── YES → assign to that bucket's iteration number
│                 │   │
│                 │   └── NO  → assign to overflow bucket
│                 │             (max(LAYER_MAPPING iteration keys) + 1)
│                 │             → runs after all explicitly mapped attributes
│                 │
│                 └── Compute dep_count for this fix rule:
│                     count of unique (object, attribute) pairs across all
│                     validation rules whose IDs appear in
│                     validation_ids[layer][attribute_name]
│                     (duplicates that appear in multiple rule IDs are counted once)
│
└── Sort and emit fix rules:
    │
    ├── Outer sort: by iteration bucket number (ascending)
    │   → respects LAYER_MAPPING priority (e.g. hoogteopening before breedteopening)
    │   → overflow attributes always run last within the layer
    │
    └── Inner sort within each bucket: by dep_count
        │
        ├── dep_count == 0  → rule reads no tracked inputs
        │                     → runs first (least constrained)
        │
        ├── dep_count  > 0  → rule reads N tracked inputs
        │                     → sorted ascending; simpler rules run before complex ones
        │                     → reduces the chance that a fix reads a value that a
        │                       prior fix in the same bucket was supposed to correct
        │
        └── dep_count equal → tie-break by fix_id ASC (determinism)
```

When `_iterate_by_steps` emits a `(step, rule)` pair, the step label encodes position:

- Single fix in a bucket → step label is the bucket number (e.g. `"2"`)
- Multiple fixes in a bucket → step label is `"<bucket>.<pos>"` (e.g. `"3.1"`, `"3.2"`)

This step label appears in log messages, making it straightforward to trace which fix ran
at which point in the execution sequence.

#### Targeted application: rechecking invalid indices before each fix

Before each fix rule is applied, `execute` calls `_invalid_indices` to determine which rows
actually need fixing at that moment. After each fix rule completes, `execute` calls
`_update_validation_result` to re-run the relevant validation rules against the current
(partially fixed) `object_gdf` and refresh `object_validation_result`. Rule IDs are removed
from a row's invalid column when the row now passes, and retained when it still fails.

This means that when the *next* fix rule calls `_invalid_indices`, it reads an up-to-date
snapshot rather than the original pre-fix validation results. Rows corrected by an earlier
fix step are no longer counted as invalid and will not be targeted again unnecessarily.

An optional `filter` in the fix rule definition can narrow the indices further. If a
`filter` is present, a logic expression is evaluated against the current state of
`object_gdf` (which does reflect previous fixes within the same execution pass), and only
rows that satisfy the filter *and* appear in the invalid indices are processed.

In summary: `_update_validation_result` keeps the validity snapshot current after each fix,
`_invalid_indices` reads that snapshot to determine eligible rows, the optional filter
determines which of those rows are in scope, and the fix function is only executed on the
intersection of the two.

---

## How the files connect

```
HyDAMO_fixer.py  (schematisation_builder/)
    └── fixer.py            # orchestrates the full pipeline
            ├── hydamo_fixes.py     # fix logic (review + execute)
            │       ├── data.py     # ExtendedHyDAMO, summaries
            │       └── mapping.py  # (via data.py) dependency analysis
            ├── data.py             # datamodel + summary classes
            └── mapping.py          # dependency/ordering analysis
```

`data.py` depends on `mapping.py` (calls `_validation_mapping`, `_validation_ids`,
`_validation_iterations`, `_fix_iterations` during `post_process_datamodel()`).

`hydamo_fixes.py` depends on `data.py` for the three extended classes and on
`mapping.py` indirectly through the pre-computed attributes on `ExtendedHyDAMO`.

`fixer.py` depends on `hydamo_fixes.py` (calls `review` and `execute`) and on `data.py`
(instantiates `ExtendedLayersSummary`, `ExtendedResultSummary`, `ExtendedHyDAMO`).

`HyDAMO_fixer.py` is a thin wrapper: it copies the input files into the working directory
and delegates to `fixer.fixer()`.

---

## Connection to `hydamo_validation`

The fixer package is designed as a post-processing layer on top of `hydamo_validation`.
A typical workflow is:

1. Run `hydamo_validation` to produce `HyDAMO_validated.gpkg` and `results.gpkg`.
2. Run the fixer on those outputs to produce `HyDAMO_fixed.gpkg`.

The key integration points are:

| `hydamo_validation` component | Used by | Purpose |
|---|---|---|
| `HyDAMO` | `data.py` | Base datamodel; `ExtendedHyDAMO` inherits from it |
| `LayersSummary`, `ResultSummary` | `data.py` | Base summary classes extended with fix-specific fields |
| `DataSets` | `fixer.py` | Enumerate available layers in the dataset directory |
| `read_validation_rules` | `fixer.py` | Parse `validationrules.json` |
| `datamodel_layers`, `missing_layers` | `fixer.py` | Check layer presence against schema |
| `_process_general_function` | `hydamo_fixes.py` | Evaluate general/fix rule functions |
| `_process_logic_function` | `hydamo_fixes.py` | Evaluate logic/filter expressions |
| `_process_topologic_function` | `hydamo_fixes.py` | Evaluate topologic rules |
| `_add_related_gdf`, `_add_join_gdf` | `hydamo_fixes.py` | Resolve cross-layer rule inputs |
| `_notna_indices`, `_nan_message` | `hydamo_fixes.py` | Handle NaN rows consistently |
| `normalize_fiona_schema`, `read_geopackage` | `data.py` | Read layers from GeoPackage |
| `Timer` | `fixer.py` | Measure pipeline duration |
