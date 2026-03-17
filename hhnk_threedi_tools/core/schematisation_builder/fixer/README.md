
# Fixer Module — Concise Developer Documentation (D1)

## Purpose
The `fixer.py` module orchestrates the HyDAMO fixing workflow used in schematisation building. It validates, prepares, and automatically corrects HyDAMO datasets based on predefined rules.

It acts as the high‑level controller coordinating:
- Dataset loading
- Validation rule parsing
- Syntax validation
- Fix preparation and execution
- Export of corrected results and summary files

The module does **not** implement fix logic itself; this is delegated to `hydamo_fixes`.

---

## Key Responsibilities

### 1. Directory and Environment Preparation
- Creates/clears `review/` and `results/` directories.
- Initializes logging (terminal + file).
- Ensures required files exist:
  - `datasets/`
  - `validationrules.json`
  - `results.gpkg`
  - `HyDAMO_validated.gpkg`

### 2. Validation Rule Loading
Reads and parses `validationrules.json`, extracting:
- HyDAMO version
- Object-level validation rules
- Optional `status_object`

### 3. Datamodel Loading
Constructs an `ExtendedHyDAMO` instance using:
- validated dataset
- validation results
- rule definitions

Missing schema layers are ignored.

### 4. Syntax Validation
Checks the dataset using:
- `datamodel_layers()`
- `missing_layers()`

Verifies that dataset structure matches the expected HyDAMO schema.

### 5. First Pass — Fix Preparation
Runs:
```
hydamo_fixes.execute(..., keep_general=False)
```
This creates a *staging version* of fix layers and exports them to the `review/` folder.

### 6. Manual Review Stage
User inspects or adjusts:
```
review/fix_summary.gpkg
```

### 7. Reload Updated Fix Summary
Updated fix instructions are loaded back into the workflow.

### 8. Second Pass — Final Fix Application
Fixes are re-applied to the HyDAMO datamodel using the updated review layers.

### 9. Export
Outputs include:
- `results/HyDAMO_fixed.gpkg`
- `results/fix_summary.gpkg`
- `results/fix_result.json`
- `results/fixer.log`

### 10. Summary Reporting
A structured `ExtendedResultSummary` reports:
- errors
- applied fixes
- unavailable layers
- fix results
- duration
- full log output

---

## Public API

### `fixer(output_types, log_level, coverages)`
Returns a callable that executes `_fixer()` for a given directory.

Example:
```python
from fixer import fixer
run_fixes = fixer()
run_fixes("/path/to/project")
```

---

## Entry Point — `_fixer()`
The internal function performing the entire workflow.

Returns:
```
(HyDAMO_datamodel, ExtendedLayersSummary, ExtendedResultSummary)
```
On failure (when `raise_error=False`):
```
(None, fix_summary, result_summary)
```

---

## When to Use This Module
Use `fixer.py` when you need to:
- Validate and correct HyDAMO model files
- Apply a structured, rule-based correction workflow
- Produce traceable fix summaries and logs
- Integrate a user review step in the correction process

---

