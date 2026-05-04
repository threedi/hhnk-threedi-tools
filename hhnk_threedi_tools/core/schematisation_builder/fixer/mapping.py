from typing import Any

from shapely.geometry import LineString, Point, Polygon

GEOTYPE_MAPPING = {LineString: "LineString", Point: "Point", Polygon: "Polygon"}
SUMMARY_COLUMNS = [
    "is_usable",
    "fix_history",
]
KEEP_COLUMNS = ["code", "geometry", "valid", "invalid_critical", "invalid_non_critical", "ignored"]
LIST_SEPARATOR = ";"
NOTNA_COL_IGNORE = ["related_parameter"]
EXCEPTION_COL = "nen3610id"
INVALID_COLUMNS = ["invalid_critical", "invalid_non_critical", "ignored"]

LAYER_MAPPING = {
    # "profiellijn": {
    #     1: ["talud_links", "talud_rechts"],
    #     2: ["waterdiepte_tov_streefpeil"],
    # },
    "duikersifonhevel": {
        1: [],
        2: ["hoogteopening"],
        3: ["breedteopening", "hoogtebinnenonderkantbov", "hoogtebinnenonderkantbene"],
    },
}


def _build_general_rules_lookup_table(validation_rules: dict[str, Any]) -> dict[str, dict[str, dict[str, Any]]]:
    """
    Build a lookup table for derived variables originating from 'general_rules'.

    This transforms the validation rules structure into a quick‑access mapping:
        {
            layer_name: {
                result_variable_name: <function-definition-dict>
            }
        }

    Parameters
    ----------
    validation_rules : dict
        Raw validation‑rules dictionary read from validationrules.json. Expected
        structure:
            validation_rules[layer]["general_rules"] → list of rules

    Returns
    -------
    dict
        Nested mapping enabling efficient recursive evaluation of general rule
        dependencies when resolving input variables.

        {
          layer_name: {
              result_variable_name: { <function dict> }
          }
        }
    """
    lookup: dict[str, dict[str, dict[str, Any]]] = {}
    for layer, ruleset in validation_rules.items():
        general_rules = ruleset.get("general_rules", [])
        layer_lookup: dict[str, dict[str, Any]] = {}
        for gr in general_rules:
            # NOTE: your JSON uses "result_variable" for name
            rv = gr.get("result_variable")
            if rv and "function" in gr:
                layer_lookup[rv] = gr["function"]
        lookup[layer] = layer_lookup
    return lookup


def _extract_inputs_from_function(
    func: dict[str, Any],
    current_layer: str,
    layers: set[str],
    columns_by_layer: dict[str, set[str]],
    general_lookup_by_layer: dict[str, dict[str, dict[str, Any]]],
    seen_general: set[tuple[str, str]] | None = None,
) -> list[dict[str, Any]]:
    """
    Recursively extract all concrete input references required by a function
    definition used in logical/general/topologic rules.

    This handles:
    - nested function definitions
    - derived variables (recursively expanding them)
    - object‑prefixed references pointing to other layers
    - geometry.* references
    - whole‑object references when no attribute is explicitly provided

    Parameters
    ----------
    func : dict
        Single‑function dictionary where the key is the function name and the
        value is its parameter mapping.
    current_layer : str
        Layer for which the function is being evaluated.
    layers : set[str]
        Set of all available layer names in the datamodel.
    columns_by_layer : dict[str, set[str]]
        Mapping of available columns for each layer.
    general_lookup_by_layer : dict
        Lookup table for derived variables constructed by
        `_build_general_rules_lookup_table`.
    seen_general : set of (layer, variable), optional
        Used to prevent infinite recursion when derived variables depend on
        each other.

    Returns
    -------
    list of dict
        List of resolved input references, each item of the form:
            {"object": <layer>, "attribute": <column-or-None>}
    """

    if seen_general is None:
        seen_general = set()

    fname = next(iter(func))
    params = func[fname]
    inputs: list[dict[str, Any]] = []
    prefix = ""

    # -------------------------------------------------------------
    # STEP 1 — Detect object references via "*object*" in key
    # -------------------------------------------------------------
    referenced_objects: dict[str, str] = {}  # {object: prefix}
    for key, val in params.items():
        if "object" in key.lower() and isinstance(val, str) and val in layers:
            prefix = key.lower().split("object")[0].rstrip("_")
            referenced_objects[val] = prefix

    # -------------------------------------------------------------
    # STEP 2 — Process each parameter
    # -------------------------------------------------------------
    for key, val in params.items():
        # ----------------------------
        # CASE A — numeric → ignore
        # ----------------------------
        if isinstance(val, (int, float, bool)) or val is None:
            continue

        # ----------------------------
        # CASE B — nested function
        # ----------------------------
        if isinstance(val, dict) and len(val) == 1:
            nested = _extract_inputs_from_function(
                val,
                current_layer,
                layers,
                columns_by_layer,
                general_lookup_by_layer,
                seen_general,
            )
            inputs.extend(nested)
            continue

        # ----------------------------
        # CASE C — string only
        # ----------------------------
        if isinstance(val, str):
            # RULE: If val is a derived variable on current layer → expand recursively
            if val in general_lookup_by_layer.get(current_layer, {}) and (current_layer, val) not in seen_general:
                seen_general.add((current_layer, val))
                derived = general_lookup_by_layer[current_layer][val]
                sub = _extract_inputs_from_function(
                    derived,
                    current_layer,
                    layers,
                    columns_by_layer,
                    general_lookup_by_layer,
                    seen_general,
                )
                inputs.extend(sub)
                continue

            # RULE: If val is a column of current object and key does not start with a prefix → bind to current layer
            if val in columns_by_layer[current_layer] or val.startswith("geometry."):
                if referenced_objects:
                    if not any([key.lower().startswith(prefix) for prefix in list(referenced_objects.values())]):
                        inputs.append({"object": current_layer, "attribute": val})
                        continue
                else:
                    inputs.append({"object": current_layer, "attribute": val})
                    continue

            # RULE: If val is a column of *another* object BUT only if key respects object-prefix rule
            for obj, prefix in referenced_objects.items():
                if key.lower().startswith(prefix):
                    # RULE: val may be a derived variable on *referenced* object
                    if val in general_lookup_by_layer.get(obj, {}) and (obj, val) not in seen_general:
                        seen_general.add((obj, val))
                        derived = general_lookup_by_layer[obj][val]
                        sub = _extract_inputs_from_function(
                            derived, obj, layers, columns_by_layer, general_lookup_by_layer, seen_general
                        )
                        inputs.extend(sub)
                        break
                    # Raw column on referenced object
                    if val in columns_by_layer.get(obj, set()) or val.startswith("geometry."):
                        inputs.append({"object": obj, "attribute": val})
                        break

    # -------------------------------------------------------------
    # STEP 3 — Whole-object inference
    # Only add {object: <obj>, attribute: None} if there is NO key
    # in 'params' that starts with the object's prefix (besides the
    # "<prefix>_object" key itself).
    # -------------------------------------------------------------
    for obj, prefix in referenced_objects.items():
        has_prefixed_key = any(k.lower().startswith(prefix) and k.lower() != f"{prefix}_object" for k in params.keys())
        # RULE: no attribute keys matching prefix → whole-object reference
        if not has_prefixed_key:
            inputs.append({"object": obj, "attribute": None})

    # -------------------------------------------------------------
    # STEP 4 — Deduplicate
    # -------------------------------------------------------------
    final = []
    seen = set()
    for inp in inputs:
        k = (inp["object"], inp["attribute"])
        if k not in seen:
            seen.add(k)
            final.append(inp)

    return final


def map_general_rule_inputs(
    datamodel,
    layers: list[str],
) -> dict[str, dict[int, list[dict[str, Any]]]]:
    """
    Build a mapping of general-rule input dependencies for each layer.

    This extracts, for each general rule:
    - all referenced layers
    - all referenced attributes
    - recursively included derived variables

    Parameters
    ----------
    datamodel : ExtendedHyDAMO
        HyDAMO datamodel containing layers, validation rules, and metadata.
    layers : list[str]
        List of layer names to analyze.

    Returns
    -------
    dict
        {
          layer_name: {
            general_rule_id: [
                {"object": <layer>, "attribute": <attribute or None>},
                ...
            ]
          }
        }
    """

    # Cache columns per layer
    columns_by_layer: dict[str, set[str]] = {
        layer: set(getattr(getattr(datamodel, layer), "columns", [])) for layer in layers
    }

    validation_rules = datamodel.validation_rules
    general_lookup_by_layer = _build_general_rules_lookup_table(validation_rules)

    mapping: dict[str, dict[int, list[dict[str, Any]]]] = {}

    for layer in layers:
        mapping[layer] = {}

        general_rules = validation_rules[layer].get("general_rules", [])
        for gr in general_rules:
            gid = gr["id"]
            func = gr["function"]

            inputs = _extract_inputs_from_function(
                func=func,
                current_layer=layer,
                layers=set(layers),
                columns_by_layer=columns_by_layer,
                general_lookup_by_layer=general_lookup_by_layer,
            )

            mapping[layer][gid] = inputs

    return mapping


def _validation_mapping(
    datamodel,
    layers: list[str],
    include_topologic: bool = True,
    omit_topologic_as_none: bool = False,
) -> dict[str, dict[int, list[dict[str, Any]]]]:
    """
    Build a mapping of validation-rule input dependencies for each layer.

    This captures the exact (object, attribute) pairs required to evaluate
    each validation rule, including:
    - logic rules
    - general rules
    - optional topologic rules (depending on settings)

    Parameters
    ----------
    datamodel : ExtendedHyDAMO
        Datamodel containing layer GeoDataFrames and validation rules.
    layers : list[str]
        List of layers to process.
    include_topologic : bool, default True
        If False, topologic rules are excluded from the mapping.
    omit_topologic_as_none : bool, default False
        If True and topologic rules are excluded, insert a dummy record of the form:
            {"object": <layer>, "attribute": None}

    Returns
    -------
    dict
        Nested mapping from layer → rule-id → resolved input references.

    {
        <layer_name>: {
            <validation_rule_id>: [
                {"object": <layer>, "attribute": <attribute>},
                ...
            ],
            ...
        },
        ...
    }
    """
    # Cache columns per layer
    columns_by_layer: dict[str, set[str]] = {}
    for layer in layers:
        gdf = getattr(datamodel, layer)
        columns_by_layer[layer] = set(getattr(gdf, "columns", []))

    # Prebuild derived-variable lookups per layer
    validation_rules = getattr(datamodel, "validation_rules")
    general_lookup_by_layer = _build_general_rules_lookup_table(validation_rules)

    mapping: dict[str, dict[int, list[dict[str, Any]]]] = {}
    for layer in layers:
        mapping[layer] = {}
        ruleset = validation_rules.get(layer, {})
        vrules = ruleset.get("validation_rules", [])

        for rule in vrules:
            rid = rule.get("id", None)
            func = rule.get("function", {})
            is_topologic = rule.get("type", "") == "topologic"

            if rid is None:
                # Skip invalid rule entries
                continue

            inputs = _extract_inputs_from_function(
                func=func,
                current_layer=layer,
                layers=set(layers),
                columns_by_layer=columns_by_layer,
                general_lookup_by_layer=general_lookup_by_layer,
            )

            if is_topologic and not include_topologic:
                # Omit or emit a recognizable placeholder
                mapping[layer][rid] = [{"object": layer, "attribute": None}] if omit_topologic_as_none else []
            else:
                mapping[layer][rid] = inputs

    return mapping


def _validation_iterations(mapping: dict) -> dict[str, int]:
    """
    Assign an execution round to each layer based on inter-layer dependencies.

    Layers with no references to other layers are assigned round 1. Layers that
    depend on layers in round N are assigned at least round N+1. Rounds are
    resolved iteratively until stable, handling transitive dependencies.

    Parameters
    ----------
    mapping : dict
        Nested mapping from layer -> {validation_id: [{"object": ..., "attribute": ...}]}.

    Returns
    -------
    dict[str, int]
        Mapping from each layer name to its execution round number.
    """
    top_level_keys = set(mapping.keys())

    # Build a dependency set per top-level key:
    # which OTHER top-level keys does it reference across all its values?
    dependencies: dict[str, set[str]] = {key: set() for key in top_level_keys}

    for key, checks in mapping.items():
        for check_list in checks.values():
            for entry in check_list:
                referenced_object = entry.get("object")
                if referenced_object and referenced_object in top_level_keys:
                    if referenced_object != key:  # exclude self-references
                        dependencies[key].add(referenced_object)

    # Iteratively assign rounds until stable.
    # Start everyone at round 1, then propagate upward.
    rounds: dict[str, int] = {key: 1 for key in top_level_keys}

    changed = True
    while changed:
        changed = False
        for key, deps in dependencies.items():
            if deps:
                required_round = max(rounds[dep] for dep in deps) + 1
                if required_round > rounds[key]:
                    rounds[key] = required_round
                    changed = True

    return rounds


def _get_validation_ids_for_attribute(mapping: dict, object_layer: str, attribute_name: str) -> list[int]:
    """
    Return validation IDs that reference a specific attribute of a given layer.

    Parameters
    ----------
    mapping : dict
        Nested dict mapping layer -> {validation_id: [{"object": ..., "attribute": ...}]}.
    object_layer : str
        Layer name to look up (e.g. ``"stuw"``).
    attribute_name : str
        Attribute name to match (e.g. ``"hoogteconstructie"``).

    Returns
    -------
    list[int]
        Validation IDs where the ``object_layer`` + ``attribute_name`` combination
        is found in the mapping.
    """
    layer_checks = mapping.get(object_layer, {})

    return [
        int(index)
        for index, entries in layer_checks.items()
        if any(entry.get("object") == object_layer and entry.get("attribute") == attribute_name for entry in entries)
    ]


def _validation_ids(validation_mapping: dict) -> dict[str, dict[str, list[int]]]:
    """
    Build a full mapping of validation IDs for every attribute of every layer.

    Parameters
    ----------
    validation_mapping : dict
        Nested dict mapping layer -> {validation_id: [{"object": ..., "attribute": ...}]}.

    Returns
    -------
    dict[str, dict[str, list[int]]]
        Mapping from layer -> {attribute_name: [validation_ids]}.
        Example: ``{'duikersifonhevel': {'hoogteopening': [10, 11], 'breedteopening': [12]}}``
    """
    result: dict[str, dict[str, list[int]]] = {}
    for object_layer, layer_checks in validation_mapping.items():
        # Collect all attributes that belong to this layer
        attributes: set[str] = set()
        for entries in layer_checks.values():
            for entry in entries:
                if entry.get("object") == object_layer and entry.get("attribute") is not None:
                    attributes.add(entry["attribute"])

        layer_result: dict[str, list[int]] = {}
        for attribute in sorted(attributes):
            ids = _get_validation_ids_for_attribute(validation_mapping, object_layer, attribute)
            if ids:
                layer_result[attribute] = ids

        result[object_layer] = layer_result
    return result


def _fix_iterations(
    validation_rules: dict,
    validation_mapping: dict,
    validation_ids: dict[str, dict[str, list[int]]],
    layer_mapping: dict = LAYER_MAPPING,
) -> dict[str, dict[int, list[int]]]:
    """
    Build a fix iteration dict based on LAYER_MAPPING attribute priorities.

    For each object layer, groups fix rules by iteration key from LAYER_MAPPING.
    Within each iteration group, fix rules are ordered by the total number of input
    parameters across their associated validation rules (fewest dependencies first).

    Args:
        validation_rules: Raw validation rules dict (e.g. datamodel.validation_rules).
            Expected structure: validation_rules[layer]["fix_rules"] -> list of fix rule dicts
            (each with at least 'fix_id' and 'attribute_name').
        validation_mapping: Nested dict mapping layer -> {validation_id: [{"object": ..., "attribute": ...}]}.
        validation_ids: Precomputed dict mapping layer -> {attribute_name: [validation_ids]}.
            As produced by _validation_ids(validation_mapping).
        layer_mapping: Dict mapping layer -> {iteration_num: [attribute_names]}.
            Defaults to LAYER_MAPPING.

    Returns:
        Dict mapping layer -> {iteration_num: [fix_ids]} where fix IDs within each
        iteration group are sorted by total validation input dependency count (ascending),
        then by fix_id (ascending).

    Example:
        {
            "duikersifonhevel": {
                1: [],
                2: [10],
                3: [11, 12],
            }
        }
    """
    # Derive fix_rules_by_layer from validation_rules; include all layers, even those without fix rules
    fix_rules_by_layer: dict[str, list[dict]] = {
        layer: ruleset.get("fix_rules", []) for layer, ruleset in validation_rules.items()
    }

    iteration: dict[str, dict[int, list[int]]] = {}

    for object_layer, fix_rules in fix_rules_by_layer.items():
        # Layers without a LAYER_MAPPING entry or without fix rules get an empty dict
        if object_layer not in layer_mapping or not fix_rules:
            iteration[object_layer] = {}
            continue

        attribute_iterations = layer_mapping[object_layer]  # {iteration_num: [attribute_names]}

        # Build a reverse lookup: attribute_name -> iteration_num
        attribute_to_iteration: dict[str, int] = {}
        for iteration_num, attributes in attribute_iterations.items():
            for attribute in attributes:
                attribute_to_iteration[attribute] = iteration_num

        # Pre-populate all iteration keys from LAYER_MAPPING, including empty ones
        # {iteration_num: [(dep_count, fix_id)]}
        iteration_groups: dict[int, list[tuple[int, int]]] = {iter_num: [] for iter_num in attribute_iterations}

        # Iteration number for attributes not covered by LAYER_MAPPING
        overflow_iteration = max(attribute_iterations.keys()) + 1 if attribute_iterations else 1

        layer_checks = validation_mapping.get(object_layer, {})

        for rule in fix_rules:
            fix_id: int = rule["fix_id"]
            attribute_name: str = rule["attribute_name"]

            # Attributes not in LAYER_MAPPING go into the overflow iteration bucket
            rule_iteration = attribute_to_iteration.get(attribute_name, overflow_iteration)

            # Count total input parameters across all validation rules associated
            # with this fix rule (via validation_ids lookup)
            attr_val_ids = validation_ids.get(object_layer, {}).get(attribute_name, [])
            dep_count = sum(len(entries) for key, entries in layer_checks.items() if int(key) in attr_val_ids)

            if rule_iteration not in iteration_groups:
                iteration_groups[rule_iteration] = []  # fallback for iterations not in LAYER_MAPPING
            iteration_groups[rule_iteration].append((dep_count, fix_id))

        # Sort each iteration group by dep_count ascending, then fix_id ascending
        layer_iteration: dict[int, list[int]] = {}
        for iter_num, id_tuples in sorted(iteration_groups.items()):
            sorted_ids = [fid for _, fid in sorted(id_tuples, key=lambda x: (x[0], x[1]))]
            layer_iteration[iter_num] = sorted_ids

        iteration[object_layer] = layer_iteration

    return iteration
