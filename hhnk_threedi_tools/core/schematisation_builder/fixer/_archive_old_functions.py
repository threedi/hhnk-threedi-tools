def create_validation_fix_reports(self):
    ## FIXME: populate FixSummary class
    """
    Create validation and fix overview report
    Inputs:
        1. HyDAMO.gpkg
        2. validation results gpkg (results.gpkg in validation directory)
        3. validation_rules.json
    Report includes per layer:
        - current attribute values (from hydamo.gpkg)
        - colomn for manual adjustments (see manual_overwrite_* columns)
        - validation summary per attribute
        - fix suggestions per attribute
        - summary columns (similar to validation results gpkg)
    Returns:
        NoneSS
    """
    # create report gpkg with per layer a summary of validation and fix suggestions
    for layer in self.fix_config["objects"]:
        layer_name = layer["object"]

        self.logger.info(f"Start creating validation and fix summary for layer: {layer_name}")

        # open hydamo layer gdf
        hydamo_layer_gdf = gpd.read_file(self.hydamo_file_path, layer=layer_name)

        # fill in standard columns based on validation results
        val_results_layer_gdf = gpd.read_file(self.validation_results_gpkg_path, layer=layer_name)

        layer_report_gdf = val_results_layer_gdf[
            ["code", "geometry", "valid", "invalid_critical", "invalid_non_critical", "ignored"]
        ].copy()

        # remove rows where invalid columns are empty strings
        layer_report_gdf["invalid_critical"] = (
            layer_report_gdf["invalid_critical"].fillna("").astype(str)
            + layer_report_gdf["invalid_critical"].apply(lambda x: "; " if x else "")
            + layer_report_gdf["ignored"].fillna("").astype(str)
        )
        layer_report_gdf["invalid_critical"] = layer_report_gdf["invalid_critical"].replace("", None)
        layer_report_gdf["invalid_non_critical"] = layer_report_gdf["invalid_non_critical"].replace("", None)

        if layer_report_gdf.empty:
            self.logger.info(
                f"No invalid features found in layer {layer_name}, fixing is not needed/finished for this layer."
            )
            return

        self.logger.info(f"Created base report gdf with {len(layer_report_gdf)} objects which need fixes")

        # add layer specific columns based on fix config
        add_specific_columns = []
        for fix in layer["fixes"]:
            attribute_name = fix["attribute_name"]
            if attribute_name not in add_specific_columns:
                add_specific_columns.append(attribute_name)
                # # add columns to layer report gdf
                if attribute_name != "geometry":
                    #     layer_report_gdf[attribute_name] = hydamo_layer_gdf[attribute_name] if attribute_name in hydamo_layer_gdf.columns else None
                    layer_report_gdf[attribute_name] = None
                layer_report_gdf[f"validation_sum_{attribute_name}"] = None
                layer_report_gdf[f"fixes_{attribute_name}"] = None
                layer_report_gdf[f"manual_overwrite_{attribute_name}"] = None

        layer_report_gdf = layer_report_gdf.dropna(subset=["invalid_critical", "invalid_non_critical"], how="all")
        self.logger.info(f"Added specific columns to report gdf for following attributes: {add_specific_columns}")

        # fill in validation and fix information
        self.logger.info(f"Filling in validation, fix and attribute information for layer: {layer_name}")
        list_attributes_filled = []
        for index, row in layer_report_gdf.iterrows():
            # connect IDs of invalid_critical to validation_id in fix config
            if row["invalid_critical"] is not None or row["invalid_non_critical"] is not None:
                invalid_ids = []
                if row["invalid_critical"] is not None:
                    invalid_ids += [int(x) for x in row["invalid_critical"].split(";")]
                if row["invalid_non_critical"] is not None:
                    invalid_ids += [int(x) for x in row["invalid_non_critical"].split(";")]

                for attribute_fix in layer["fixes"]:
                    validation_ids = attribute_fix["validation_ids"]
                    attribute_name = attribute_fix["attribute_name"]
                    fix_method = attribute_fix["fix_method"]
                    fix_id = attribute_fix["fix_id"]
                    fix_description = attribute_fix["fix_description"]

                    if any(validation_id in invalid_ids for validation_id in validation_ids):
                        # mark attribute as filled
                        if attribute_name not in list_attributes_filled:
                            list_attributes_filled.append(attribute_name)

                        # open validation_rules.json for specific layer
                        validation_rules_layer = self._select_validation_rules_layer(layer_name)

                        # Loop through all validation ids of attribute fix which are present in invalid ids'
                        for validation_id in validation_ids:
                            if validation_id in invalid_ids:
                                # based on validation_rules.json, check error type and message
                                for rule in validation_rules_layer:
                                    if rule["id"] == validation_id:
                                        # define validation sum text
                                        if rule["error_type"] == "critical":
                                            text_val_sum = f"C{validation_id}:{rule['error_message']}"
                                        else:
                                            text_val_sum = f"W{validation_id}:{rule['error_message']}"

                                        # fill in the validation sum column
                                        current_val_sum = layer_report_gdf.at[
                                            index, f"validation_sum_{attribute_name}"
                                        ]
                                        if current_val_sum is None:
                                            layer_report_gdf.at[index, f"validation_sum_{attribute_name}"] = (
                                                text_val_sum
                                            )
                                        else:
                                            layer_report_gdf.at[index, f"validation_sum_{attribute_name}"] = (
                                                f"{current_val_sum};{text_val_sum}"
                                            )

                        # define fix suggestion text
                        if attribute_fix["fix_type"] == "automatic":
                            text_fix_suggestion = f"AF{fix_id}:{fix_description}"
                        else:
                            text_fix_suggestion = f"MF{fix_id}:{fix_description}"

                        # fill in fix suggestion column
                        current_fix = layer_report_gdf.at[index, f"fixes_{attribute_name}"]
                        if current_fix is None:
                            layer_report_gdf.at[index, f"fixes_{attribute_name}"] = text_fix_suggestion
                        else:
                            layer_report_gdf.at[index, f"fixes_{attribute_name}"] = (
                                f"{current_fix};{text_fix_suggestion}"
                            )

                        # fill in attribute value if present in hydamo layer
                        if attribute_name in hydamo_layer_gdf.columns and attribute_name != "geometry":
                            code = row["code"]
                            if fix_id == 2 and "equal" in list(fix_method.keys()):
                                hydamo_value = hydamo_layer_gdf[fix_method["equal"]["to"]].values
                            else:
                                hydamo_value = hydamo_layer_gdf.loc[
                                    hydamo_layer_gdf["code"] == code, attribute_name
                                ].values
                            if len(hydamo_value) > 0:
                                layer_report_gdf.loc[index, attribute_name] = hydamo_value[0]
                            else:
                                self.logger.warning(
                                    f"Could not find attribute value for code {code} and attribute {attribute_name}"
                                )
        print(layer_report_gdf["breedteopening"])

        self.logger.info(
            f"Filled in validation,fix and attribute information for {list_attributes_filled} attributes in layer {layer_name}"
        )

        # remove columns with no values filled in
        cols_to_save = ["code", "geometry", "valid", "invalid_critical", "invalid_non_critical", "ignored"]
        for attribute_name in list_attributes_filled:
            cols_to_save += [
                attribute_name,
                f"validation_sum_{attribute_name}",
                f"fixes_{attribute_name}",
                f"manual_overwrite_{attribute_name}",
            ]
        layer_report_gdf = layer_report_gdf[cols_to_save]

        # save layer report gdf to report gpkg
        layer_report_gdf.to_file(self.report_gpkg_path, layer=layer_name, driver="GPKG")
        self.logger.info(f"Finshed and saved report gdf for layer {layer_name} to {self.report_gpkg_path}")

        self.fix_overview[layer_name] = layer_report_gdf


def _select_validation_rules_layer(self, layer_name: str):
    """
    Select validation rules for a specific layer from the validation_rules.json
    Parameters:
        layer_name (str): Name of the layer to select validation rules for.
    Returns:
        List of validation rules for the specified layer.
    """
    for rules_layer in self.validation_rules["objects"]:
        if rules_layer["object"] == layer_name:
            return rules_layer["validation_rules"]
    return []  # Return empty list if layer not found


# Example function
# TODO: list_features could also be summary validation/fix dataframe with codes to remove.
def omit_features(
    gdf_HyDAMO: gpd.GeoDataFrame, layer: str, list_features: list, logger: logging.Logger
) -> "gpd.GeoDataFrame":
    """Remove features from the HyDAMO geodataframe.

    Args:
        gdf_HyDAMO (gpd.GeoDataFrame): HyDAMO geodataframe.
        layer (str): The layer in which features need to be asingned as not usable.
        list_features (list): List of old feature IDs to remove.

    Returns:
        gpd.GeoDataFrame: HyDAMO geodataframe with extra column 'is_usable' indicating features which arenot usable.
    """
    features_layer = gdf_HyDAMO
    if not features_layer.empty:
        try:
            if "is_usable" not in features_layer.columns:
                features_layer["is_usable"] = None
            # features_layer_adjusted = features_layer[~features_layer["code"].isin(list_features)]

            # add column 'is_usable' to indicate features and set true for features in list_features
            features_layer_adjusted = features_layer.copy()
            features_layer_adjusted["is_usable"] = (
                True  ## FIXME: meer het idee om bij te houden welke features wel of niet gebruikt worden. is_usable is beter
            )
            features_layer_adjusted.loc[features_layer_adjusted["code"].isin(list_features), "is_usable"] = False

            features_layer.update(features_layer_adjusted)
            logger.info(f"Indicated {len(list_features)} features as not usable in layer {layer}.")
        except Exception as e:
            logger.error(f"Error indicating features as not usable in layer {layer}: {e}")
            pass
    else:
        logger.warning(f"No features found in layer {layer} which are not usable.")

    return features_layer


def edit_features(
    gdf_HyDAMO: gpd.GeoDataFrame, layer: str, attribute_name: str, value, logger: logging.Logger
) -> "gpd.GeoDataFrame":
    """Edits features from the HyDAMO geodataframe.

    Args:
        gdf_HyDAMO (gpd.GeoDataFrame): HyDAMO geodataframe.
        layer (str): The layer in which features need to be edited.
        attribute_name (str): The attribute that needs to be edited

    Returns:
        gpd.GeoDataFrame: HyDAMO geodataframe with extra column 'is_usable' indicating features which arenot usable.
    """
    features_layer = gdf_HyDAMO
    if not features_layer.empty:
        try:
            if "is_usable" not in features_layer.columns:
                features_layer["is_usable"] = None
            # features_layer_adjusted = features_layer[~features_layer["code"].isin(list_features)]

            # add column 'is_usable' to indicate features and set true for features in list_features
            features_layer_adjusted = features_layer.copy()
            features_layer_adjusted["is_usable"] = (
                True  ## FIXME: meer het idee om bij te houden welke features wel of niet gebruikt worden. is_usable is beter
            )
            features_layer_adjusted.loc[features_layer_adjusted["code"].isin(list_features), "is_usable"] = False

            features_layer.update(features_layer_adjusted)
            logger.info(f"Indicated {len(list_features)} features as not usable in layer {layer}.")
        except Exception as e:
            logger.error(f"Error indicating features as not usable in layer {layer}: {e}")
            pass
    else:
        logger.warning(f"No features found in layer {layer} which are not usable.")

    return features_layer


def _check_attributes(func: dict, validation_rules: dict, attributes: str):
    all_attributes = []
    if func:
        func_name = list(func.keys())[0]
        func_contents: dict = func[func_name]
        for key, item in func_contents.items():
            if item in attributes:
                all_attributes.append(item)
            elif item in _general_rule_variables(validation_rules):
                pass


def _general_rule_variables(validation_rules: dict):
    general_rules = validation_rules.get("general_rules", [])
    variables = [general_rule["attribute_name"] for general_rule in general_rules]
    return variables


def _get_related_attributes(input_dict):
    pass


def _read_validation_rules(gdf: gpd.GeoDataFrame, validation_rules: list[dict], attribute: str):
    columns = gdf.columns
    related_attributes = []
    general_rule_variables = _general_rule_variables(validation_rules)
    for rule in validation_rules:
        func = rule["function"]
        func_name = list(func.keys())[0]
        func_contents: dict = func[func_name]
        func_inputs = func_contents.values()
        func_attribute_inputs = [func_input for func_input in func_inputs if func_input in columns]
        func_general_inputs = [func_input for func_input in func_inputs if func_input in general_rule_variables]

        general_rule_inputs = _check_general_rule_attributes(func_inputs)
        attributes = []
        related_attributes.extend(attributes)
    return related_attributes


def check(
    datamodel: ExtendedHyDAMO,
    layerssummary: ExtendedLayersSummary,
    resultsummary: ExtendedResultSummary,
    layers: list[str],
    columns: list[str],
    logger: logging.Logger,
    raise_error: bool,
):
    # input_mapping_general = ...
    mapped_inputs = map_validation_rule_inputs(datamodel, layers)
    # mapped_inputs_fix = map_fix_rule_inputs()

    ## --> set mappings as property in layerssummary

    # layerssummary.mapping["validation_rules"]

    for layer in layers:
        gdf = getattr(datamodel, layer)
        fix_gdf = prepare(
            gdf,
            layer=layer,
            schema={},  ## schema is most likely needed to account for ignored validation rules
            validation_schema=datamodel.validation_schemas[layer],
            validation_result=datamodel.validation_results[layer],
            validation_rules=datamodel.validation_rules[layer],
            keep_columns=columns,
            logger=logger,
            raise_error=raise_error,
            datamodel=datamodel,
        )
        layerssummary.set_data(fix_gdf, layer, "FIXME")

    return layerssummary


def prepare(  # maybe rename to check() !!! When redoing a fix_overview creation, keep the original manual overwrite columns, but replace the rest of the columns based on new rules.
    gdf: gpd.GeoDataFrame,
    layer: str,
    schema,
    validation_schema,
    validation_result: gpd.GeoDataFrame,
    validation_rules: dict,
    keep_columns,
    logger: logging.Logger,
    raise_error,
    datamodel: ExtendedHyDAMO = None,
):
    """
    Create validation and fix overview report
    Inputs:
        1. gdf: HyDAMO layer geodataframe
        2. validation results gpkg (results.gpkg in validation directory)
        3. validation_rules.json
        4. FixConfig.json
    Report includes per layer:
    - current attribute values (from hydamo.gpkg)
    - colomn for manual adjustments (see manual_overwrite_* columns)
    - validation summary per attribute
    - fix suggestions per attribute
    - summary columns (similar to validation results gpkg)
    Returns:
        NoneSS
    """
    ## Read the fix rules on attributes and make an overview of which validation rules are connected to this attribute
    ## There should be fix for every attribute that has a validation rule

    # create report gpkg with per layer a summary of validation and fix suggestions
    ## if validation_result.empty or not validation_rules: raise error
    logger.info(f"Start creating validation and fix summary for layer: {layer}")

    layer_name = layer
    rules = validation_rules
    validation_rules = rules.get("validation_rules", None)
    fix_rules = rules.get("fix_rules", None)

    if not rules:
        logger.info(f"Validation rules set not filled in for {layer_name}. Creating empty dataframe.")
        layer_report_gdf = gpd.GeoDataFrame(columns=keep_columns)
    else:
        layer_report_gdf = validation_result[
            ["code", "geometry", "valid", "invalid_critical", "invalid_non_critical", "ignored"]
        ].copy()

    if not validation_rules or not fix_rules:
        logger.info(f"Quitting. Validation rules or fix rules for layer {layer_name} unknown.")
        return layer_report_gdf

    # remove rows where invalid columns are empty strings
    layer_report_gdf["invalid_critical"] = (
        layer_report_gdf["invalid_critical"].fillna("").astype(str)
        + layer_report_gdf["invalid_critical"].apply(lambda x: "; " if x else "")
        + layer_report_gdf["ignored"].fillna("").astype(str)
    )
    layer_report_gdf["invalid_critical"] = layer_report_gdf["invalid_critical"].replace("", None)
    layer_report_gdf["invalid_non_critical"] = layer_report_gdf["invalid_non_critical"].replace("", None)

    if layer_report_gdf.empty:
        logger.info(f"No invalid features found in layer {layer_name}, fixing is not needed/finished for this layer.")
        return layer_report_gdf

    logger.info(f"Created base report gdf with {len(layer_report_gdf)} objects which need fixes")

    # add layer specific columns based on fix config
    add_specific_columns = []
    """
    {
        profiellijn: {},
        duikersifonhevel: {
            0: [{layer: duikersifonhevel, attribute: hoogtebinnenonderkantbov]},
            1: [{layer: duikersifonhevel, attribute: hoogtebinnenonderkantbene]},
            2: [{layer: duikersifonhevel, attribute: hoogtebinnenonderkantbov]},
            3: [{layer: duikersifonhevel, attribute: hoogtebinnenonderkantbene]},
            4: [{layer: duikersifonhevel, attribute: lengte]},
            5: [{layer: duikersifonhevel, attribute: breedteopening}, {layer: duikersifonhevel, attribute: hoogteopening}],
            6: [], # topological function -> set to omit
            7: [{layer: duikersifonhevel, attribute: hoogtebinnenonderkantbov}, {layer: duikersifonhevel, attribute: hoogtebinnenonderkantbene}, {layer: duikersifonhevel, attribute: lengte}]
            8: [{layer: duikersifonhevel, attribute: hoogtebinnenonderkantbov}, {layer: duikersifonhevel, attribute: hoogtebinnenonderkantbene}]
            9: [], # topological function -> set to omit
            10: [{layer: duikersifonhevel, attribute: hoogtebinnenonderkantbov}, {layer: profiellijn, attribute: bodemhoogte}],
            11: [{layer: duikersifonhevel, attribute: hoogtebinnenonderkantbene}, {layer: profiellijn, attribute: bodemhoogte}]
        },
    }


    attribute + validation rule order:
    [hoogtebinnenonderkantbov, fix, 0_fix_description, 2_fix_description, 7_fix_description, 8_fix_description, 10_fix_description, fix_rating, hoogtebinnenonderkantbene, 1, 3, 7, 8, 11, lengte, 4, 7, vormkoker, 5, breedteopening, 5]
    [hoogtebinnenonderkantbov, val_sum_hoogtebinnenonderkantbov                                  , fix_hoogtebinnenonderkantbov, fix_check_hoogtebinnenonderkantbov, fix_history_boogtebinnenonderkantbov                                                                 ]
    [None                    , C0:bok_boven niet plausibel;C2bok_boven > maaiveld;W7:verhang niet tussen 2 tot 5 cm/m;W8:verval niet kleiner dan 50cm;C10:bok bovenstrooms beneden bodem, 
        AA:hoogtebinnenonderkantbov - 0,2, 0:Invalid;2:Invalid;7:Valid; ]
        - Styling: Als waarde hoogtebinnenonderkantbov and {string statement} = True --> groen, else --> rood
    fix_rating: indicate which fixes are fixed when applying the eventual fix
        - Use chosen/filled in value of attribute to check validity of every rule associated using code from validator
    fix_hierarchy: check for a fix whether an input is another attribute/layer. Fixes without a relation will be done last
    What to do with nr_of_profielpunten: how does this get into the fix_overview
        - If a validation_id has no column names as input, then add the general column name and show why it is not valid. You cannot fix it, so then omit the feature

    """
    attributes_in_validation = ...
    """ {
        0: [hoogtebinnenonderkantbov],
        1: [hoogtebinnenonderkantbene],
        2: [hoogtebinnenonderkantbov],
        3: [hoogtebinnenonderkantbene],
        4: [lengte],
        5: [vormkoker, breedteopening, {layer: duikersifonhevel, attribute: hoogteopening}],
        6: [], # empty, when the function is topological
        7: [hoogtebinnenonderkantbov, hoogtebinnenonderkantbene, lengte]
        8: [hoogtebinnenonderkantbov, hoogtebinnenonderkantbene]
        9: [], # topological function
        10: [hoogtebinnenonderkantbov,]


    }
    """
    ## column names: {layer}__{attribute}?
    ## use an _is_valid() to check whether parameters have been validated or are valid (?)
    ## IDEA: make layers for every attribute that need fixing:
    ##      fix_overview__duikersifonhevel__hoogtebinnenonderkantbov
    ##      fix_overview__duikersifonhevel__hoogtebinnenonderkantbene
    ##      etc
    # _check_necessary_fixes() --> check if a fix exists for all validation ids for at least one parameter

    ## prepare fixes and create fix_overview
    ## create temp hydamo and execute fixes
    ## run validation and save results.gpkg
    ## read results.gpkg and style the contents of fix_overview according to which fixes worked (?)

    for fix in fix_rules:
        attribute_name = fix["attribute_name"]
        attribute_names = []  ## attribute and attributes it is related to
        # other_attributes = _read_validation_rules(validation_rules, attribute_name)

        # attribute_name_test = read_attributes() ## for an object, read the
        ## think about if we can read the attribute name and deduce the validation ids from it.
        ## every validation_rule_id should be in the
        ## so, for instance a rule is dependent on slope. slope is depenedent on delta_h and lengte
        ## lengte is an attribute, delta_h is a general_rule variable dependent on hoogtebinnenonderkantbov and hoogtebinnenonderkantbene.
        ## So: the slope fix should be based on lengte, hoogtebinnenonderkantbov and hoogtebinnenonderkantbene
        ## another rule, verval is also dependent on hoogtebinnenonderkantbov and hoogtebinnenonderkantbene
        ## so: lengte, hoogtebinnenonderkantbov and hoogtebinnenonderkantbene should be next to each other, and next to them should be the fix overview of the slope and verval fix rules

        if attribute_name not in add_specific_columns:
            add_specific_columns.append(attribute_name)
            # # add columns to layer report gdf
            if attribute_name != "geometry":
                #     layer_report_gdf[attribute_name] = hydamo_layer_gdf[attribute_name] if attribute_name in hydamo_layer_gdf.columns else None
                layer_report_gdf[attribute_name] = None
            layer_report_gdf[f"validation_sum_{attribute_name}"] = None
            layer_report_gdf[f"fixes_{attribute_name}"] = None
            layer_report_gdf[f"fix_checks_{attribute_name}"] = None
            layer_report_gdf[f"manual_overwrite_{attribute_name}"] = None

    layer_report_gdf = layer_report_gdf.dropna(subset=["invalid_critical", "invalid_non_critical"], how="all")
    logger.info(f"Added specific columns to report gdf for following attributes: {add_specific_columns}")

    # fill in validation and fix information
    hydamo_check = datamodel
    gdf_check = getattr(hydamo_check, layer_name)

    logger.info(f"Filling in validation, fix and attribute information for layer: {layer_name}")
    list_attributes_filled = []
    for index, row in layer_report_gdf.iterrows():
        # connect IDs of invalid_critical to validation_id in fix config
        if row["invalid_critical"] is not None or row["invalid_non_critical"] is not None:
            invalid_ids = []
            if row["invalid_critical"] is not None:
                invalid_ids += [int(x) for x in row["invalid_critical"].split(";")]
            if row["invalid_non_critical"] is not None:
                invalid_ids += [int(x) for x in row["invalid_non_critical"].split(";")]

            for attribute_fix in fix_rules:
                ## related_validation_rules --> rules where the input is one of the attributes
                ## related_ids --> corresponding ids

                validation_ids = attribute_fix["validation_ids"]
                attribute_name = attribute_fix["attribute_name"]
                ## validation_ids = get_ids_related_to_attribute()
                fix_method = attribute_fix["fix_method"]
                fix_id = attribute_fix["fix_id"]
                fix_description = attribute_fix["fix_description"]

                if any(validation_id in invalid_ids for validation_id in validation_ids):
                    # mark attribute as filled
                    if attribute_name not in list_attributes_filled:
                        list_attributes_filled.append(attribute_name)

                    # Loop through all validation ids of attribute fix which are present in invalid ids'
                    for validation_id in validation_ids:
                        if validation_id in invalid_ids:
                            # based on validation_rules.json, check error type and message
                            for rule in validation_rules:
                                if rule["id"] == validation_id:
                                    # define validation sum text
                                    if rule["error_type"] == "critical":
                                        text_val_sum = f"C{validation_id}:{rule['error_message']}"
                                    else:
                                        text_val_sum = f"W{validation_id}:{rule['error_message']}"

                                    # fill in the validation sum column
                                    current_val_sum = layer_report_gdf.at[index, f"validation_sum_{attribute_name}"]
                                    if current_val_sum is None:
                                        layer_report_gdf.at[index, f"validation_sum_{attribute_name}"] = text_val_sum
                                    else:
                                        layer_report_gdf.at[index, f"validation_sum_{attribute_name}"] = (
                                            f"{current_val_sum};{text_val_sum}"
                                        )

                    # define fix suggestion text
                    if attribute_fix["fix_type"] == "automatic":
                        text_fix_suggestion = f"AF{fix_id}:{fix_description}"
                    else:
                        text_fix_suggestion = f"MF{fix_id}:{fix_description}"

                    # fill in fix suggestion column
                    current_fix = layer_report_gdf.at[index, f"fixes_{attribute_name}"]
                    if current_fix is None:
                        layer_report_gdf.at[index, f"fixes_{attribute_name}"] = text_fix_suggestion
                    else:
                        layer_report_gdf.at[index, f"fixes_{attribute_name}"] = f"{current_fix};{text_fix_suggestion}"

                    # fill in attribute value if present in hydamo layer
                    if attribute_name in gdf_check.columns and attribute_name != "geometry":
                        ## ADD CHECK FOR ATTRIBUTE THAT HAS MULTIPLE VALIDATION IDS WITH MULTIPLE INPUTS
                        ## USE A FIX_ITER FUNCTION

                        ## Fill in the attribute data based on hydamo and based on fix_rules
                        ## For now, say that the highest fix id that is not omit is used to fill in the fixed version
                        ## --> Needs to be based on the input mapping

                        ## !!!!!!!!!!!!!!!!!! HERE WE POPULATE THE DATAMODEL COPY, AFTERWARDS WE COPY DATA TO LAYERREPORT
                        code = row["code"]
                        if fix_id == 2 and "equal" in list(fix_method.keys()):
                            hydamo_value = [fix_method["equal"]["to"]]
                        else:
                            hydamo_value = gdf_check.loc[gdf_check["code"] == code, attribute_name].values
                        if len(hydamo_value) > 0:
                            hydamo_value = hydamo_value[0]
                            if isinstance(hydamo_value, str) and hydamo_value in gdf_check.columns:
                                gdf_check.loc[index, attribute_name] = gdf_check.loc[index, hydamo_value]
                                layer_report_gdf.loc[index, attribute_name] = gdf_check.loc[index, attribute_name]
                            else:
                                gdf_check.loc[index, attribute_name] = hydamo_value
                                layer_report_gdf.loc[index, attribute_name] = gdf_check.loc[index, attribute_name]
                        else:
                            logger.warning(
                                f"Could not find attribute value for code {code} and attribute {attribute_name}"
                            )

                ## Here, use the validation ids and their inputs to run the functions again with the inputs in the validation settings
                check_overview = None
                for validation_id in validation_ids:
                    if validation_id in invalid_ids:
                        validation_rule = next((rule for rule in validation_rules if rule["id"] == validation_id), {})
                        validation_type = validation_rule["type"]
                        validation_function = validation_rule["function"]
                        validation_function_name = next(iter(validation_function))
                        validation_function_inputs = validation_function[validation_function_name]

                        if "related_object" in validation_function_inputs.keys():
                            validation_function_inputs = _add_related_gdf(
                                validation_function_inputs, hydamo_check, layer_name
                            )
                        elif "custom_function_name" in validation_function_inputs.keys():
                            validation_function_inputs["hydamo"] = hydamo_check
                        elif "join_object" in validation_function_inputs.keys():
                            validation_function_inputs = _add_join_gdf(validation_function_inputs, hydamo_check)

                        if validation_type == "logic":
                            ## the layer_report_gdf needs to have the right columns. otherwise check will fail
                            ## make a gdf that combines all the input columns maybe?
                            if gdf_check.loc[[index]].empty:
                                result = np.nan
                            else:
                                result = _process_logic_function(
                                    gdf_check.loc[[index]], validation_function_name, validation_function_inputs
                                ).values[0]  ## gdf / series that gives true or false
                        validity_check = "Valid" if result else "Invalid"
                        if check_overview is None:
                            check_overview = ""
                        if check_overview:
                            check_overview += ";"
                        check_overview += f"{validation_id}:{validity_check}"

                print(f"{layer} - {attribute_name} - {check_overview}")
                layer_report_gdf.loc[index, f"fix_checks_{attribute_name}"] = check_overview
                ## MAYBE BETTER: UPDATE VALID, INVALID_NON_CRITICAL AND INVALID_CRITICAL SUCH THAT THE STYLING CAN SHOW WHAT FEATURES ARE NOT GOOD YET
                ## GOOD IDEA BUT DO BOTH SUCH THAT YOU CAN SEE WHICH FOR WHICH INPUT THE VALIDATION RULE IS VALID/INVALID

                ### POPULATE LAYER_REPORT_GDF WITH DATA FROM GDF_CHECK / DATAMODEL COPY

    logger.info(
        f"Filled in validation,fix and attribute information for {list_attributes_filled} attributes in layer {layer_name}"
    )

    # remove columns with no values filled in
    cols_to_save = ["code", "geometry", "valid", "invalid_critical", "invalid_non_critical", "ignored"]
    for attribute_name in list_attributes_filled:
        cols_to_save += [
            attribute_name,
            f"validation_sum_{attribute_name}",
            f"fixes_{attribute_name}",
            f"fix_checks_{attribute_name}",
            f"manual_overwrite_{attribute_name}",
        ]
    layer_report_gdf = layer_report_gdf[cols_to_save]
    logger.info(f"Finshed report gdf for layer {layer_name}")

    return layer_report_gdf


def _get_validation_summary(
    validation_ids: list[int], validation_result: gpd.GeoDataFrame, validation_rules: list[dict], indices, column
):
    gdf = validation_result.loc[indices, :]
    summary = validation_result.loc[indices, [column]]
    for index, row in gdf.iterrows():
        invalid_ids = []
        if row["invalid_critical"] != "":
            invalid_ids += [int(x) for x in row["invalid_critical"].split(";")]
        if row["invalid_non_critical"] != "":
            invalid_ids += [int(x) for x in row["invalid_non_critical"].split(";")]

        if any(validation_id in invalid_ids for validation_id in validation_ids):
            # Loop through all validation ids of attribute fix which are present in invalid ids'
            for validation_id in validation_ids:
                if validation_id in invalid_ids:
                    # based on validation_rules.json, check error type and message
                    for rule in validation_rules:
                        if rule["id"] == validation_id:
                            # define validation sum text
                            if rule["error_type"] == "critical":
                                text_val_sum = f"C{validation_id}:{rule['error_message']}"
                            else:
                                text_val_sum = f"W{validation_id}:{rule['error_message']}"

                            # fill in the validation sum column
                            current_val_sum = summary.at[index, column]
                            if current_val_sum == "":
                                summary.at[index, column] = text_val_sum
                            else:
                                summary.at[index, column] = f"{current_val_sum};{text_val_sum}"

    return summary


def execute(
    datamodel,
    validation_rules_sets,
    layers_summary,
    result_summary,
    logger=None,
    raise_error=False,
):
    """Execute the logical validation."""

    return datamodel, layers_summary, result_summary

    object_rules_sets = [i for i in validation_rules_sets["objects"] if i["object"] in datamodel.data_layers]
    logger.info(rf"lagen met valide objecten en regels: {[i['object'] for i in object_rules_sets]}")
    for object_rules in object_rules_sets:
        col_translation: dict = {}

        object_layer = object_rules["object"]
        logger.info(f"{object_layer}: start")
        object_gdf = getattr(datamodel, object_layer).copy()

        # add summary columns
        object_gdf["rating"] = 10
        for col in SUMMARY_COLUMNS:
            object_gdf[col] = ""

        # general rule section
        if "fix_rules" in object_rules.keys():
            ## sort based on hierarchy key that the user can set in fix_overview.gpkg?
            ## apply omissions
            ## then do the other fixes and filter for is_usable
            ## gdf_add_summary / history function
            pass

        if "general_rules" in object_rules.keys():
            general_rules = object_rules["general_rules"]
            general_rules_sorted = sorted(general_rules, key=lambda k: k["id"])
            for rule in general_rules_sorted:
                logger.info(f"{object_layer}: uitvoeren general-rule met id {rule['id']}")
                try:
                    result_variable = rule["result_variable"]
                    result_variable_name = f"general_{rule['id']:03d}_{rule['result_variable']}"

                    # get function
                    function = next(iter(rule["function"]))
                    input_variables = rule["function"][function]

                    # remove all nan indices
                    indices = _notna_indices(object_gdf, input_variables)
                    dropped_indices = [i for i in object_gdf.index[object_gdf.index.notna()] if i not in indices]

                    # add object_relation
                    if "related_object" in input_variables.keys():
                        input_variables = _add_related_gdf(input_variables, datamodel, object_layer)
                    elif "custom_function_name" in input_variables.keys():
                        input_variables["hydamo"] = datamodel
                    elif "join_object" in input_variables.keys():
                        input_variables = _add_join_gdf(input_variables, datamodel)

                    if dropped_indices:
                        result_summary.append_warning(
                            _nan_message(
                                len(dropped_indices),
                                object_layer,
                                rule["id"],
                                "general_rule",
                            )
                        )
                    if object_gdf.loc[indices].empty:
                        object_gdf[result_variable] = np.nan
                    else:
                        result = _process_general_function(object_gdf.loc[indices], function, input_variables)
                        object_gdf.loc[indices, result_variable] = result

                        getattr(datamodel, object_layer).loc[indices, result_variable] = result

                    col_translation = {
                        **col_translation,
                        result_variable: result_variable_name,
                    }
                except Exception as e:
                    logger.error(f"{object_layer}: general_rule {rule['id']} crashed width Exception {e}")
                    result_summary.append_error(
                        (
                            "general_rule niet uitgevoerd. Inspecteer de invoer voor deze regel: "
                            f"(object: '{object_layer}', id: '{rule['id']}', function: '{function}', "
                            f"input_variables: {input_variables}, Reason (Exception): {e})"
                        )
                    )
                    if raise_error:
                        raise e
                    else:
                        pass

        validation_rules = object_rules["validation_rules"]
        validation_rules = [i for i in validation_rules if ("active" not in i.keys()) | i["active"]]
        validation_rules_sorted = sorted(validation_rules, key=lambda k: k["id"])
        # validation rules section
        for rule in validation_rules_sorted:
            try:
                rule_id = rule["id"]
                logger.info(f"{object_layer}: uitvoeren validatieregel met id {rule_id} ({rule['name']})")
                result_variable = rule["result_variable"]
                if "exceptions" in rule.keys():
                    exceptions = rule["exceptions"]
                    indices = object_gdf.loc[~object_gdf[EXCEPTION_COL].isin(exceptions)].index
                else:
                    indices = object_gdf.index
                    exceptions = []
                result_variable_name = f"validate_{rule_id:03d}_{rule['result_variable']}"

                # get function
                function = next(iter(rule["function"]))
                input_variables = rule["function"][function]

                # remove all nan indices
                notna_indices = _notna_indices(object_gdf, input_variables)
                indices = [i for i in indices[indices.notna()] if i in notna_indices]

                # add object_relation
                if "join_object" in input_variables.keys():
                    input_variables = _add_join_gdf(input_variables, datamodel)

                # apply filter on indices
                if "filter" in rule.keys():
                    filter_function = next(iter(rule["filter"]))
                    filter_input_variables = rule["filter"][filter_function]
                    series = _process_logic_function(object_gdf, filter_function, filter_input_variables)
                    series = series[series.index.notna()]
                    filter_indices = series.loc[series].index.to_list()
                    indices = [i for i in filter_indices if i in indices]
                else:
                    filter_indices = []

                if object_gdf.loc[indices].empty:
                    object_gdf[result_variable] = None
                elif rule["type"] == "logic":
                    object_gdf.loc[indices, (result_variable)] = _process_logic_function(
                        object_gdf.loc[indices], function, input_variables
                    )
                elif (rule["type"] == "topologic") and (hasattr(datamodel, "hydroobject")):
                    result_series = _process_topologic_function(
                        # getattr(
                        #     datamodel, object_layer
                        # ),  # FIXME: commented as we need to apply filter in topologic functions as well. Remove after tests pass
                        object_gdf,
                        datamodel,
                        function,
                        input_variables,
                    )
                    object_gdf.loc[indices, (result_variable)] = result_series.loc[indices]

                col_translation = {
                    **col_translation,
                    result_variable: result_variable_name,
                }

                # summarize
                if rule["error_type"] == "critical":
                    penalty = 5
                    critical = True
                else:
                    penalty = 1
                    critical = False
                if "penalty" in rule.keys():
                    penalty = rule["penalty"]

                error_message = rule["error_message"]

                if "tags" in rule.keys():
                    tags = LIST_SEPARATOR.join(rule["tags"])
                else:
                    tags = None

                auto_fixable = rule.get("auto_fixable", False)

                exceptions += filter_indices
                _valid_indices = object_gdf[~object_gdf.index.isna()].index
                tags_indices = [i for i in _valid_indices if i not in exceptions]
                object_gdf = gdf_add_summary(
                    gdf=object_gdf,
                    variable=result_variable,
                    rule_id=rule_id,
                    penalty=penalty,
                    error_message=error_message,
                    critical=critical,
                    tags=tags,
                    tags_indices=tags_indices,
                    auto_fixable=auto_fixable,
                )

            except Exception as e:
                logger.error(f"{object_layer}: validation_rule {rule['id']} width Exception {e}")
                result_summary.append_error(
                    (
                        "validation_rule niet uitgevoerd. Inspecteer de invoer voor deze regel: "
                        f"(object '{object_layer}', rule_id '{rule['id']}', function: '{function}', "
                        f"input_variables: {input_variables}, Reason (Exception): {e})"
                    )
                )
                if raise_error:
                    raise e
                else:
                    pass

        # drop columns
        drop_columns = [
            i
            for i in object_gdf.columns
            if i not in list(col_translation.keys()) + ["nen3610id", "geometry", "rating"] + SUMMARY_COLUMNS
        ]
        object_gdf.drop(columns=drop_columns, inplace=True)
        # re_order columns
        column_order = ["nen3610id"]
        column_order += list(col_translation.keys())
        column_order += ["rating"] + SUMMARY_COLUMNS
        if "geometry" in object_gdf.columns:
            column_order += ["geometry"]
        object_gdf = object_gdf[column_order]

        # finish result columns
        for i in SUMMARY_COLUMNS:
            if i in object_gdf.columns:
                object_gdf.loc[:, i] = object_gdf[i].map(lambda x: str(x)[:-1])
        if "rating" in object_gdf.columns:
            object_gdf.loc[:, "rating"] = np.maximum(1, object_gdf["rating"])
        for i in ["tags_assigned", "tags_invalid"]:
            if i in object_gdf.columns:
                object_gdf.loc[:, i] = object_gdf[i].map(lambda x: ";".join(list(set(str(x).split(LIST_SEPARATOR)))))

        # rename columns
        object_gdf.rename(columns=col_translation, inplace=True)

        # join gdf to layer_summary
        layers_summary.join_gdf(object_gdf, object_layer)

        if gdf.empty:
            logger.warning(
                f"{layer}: geen valide objecten na syntax-validatie. Inspecteer 'syntax_oordeel' in de resultaten; deze is false voor alle objecten. De laag zal genegeerd worden in de (topo)logische validatie."
            )
        else:
            datamodel.set_data(gdf, layer, index_col=None)

    return datamodel, layers_summary, result_summary


def _execute(
    ## read validation fix report
    ## apply fix actions to hydamo gpkg
    ## update validation fix report with history
    self,
):
    """Execute the logical fixes."""

    object_dict: dict[str, gpd.GeoDataFrame] = self.fix_overview
    execution_dict = {}
    for (
        layer_name,
        object_gdf,
    ) in (
        object_dict.items()
    ):  ## for now use this, but in the end we need to figure out an order to do certain layers before the other.
        fix_config_object = next(obj for obj in self.fix_config["objects"] if obj["object"] == layer_name)
        fix_config_fixes = fix_config_object["fixes"]  ## list with all the fixes for every attribute possible
        # fix_type = fix_config_fixes["fix_type"]
        # fix_action = fix_config_fixes["fix_action"]

        fix_cols = [c for c in object_gdf.columns if c.startswith("fixes_")]
        manual_cols = [c for c in object_gdf.columns if c.startswith("manual_overwrite_")]
        results = []
        ## Store the fixes for each feature and each attribute in a feature_dict and save the highest priority fix method
        ## Question: how can a report table look? Multiple attributes? Just one fix per attribute right? But multiple fixes per feature?
        for idx, row in object_gdf.iterrows():
            feature_dict = {
                "id": idx,
                "code": row["code"],
                "fixes": {col.replace("fixes_", ""): row[col] for col in fix_cols},
                "manual_inputs": {col.replace("manual_overwrite_", ""): row[col] for col in manual_cols},
            }
            results.append(feature_dict)

        execution_dict[layer_name] = [{}]
        list_features_to_remove = []  ## codes
        list_features_to_edit = []  ## codes
        highest_prios = []
        for result in results:
            fixes: dict = result["fixes"]
            fix_suggestions = fixes.values()
            fix_ids = []
            for fix in fix_suggestions:
                if fix:
                    fix_ids.append(int(re.search(r"^[A-Za-z]{2}(\d+):", fix).group(1)))
            fix_ids.sort()
            highest_prio_fix = min(fix_ids)
            if result["code"] in [
                "KDU-OH-5108",
                "KDU-Q-8338",
                "KDU-Q-2146",
                "KDU-Q-1355",
                "KDU-OH-4971",
                "KDU-Q-8343",
            ]:
                highest_prio_fix = 2
            highest_prios.append(highest_prio_fix)
            if highest_prio_fix == 1:
                list_features_to_remove.append(result["code"])
                execution_dict[layer_name][0]["fix_id"] = min(highest_prios)
                execution_dict[layer_name][0]["inputs"] = list_features_to_remove
            if highest_prio_fix == 2:
                list_features_to_edit.append(result["code"])
                if not len(execution_dict[layer_name]) >= highest_prio_fix:
                    execution_dict[layer_name].append({})
                execution_dict[layer_name][1]["fix_id"] = 2
                execution_dict[layer_name][1]["inputs"] = list_features_to_edit

    ## Make an execution dict that executes fixes based on an order that prevents conflicts
    for layer_name, execution in execution_dict.items():
        hydamo_layer_gdf = gpd.read_file(self.hydamo_file_path, layer=layer_name)
        if execution[0]["fix_id"] == FIX_MAPPING.omit:
            gdf_hydamo_fixed = hydamo_fixes.omit_features(
                gdf_HyDAMO=hydamo_layer_gdf,
                layer=layer_name,
                list_features=execution[0]["inputs"],
                logger=self.logger,
            )
            # save layer report gdf to report gpkg
            gdf_hydamo_fixed.to_file(self.hydamo_fixed_file_path, layer=layer_name, driver="GPKG")
            self.logger.info(f"Finshed and saved report gdf for layer {layer_name} to {self.report_gpkg_path}")
        if len(execution) > 2 and execution[1]["fix_id"] == FIX_MAPPING.edit:
            pass
            ## do the edit logic
        if execution[0]["fix_id"] == FIX_MAPPING.multi_edit:
            pass

        ## check fixes
        ## for each fix suggested per feature, execute the fix
        ## compile a dict of fixes per feature
        ## use typing to rank the used fix
        ## maybe use levels in fixconfig to denote importance

        # if fix_type == "automatic":
        #     ## use the rules
        #     pass
        # elif fix_type == "manual":
        #     ## use the value that is filled in the manual column
        #     pass

        # if fix_action == "Remove":
        #     pass

        # elif rule["type"] == "logic":
        #     object_gdf.loc[indices, (result_variable)] = (
        #         _process_logic_function(
        #             object_gdf.loc[indices], function, input_variables
        #         )
        #     )
        # elif (rule["type"] == "topologic") and (
        #     hasattr(datamodel, "hydroobject")
        # ):
        #     result_series = _process_topologic_function(
        #         # getattr(
        #         #     datamodel, object_layer
        #         # ),  # FIXME: commented as we need to apply filter in topologic functions as well. Remove after tests pass
        #         object_gdf,
        #         datamodel,
        #         function,
        #         input_variables,
        #     )
        #     object_gdf.loc[indices, (result_variable)] = result_series.loc[
        #         indices
        #     ]
