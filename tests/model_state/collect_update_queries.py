from ...queries.model_states.update_queries import create_global_settings_rows_update_query, \
    construct_global_settings_control_group_query, create_bank_levels_update_query, create_new_manholes_query, \
    construct_manholes_update_query, construct_weir_height_update_statement, construct_channels_update_statement

def get_all_update_queries(global_settings_df=None,
                           global_settings_to_add=[],
                           global_settings_to_delete=[],
                           global_settings_excluded=[],
                           bank_levels_df=None,
                           bank_levels_excluded=[],
                           new_manholes_df=None,
                           new_manholes_excluded=[],
                           update_manholes_df=None,
                           update_manholes_excluded=[],
                           weir_width_df=None,
                           weir_width_excluded=[],
                           channels_df=None,
                           channels_excluded=[]):
    try:
        query_list = []
        if global_settings_df is not None and not global_settings_df.empty:
            query = create_global_settings_rows_update_query(excluded_ids=global_settings_excluded,
                                                             ids_to_add=global_settings_to_add,
                                                             ids_to_delete=global_settings_to_delete)
            if query is not None:
                query_list.append(query)
            rows_not_to_delete = [item for item in global_settings_to_delete if item in global_settings_excluded]
            # We have to filter the excluded ids and take out the ones that are excluded from being
            # removed (as they will be in the model). List now contains all id's that are not being added
            # and not the ones not being deleted
            update_skip_ids = [item for item in global_settings_excluded if item not in rows_not_to_delete]
            query = construct_global_settings_control_group_query(global_settings_to_update_df=global_settings_df,
                                                                  excluded_ids=update_skip_ids)
            if query is not None:
                query_list.append(query)
        if bank_levels_df is not None and not bank_levels_df.empty:
            query = create_bank_levels_update_query(new_bank_levels_df=bank_levels_df,
                                                    excluded_ids=bank_levels_excluded)
            if query is not None:
                query_list.append(query)
        if new_manholes_df is not None and not new_manholes_df.empty:
            query = create_new_manholes_query(new_manholes_df=new_manholes_df,
                                              excluded_ids=new_manholes_excluded)
            if query is not None:
                query_list.append(query)
        if update_manholes_df is not None and not update_manholes_df.empty:
            query =construct_manholes_update_query(manholes_to_update_df=update_manholes_df,
                                                   excluded_ids=update_manholes_excluded)
            if query is not None:
                query_list.append(query)
        if weir_width_df is not None and not weir_width_df.empty:
            query = construct_weir_height_update_statement(weir_widths_to_update_df=weir_width_df,
                                                           excluded_ids=weir_width_excluded)
            if query is not None:
                query_list.append(query)
        if channels_df is not None and not channels_df.empty:
            query = construct_channels_update_statement(channels_to_update_df=channels_df,
                                                        excluded_ids=channels_excluded)
            if query is not None:
                query_list.append(query)
        if query_list:
            full_query = ';\n'.join(query_list)
        else:
            full_query = ""
        return full_query
    except Exception as e:
        raise e from None