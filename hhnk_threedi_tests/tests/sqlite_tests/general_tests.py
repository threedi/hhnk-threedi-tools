from ...queries.tests.sqlite_tests.general_checks_queries import ModelCheck
import hhnk_research_tools as hrt
from ...variables.database_variables import id_col


def run_model_checks(test_env):
    """
    Collects all queries that are part of general model checks (see general_checks_queries file)
    and executes them
    """
    try:
        model_path = test_env.src_paths['model']
        queries_lst = [item for item in vars(ModelCheck()).values()]
        query = "UNION ALL\n".join(queries_lst)
        db = hrt.execute_sql_selection(query=query, database_path=model_path, index_col=id_col)
        return db
    except Exception as e:
        raise e from None
