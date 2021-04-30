class ThreediInformation():
    """
    Initialization:
    ThreediInformation(result, scenario_df)

    Members:
        result  (GridH5ResultAdmin object)
        scenario_df  (dataframe containing indexes of important timesteps
                        in rain scenario)
    return value: object
    """
    def __init__(self, result, df):
        self.result = result
        self.scenario_df = df
