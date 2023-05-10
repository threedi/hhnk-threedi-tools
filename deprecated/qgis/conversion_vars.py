class modelConversionVariables:
    """
    Container for model conversion variables:

        modelConversionVariables(
            from_state ->           str, see definitions file (in this folder) for options
            to_state ->             ""
            one_d_two_d_source ->   str, optional (but necessary if we are converting to 1d2d state). Determines
                                    whether to update by calculating or from backup. See definitions file for options.
        )
    """

    def __init__(self, from_state, to_state, one_d_two_d_source=None):
        self.from_state = from_state
        self.to_state = to_state
        self.one_d_two_d_source = one_d_two_d_source
