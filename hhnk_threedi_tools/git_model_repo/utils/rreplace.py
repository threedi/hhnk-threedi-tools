def rreplace(string: str, old: str, new: str, occurrences: int) -> str:
    """Replace occurrences of a substring from the end of the string.

    Parameters
    ----------
    string : str
        The string to perform replacements in.
    old : str
        The substring to replace.
    new : str
        The substring to replace with.
    occurrences : int
        The number of occurrences to replace from the end.

    Returns
    -------
    str
        The string with replaced substrings.
    """
    li = string.rsplit(old, occurrences)
    return new.join(li)
