def rreplace(string, old, new, occurrences):
    """Replace but with occurrences from the end of the string.

    :param string: string to replace in
    :param old: substring to replace
    :param new: substring to replace with
    :param occurrences: the number of occurrences to replace
    :return: string with replaced substring
    """
    li = string.rsplit(old, occurrences)
    return new.join(li)
