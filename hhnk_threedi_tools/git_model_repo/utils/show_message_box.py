import ctypes  # Included in Python installation.
import os


def show_message_mbox(title: str, text: str):
    """Display a message box with the given title and text.

    This function uses platform-specific methods to display a message box:
    - On Windows, it uses `ctypes` to call the Windows API.
    - On macOS, it uses `osascript` to display a dialog in Finder.
    - On Linux, it simply prints the message to the console.

    Parameters
    ----------
    title (str): The title of the message box.
    text (str): The text to display in the message box.

    Returns
    -------
    None
    """
    if os.name == "nt":
        ctypes.windll.user32.MessageBoxExW(0, text, title, 0x40000)
        # return ctypes.windll.user32.MessageBoxW(0, text, title, style)
    elif os.name == "posix":
        # if mac
        os.system("""osascript -e 'tell app "Finder" to display dialog "{}" with title "{}"'""".format(text, title))
    else:
        # if linux
        print(title + ": " + text)
