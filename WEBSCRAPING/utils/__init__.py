import re
from unidecode import unidecode


def format_filename(filename: str) -> str:
    """Format filename to remove spaces and replace slashes  with hifen"""
    return re.sub(r"[\\/:*?\"<>|,.]", "_", unidecode(filename.strip()).replace(" ", "_"))
