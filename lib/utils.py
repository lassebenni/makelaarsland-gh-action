import glob
import os

import pandas as pd


def pandas_read(path: str, extension: str) -> pd.DataFrame:
    """
    Reads all files in a given path with a given extension.

    Parameters
    ----------
    path : str
        Path to the folder where the files are located.
    extension : str
        Extension of the files to be read.

    Returns
    -------
    pd.DataFrame
        DataFrame containg the files that were read.
    """
    files = glob.glob(os.path.join(path, '*.' + extension))
    return pd.concat(map(pd.read_csv, files))
