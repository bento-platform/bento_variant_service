import os

__all__ = ["DATA_PATH"]

DATA_PATH = os.environ.get("DATA", "data/")
