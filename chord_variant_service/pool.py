import os

from flask import g
from multiprocessing import Pool

WORKERS = len(os.sched_getaffinity(0))


def get_pool():
    if "pool" not in g:
        g.pool = Pool(processes=WORKERS)

    return g.pool


def teardown_pool(err):
    if err is not None:  # pragma: no cover
        print(err)
    pool = g.pop("pool", None)
    if pool is not None:
        pool.close()
