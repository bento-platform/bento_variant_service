import os

from flask import g
from multiprocessing import Pool


try:  # pragma: no cover
    # noinspection PyUnresolvedReferences
    WORKERS = len(os.sched_getaffinity(0))
except AttributeError:  # pragma: no cover
    # sched_getaffinity isn't available on all systemps
    WORKERS = 4


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
