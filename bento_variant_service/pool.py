import os

from flask import g


try:  # pragma: no cover
    WORKERS = int(os.environ.get("WORKERS", ""))
except ValueError:  # pragma: no cover
    try:
        # noinspection PyUnresolvedReferences
        WORKERS = len(os.sched_getaffinity(0))
    except AttributeError:  # pragma: no cover
        # sched_getaffinity isn't available on all systemps
        WORKERS = int(os.environ.get("WORKERS", "1"))


if WORKERS == 1:
    from multiprocessing.dummy import Pool
else:
    from multiprocessing import Pool


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
