from flask import g
from multiprocessing import Pool

from chord_variant_service.app import application
from chord_variant_service.pool import WORKERS, get_pool, teardown_pool


def test_pool_init():
    try:
        # noinspection PyUnresolvedReferences
        from pytest_cov.embed import cleanup_on_sigterm
    except ImportError:
        pass
    else:
        cleanup_on_sigterm()

    dummy_pool = Pool(processes=WORKERS)

    with application.app_context():
        pool = get_pool()
        assert isinstance(pool, type(dummy_pool))

        pool2 = get_pool()
        assert pool2 == pool

        pool.close()
        pool.join()

    dummy_pool.close()
    dummy_pool.join()


def test_pool_tear_down():
    dummy_pool = Pool(processes=WORKERS)

    try:
        with application.app_context():
            pool = get_pool()
            assert isinstance(pool, type(dummy_pool))

            teardown_pool(None)
            pool.join()
            assert "pool" not in g
    finally:
        dummy_pool.close()
        dummy_pool.join()
