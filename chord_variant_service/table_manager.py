import sys

from flask import current_app, g

from chord_variant_service.constants import DATA_PATH, SERVICE_NAME
from chord_variant_service.tables.base import TableManager
from chord_variant_service.tables.memory import MemoryTableManager
from chord_variant_service.tables.vcf.drs_manager import DRSVCFTableManager
from chord_variant_service.tables.vcf.vcf_manager import VCFTableManager

from typing import Optional


__all__ = [
    "MANAGER_TYPE_DRS",
    "MANAGER_TYPE_MEMORY",
    "MANAGER_TYPE_VCF",

    "create_table_manager_of_type",
    "get_table_manager",
    "clear_table_manager",
]


# TODO: How to share this across processes?
# TODO: Per process? We probably shouldn't just use a global here.
_table_manager = None


MANAGER_TYPE_DRS = "drs"
MANAGER_TYPE_MEMORY = "memory"
MANAGER_TYPE_VCF = "vcf"


def create_table_manager_of_type(manager_type: str) -> Optional[TableManager]:
    if manager_type == MANAGER_TYPE_DRS:
        return DRSVCFTableManager(DATA_PATH)
    elif manager_type == MANAGER_TYPE_MEMORY:
        return MemoryTableManager()
    elif manager_type == MANAGER_TYPE_VCF:
        return VCFTableManager(DATA_PATH)


def get_table_manager() -> TableManager:
    global _table_manager

    if "table_manager" not in g:
        if _table_manager is None:
            manager_type = current_app.config["TABLE_MANAGER"]

            _table_manager = create_table_manager_of_type(manager_type)
            if _table_manager is None:  # pragma: no cover
                print(f"[{SERVICE_NAME}] Invalid table manager type: {manager_type}", file=sys.stderr, flush=True)
                exit(1)

            _table_manager.update_tables()

        g.table_manager = _table_manager

    return g.table_manager


def clear_table_manager(_e):
    g.pop("table_manager", None)
