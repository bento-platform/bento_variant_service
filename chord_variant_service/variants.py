# Possible operations: eq, lt, gt, le, ge, co
# TODO: Regex verification with schema, to front end

VARIANT_SCHEMA = {
    "$id": "TODO",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "description": "CHORD variant data type",
    "type": "object",
    "required": ["chromosome", "start", "end", "ref", "alt"],
    "search": {
        "operations": [],
    },
    "properties": {
        "chromosome": {
            "type": "string",
            # TODO: Choices
            "search": {
                "operations": ["eq"],
                "canNegate": False,
                "required": True,
                "type": "single",  # single / unlimited
                "order": 0
            }
        },
        "start": {
            "type": "integer",
            "search": {
                "operations": ["eq"],
                "canNegate": False,
                "required": True,
                "type": "single",  # single / unlimited
                "order": 1
            }
        },
        "end": {
            "type": "integer",
            "search": {
                "operations": ["eq"],
                "canNegate": False,
                "required": True,
                "type": "single",  # single / unlimited
                "order": 2
            }
        },
        "ref": {
            "type": "string",
            "search": {
                "operations": ["eq"],
                "canNegate": True,
                "required": False,
                "type": "single",  # single / unlimited
                "order": 3
            }
        },
        "alt": {
            "type": "string",
            "search": {
                "operations": ["eq"],
                "canNegate": True,
                "required": False,
                "type": "single",  # single / unlimited
                "order": 4
            }
        }
    }
}
