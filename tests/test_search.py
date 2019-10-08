from chord_variant_service.search import parse_conditions

TEST_CONDITIONS = [
    {
        "field": "[dataset item].chromosome",
        "negated": False,
        "operation": "eq",
        "searchValue": "5"
    }
]

TEST_CONDITIONS_INVALID = [
    {
        "field": "[dataset item].chromosome",
        "negated": False,
        "operation": "invalid",
        "searchValue": "5"
    },
    {
        "field": "[dataset item].invalid",
        "negated": False,
        "operation": "eq",
        "searchValue": "5"
    },
    {
        "field": "[dataset item].chromosome",
        "negated": "invalid",
        "operation": "eq",
        "searchValue": "5"
    }
]


def test_parse_conditions():
    cd = parse_conditions(TEST_CONDITIONS)

    assert "chromosome" in cd
    assert "field" in cd["chromosome"]
    assert "negated" in cd["chromosome"] and not cd["chromosome"]["negated"]
    assert "searchValue" in cd["chromosome"] and cd["chromosome"]["searchValue"] == "5"

    cd_i = parse_conditions(TEST_CONDITIONS_INVALID)

    assert "chromosome" not in cd_i
    assert "invalid" not in cd_i
