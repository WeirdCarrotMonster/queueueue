import pytest as pytest

from queueueue.app import build_app, setup_basic_auth
from queueueue.utils import safe_int_conversion


def test_save_int_conversion():
    assert safe_int_conversion(None, 10) == 10
    assert safe_int_conversion("10", 0) == 10
    assert safe_int_conversion(10, 10) == 10
    assert safe_int_conversion(10, 10, max_val=5) == 5
    assert safe_int_conversion(10, 10, min_val=15) == 15


def test_auth_invalid_credentials():
    app = build_app()

    with pytest.raises(ValueError):
        setup_basic_auth(app, ["invalid,credentials"])
