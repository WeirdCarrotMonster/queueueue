import pytest
from queueueue.queueueue import Manager, safe_int_conversion


def test_manager_create():
    Manager()


def test_manager_create_auth():
    Manager(auth=("username", "password"))


def test_manager_create_invalid_auth():
    with pytest.raises(AssertionError):
        Manager(auth=("username",))
    with pytest.raises(AssertionError):
        Manager(auth=["username"])


def test_save_int_conversion():
    assert safe_int_conversion(None, 10) == 10
    assert safe_int_conversion("10", 0) == 10
    assert safe_int_conversion(10, 10) == 10
    assert safe_int_conversion(10, 10, max_val=5) == 5
    assert safe_int_conversion(10, 10, min_val=15) == 15
