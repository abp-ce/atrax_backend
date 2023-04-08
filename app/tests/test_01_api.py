import pytest
from fastapi import status

from . import conftest as cnft


def test_good_phone():
    response = cnft.client.get("/api/" + cnft.GOOD_NUM)
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {
        "inn": 7707049388,
        "operator": 'ПАО "Ростелеком"',
        "region": "Российская Федерация",
        "sub_region": "",
    }


def test_empty_num():
    response = cnft.client.get("/api/" + cnft.EMPTY_NUM)
    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json() == {"detail": "Not Found"}


def test_num_not_in_base():
    response = cnft.client.get("/api/" + cnft.NUM_NOT_IN_BASE)
    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json() == {"detail": "К сожалению, номер не найден."}


@pytest.mark.parametrize("phone_num", cnft.BAD_NUM_NOT_REGEX)
def test_bad_num_not_regex(phone_num):
    response = cnft.client.get("/api/" + phone_num)
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert "string does not match regex" in response.json()["detail"][0]["msg"]
