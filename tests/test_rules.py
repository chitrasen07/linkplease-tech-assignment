"""POST /rules contract."""

from __future__ import annotations


def test_create_rule_returns_201_with_contract_fields(client):
    response = client.post(
        "/rules", json={"keyword": "PRICE", "dm_message": "Here's the price list: ..."}
    )

    assert response.status_code == 201
    body = response.json()
    assert set(body) == {"rule_id", "keyword", "dm_message"}
    assert body["keyword"] == "PRICE"
    assert body["dm_message"] == "Here's the price list: ..."
    assert isinstance(body["rule_id"], str) and body["rule_id"]


def test_keyword_and_message_are_trimmed(client):
    response = client.post(
        "/rules", json={"keyword": "  PRICE \n", "dm_message": "  hello  "}
    )

    assert response.status_code == 201
    assert response.json()["keyword"] == "PRICE"
    assert response.json()["dm_message"] == "hello"


def test_empty_keyword_is_rejected(client):
    response = client.post("/rules", json={"keyword": "", "dm_message": "hi"})
    assert response.status_code == 400


def test_whitespace_only_keyword_is_rejected(client):
    response = client.post("/rules", json={"keyword": "   ", "dm_message": "hi"})
    assert response.status_code == 400


def test_empty_message_is_rejected(client):
    response = client.post("/rules", json={"keyword": "PRICE", "dm_message": "  "})
    assert response.status_code == 400


def test_missing_fields_are_rejected(client):
    assert client.post("/rules", json={"keyword": "PRICE"}).status_code == 422
    assert client.post("/rules", json={}).status_code == 422


def test_oversized_message_is_rejected(client):
    response = client.post("/rules", json={"keyword": "PRICE", "dm_message": "x" * 5000})
    assert response.status_code == 422


def test_rules_can_be_listed(client):
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "a"})
    client.post("/rules", json={"keyword": "LINK", "dm_message": "b"})

    body = client.get("/rules").json()
    assert {r["keyword"] for r in body} == {"PRICE", "LINK"}
