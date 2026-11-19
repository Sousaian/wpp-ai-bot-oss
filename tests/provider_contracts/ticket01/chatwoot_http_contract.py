#!/usr/bin/env python3
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request


BASE_URL = os.environ["TICKET01_CHATWOOT_URL"].rstrip("/")
ACCOUNT_ID = int(os.environ["TICKET01_ACCOUNT_ID"])
INBOX_ID = int(os.environ["TICKET01_INBOX_ID"])
CONTACT_ID = int(os.environ["TICKET01_CONTACT_ID"])
CONVERSATION_ID = int(os.environ["TICKET01_CONVERSATION_ID"])
API_TOKEN = os.environ["TICKET01_API_TOKEN"]
MESSAGES_URL = (
    f"{BASE_URL}/api/v1/accounts/{ACCOUNT_ID}/conversations/{CONVERSATION_ID}/messages"
)


def request(method: str, url: str, payload: dict[str, object] | None = None) -> tuple[int, object]:
    body = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={"api_access_token": API_TOKEN, "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def wait_ready() -> None:
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(BASE_URL, timeout=2):
                return
        except Exception:
            time.sleep(1)
    raise RuntimeError("Chatwoot fixture did not become ready")


def scan_messages() -> tuple[list[dict[str, object]], list[int], int]:
    after = 0
    pages: list[int] = []
    messages: list[dict[str, object]] = []
    seen: set[int] = set()
    contact_id = 0
    while True:
        status, body = request("GET", f"{MESSAGES_URL}?after={after}")
        assert status == 200 and isinstance(body, dict)
        rows = body["payload"]
        assert isinstance(rows, list)
        pages.append(len(rows))
        contact_id = body["meta"]["contact"]["id"]
        for row in rows:
            assert row["id"] not in seen
            seen.add(row["id"])
            messages.append(row)
        if len(rows) < 100:
            break
        after = rows[-1]["id"]
    return messages, pages, contact_id


def main() -> None:
    wait_ready()

    conversation_url = (
        f"{BASE_URL}/api/v1/accounts/{ACCOUNT_ID}/conversations/{CONVERSATION_ID}"
    )
    status, conversation = request("GET", conversation_url)
    assert status == 200 and isinstance(conversation, dict)
    assert conversation["account_id"] == ACCOUNT_ID
    assert conversation["inbox_id"] == INBOX_ID
    assert conversation["meta"]["sender"]["id"] == CONTACT_ID

    marker = "wppctx:v1:attempt-0001:chunk-0001"
    content = f"[{marker}]\nsynthetic late context"
    status, created = request(
        "POST",
        MESSAGES_URL,
        {
            "content": content,
            "private": True,
            "message_type": "outgoing",
            "source_id": marker,
        },
    )
    assert status == 200 and isinstance(created, dict)
    for key, expected in {
        "content": content,
        "inbox_id": INBOX_ID,
        "conversation_id": CONVERSATION_ID,
        "message_type": 1,
        "private": True,
        "source_id": marker,
    }.items():
        assert created[key] == expected

    control_marker = "wppctx:v1:filter-control"
    status, control = request(
        "POST",
        MESSAGES_URL,
        {
            "content": "synthetic visible filter control",
            "private": False,
            "message_type": "outgoing",
            "source_id": control_marker,
        },
    )
    assert status == 200 and isinstance(control, dict)
    assert control["id"] > created["id"]
    status, filtered = request(
        "GET",
        (
            f"{MESSAGES_URL}?after={created['id'] - 1}"
            "&filter_internal_messages=true"
        ),
    )
    assert status == 200 and isinstance(filtered, dict)
    filtered_source_ids = {row.get("source_id") for row in filtered["payload"]}
    assert marker not in filtered_source_ids
    assert control_marker in filtered_source_ids

    limit_results: dict[int, int] = {}
    for size in (150_000, 150_001):
        status, body = request(
            "POST",
            MESSAGES_URL,
            {
                "content": "x" * size,
                "private": True,
                "message_type": "outgoing",
                "source_id": f"wppctx:length:{size}",
            },
        )
        limit_results[size] = status
        if size == 150_000:
            assert status == 200 and isinstance(body, dict)
        else:
            assert status == 422 and isinstance(body, dict)
            assert set(body) == {"error"}

    duplicate_marker = "wppctx:duplicate-probe"
    duplicate_ids: list[int] = []
    for _ in range(2):
        status, body = request(
            "POST",
            MESSAGES_URL,
            {
                "content": "synthetic duplicate marker probe",
                "private": True,
                "message_type": "outgoing",
                "source_id": duplicate_marker,
            },
        )
        assert status == 200 and isinstance(body, dict)
        duplicate_ids.append(body["id"])
    assert len(set(duplicate_ids)) == 2

    messages, pages, recovered_contact_id = scan_messages()
    matches = [row for row in messages if row.get("source_id") == marker]
    duplicates = [row for row in messages if row.get("source_id") == duplicate_marker]
    assert len(matches) == 1
    assert len(duplicates) == 2
    assert recovered_contact_id == CONTACT_ID
    recovered = matches[0]
    assert recovered["private"] is True
    assert recovered["inbox_id"] == INBOX_ID
    assert recovered["conversation_id"] == CONVERSATION_ID
    assert recovered["content"] == content

    report = {
        "evidence": "CHATWOOT_LIVE",
        "create_status": 200,
        "private_round_trip": True,
        "account_inbox_contact_attempt_proof": True,
        "content_limit_status": limit_results,
        "pagination_page_sizes": pages,
        "exact_positive_marker_recovery": True,
        "private_hidden_when_filter_internal_messages_is_set": True,
        "filtered_range_reached_private_note_id": True,
        "filtered_range_visible_control_returned": True,
        "duplicate_source_id_count": len(duplicates),
        "source_id_provider_idempotency": False,
        "ambiguous_absence_authorizes_retry": False,
    }
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
