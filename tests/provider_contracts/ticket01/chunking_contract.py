#!/usr/bin/env python3
import hashlib
import json
from dataclasses import dataclass


@dataclass(frozen=True)
class Record:
    coverage_id: str
    content: str


def _render_record(record: Record) -> str:
    return json.dumps(
        {"coverage_id": record.coverage_id, "content": record.content},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _render_chunk(
    attempt_id: str,
    batch: tuple[Record, ...],
    *,
    ordinal: int,
    total: int,
) -> dict[str, object]:
    body = "\n".join(_render_record(record) for record in batch)
    body_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
    marker = f"wppctx:v1:{attempt_id}:{ordinal:04d}:{total:04d}:{body_hash}"
    header = {
        "body_sha256": body_hash,
        "chunk": ordinal,
        "chunks": total,
        "coverage": [record.coverage_id for record in batch],
        "marker": marker,
    }
    content = (
        json.dumps(
            header,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
        + body
    )
    return {
        "source_id": marker,
        "coverage": tuple(record.coverage_id for record in batch),
        "content": content,
    }


def _pack_for_total(
    attempt_id: str,
    records: tuple[Record, ...],
    *,
    maximum_characters: int,
    total: int,
) -> tuple[tuple[Record, ...], ...]:
    batches: list[tuple[Record, ...]] = []
    current: tuple[Record, ...] = ()
    for record in records:
        candidate = current + (record,)
        rendered = _render_chunk(
            attempt_id,
            candidate,
            ordinal=len(batches) + 1,
            total=total,
        )
        if len(rendered["content"]) <= maximum_characters:
            current = candidate
            continue
        if not current:
            raise ValueError("one indivisible record exceeds the verified note budget")
        batches.append(current)
        current = (record,)
        rendered = _render_chunk(
            attempt_id,
            current,
            ordinal=len(batches) + 1,
            total=total,
        )
        if len(rendered["content"]) > maximum_characters:
            raise ValueError("one indivisible record exceeds the verified note budget")
    if current:
        batches.append(current)
    return tuple(batches)


def plan_chunks(
    attempt_id: str,
    records: tuple[Record, ...],
    *,
    maximum_characters: int,
) -> tuple[dict[str, object], ...]:
    if not attempt_id or maximum_characters <= 0:
        raise ValueError("attempt and positive character limit are required")
    if not records:
        return ()
    if any(not record.coverage_id for record in records):
        raise ValueError("coverage IDs must not be empty")
    if len({record.coverage_id for record in records}) != len(records):
        raise ValueError("coverage IDs must be unique")

    total = 1
    for _ in range(len(records) + 2):
        batches = _pack_for_total(
            attempt_id,
            records,
            maximum_characters=maximum_characters,
            total=total,
        )
        actual_total = len(batches)
        if actual_total == total:
            break
        if actual_total < total:
            raise AssertionError("chunk total must converge monotonically")
        total = actual_total
    else:
        raise AssertionError("chunk total did not converge")

    chunks = tuple(
        _render_chunk(
            attempt_id,
            batch,
            ordinal=ordinal,
            total=total,
        )
        for ordinal, batch in enumerate(batches, start=1)
    )
    if any(len(chunk["content"]) > maximum_characters for chunk in chunks):
        raise AssertionError("final complete content exceeded provider limit")
    return chunks


def parse_chunk(
    chunk: dict[str, object],
    *,
    expected_attempt_id: str,
    expected_ordinal: int,
    expected_total: int,
) -> tuple[dict[str, object], tuple[Record, ...]]:
    content = chunk["content"]
    assert isinstance(content, str)
    header_line, body = content.split("\n", 1)
    header = json.loads(header_line)
    parsed = tuple(
        Record(**json.loads(line))
        for line in body.splitlines()
        if line
    )
    body_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
    marker_prefix, marker_ordinal, marker_total, marker_hash = header[
        "marker"
    ].rsplit(":", 3)
    expected_prefix = f"wppctx:v1:{expected_attempt_id}"
    expected_marker = (
        f"{expected_prefix}:{expected_ordinal:04d}:"
        f"{expected_total:04d}:{body_hash}"
    )
    assert marker_prefix == expected_prefix
    assert header["chunk"] == expected_ordinal
    assert header["chunks"] == expected_total
    assert marker_ordinal == f"{expected_ordinal:04d}"
    assert marker_total == f"{expected_total:04d}"
    assert marker_hash == body_hash
    assert header["body_sha256"] == body_hash
    assert header["marker"] == expected_marker
    assert chunk["source_id"] == expected_marker
    assert tuple(header["coverage"]) == tuple(record.coverage_id for record in parsed)
    assert tuple(chunk["coverage"]) == tuple(header["coverage"])
    return header, parsed


def assert_round_trip(
    attempt_id: str,
    records: tuple[Record, ...],
    *,
    maximum_characters: int,
) -> tuple[dict[str, object], ...]:
    first = plan_chunks(
        attempt_id,
        records,
        maximum_characters=maximum_characters,
    )
    second = plan_chunks(
        attempt_id,
        records,
        maximum_characters=maximum_characters,
    )
    assert first == second
    parsed_records: list[Record] = []
    for ordinal, chunk in enumerate(first, start=1):
        header, parsed = parse_chunk(
            chunk,
            expected_attempt_id=attempt_id,
            expected_ordinal=ordinal,
            expected_total=len(first),
        )
        parsed_records.extend(parsed)
    assert tuple(parsed_records) == records
    coverage = [record.coverage_id for record in parsed_records]
    assert coverage == [record.coverage_id for record in records]
    assert len(coverage) == len(set(coverage))
    assert len({chunk["source_id"] for chunk in first}) == len(first)
    assert all(len(chunk["content"]) <= maximum_characters for chunk in first)
    return first


def main() -> None:
    many_short = tuple(Record(f"m{index:03d}", "") for index in range(35))
    many_chunks = assert_round_trip(
        "attempt-many-short",
        many_short,
        maximum_characters=1_000,
    )
    assert len(many_chunks) > 1

    long_ids_unicode = tuple(
        Record(
            f"message-{index:04d}-" + ("long-id-" * 14),
            content,
        )
        for index, content in enumerate(
            (
                "ação e informação",
                "astral 😀🧠🚀",
                "combining e\u0301 a\u0303 o\u0302",
                "mistura ç漢字🙂" * 20,
                "curta",
            ),
            start=1,
        )
    )
    unicode_chunks = assert_round_trip(
        "attempt-unicode",
        long_ids_unicode,
        maximum_characters=900,
    )

    boundary_record = (Record("boundary-id", "á😀e\u0301" * 30),)
    provisional = plan_chunks(
        "attempt-boundary",
        boundary_record,
        maximum_characters=10_000,
    )
    exact_length = len(provisional[0]["content"])
    exact = assert_round_trip(
        "attempt-boundary",
        boundary_record,
        maximum_characters=exact_length,
    )
    assert len(exact[0]["content"]) == exact_length

    tampered = dict(exact[0])
    tampered_header_line, tampered_body = tampered["content"].split("\n", 1)
    tampered_header = json.loads(tampered_header_line)
    wrong_attempt_marker = tampered_header["marker"].replace(
        "wppctx:v1:attempt-boundary:",
        "wppctx:v1:wrong-attempt:",
        1,
    )
    tampered_header["marker"] = wrong_attempt_marker
    tampered["source_id"] = wrong_attempt_marker
    tampered["content"] = (
        json.dumps(
            tampered_header,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
        + tampered_body
    )
    try:
        parse_chunk(
            tampered,
            expected_attempt_id="attempt-boundary",
            expected_ordinal=1,
            expected_total=1,
        )
    except AssertionError:
        wrong_attempt_rejected = True
    else:
        wrong_attempt_rejected = False
    assert wrong_attempt_rejected, "wrong-attempt marker must be rejected"
    try:
        plan_chunks(
            "attempt-boundary",
            boundary_record,
            maximum_characters=exact_length - 1,
        )
        raise AssertionError("one-character-under boundary must block")
    except ValueError:
        pass

    try:
        plan_chunks(
            "attempt-oversize",
            (Record("message-too-large", "x" * 1_000),),
            maximum_characters=1_000,
        )
        raise AssertionError("oversized record must not be truncated")
    except ValueError:
        pass

    print(
        json.dumps(
            {
                "evidence": "SYNTHETIC_CHUNKING",
                "many_short_records": len(many_short),
                "many_short_chunks": len(many_chunks),
                "long_ids": True,
                "unicode_cases": ["accent", "astral", "combining"],
                "unicode_chunks": len(unicode_chunks),
                "actual_complete_length_packing": True,
                "parse_back_exact_attempt_marker_source_id_hash_ordinal_total_coverage": True,
                "wrong_attempt_tamper_rejected": wrong_attempt_rejected,
                "coverage_ids_exactly_once": True,
                "order_preserved": True,
                "exact_boundary": exact_length,
                "one_character_under_blocked": True,
                "oversized_single_record": "blocked",
                "silent_truncation": False,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
