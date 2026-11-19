"""Deterministic Chatwoot private-note chunks for accepted late input.

Every size decision is taken on the complete rendered note exactly as it is
sent (header line + JSON-escaped body), counted in characters like the pinned
Chatwoot 150000-character validation. JSON escaping (control characters as
``\\u00XX``, quotes, backslashes) and the marker/coverage header are therefore
inside the budget.

A record whose rendering cannot fit in one note is split deterministically by
code points into ordered parts with explicit part coverage
(``membership:{id}#part{n:04d}/{total:04d}``); the parts concatenate back to
the exact original content. Nothing is truncated or reordered. For the
verified provider limit the planner is total: only a budget smaller than the
fixed header overhead (a programming error) can make it raise.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re


CHATWOOT_PRIVATE_NOTE_MAX_CHARACTERS = 150_000
MAX_CHUNKS = 9999
MAX_PARTS = 9999

COVERAGE_RE = re.compile(r"membership:([1-9][0-9]*)(?:#part([0-9]{4})/([0-9]{4}))?")


@dataclass(frozen=True)
class LateInputRecord:
    membership_id: int
    input_sequence: int
    content: str

    @property
    def coverage_id(self) -> str:
        return f"membership:{self.membership_id}"


@dataclass(frozen=True)
class _Piece:
    """A whole record or one ordered part of a split record."""

    membership_id: int
    input_sequence: int
    content: str
    part: int | None = None
    parts: int | None = None

    @property
    def coverage_id(self) -> str:
        base = f"membership:{self.membership_id}"
        if self.part is None:
            return base
        return f"{base}#part{self.part:04d}/{self.parts:04d}"


@dataclass(frozen=True)
class ContextChunk:
    ordinal: int
    total: int
    source_id: str
    body_sha256: str
    coverage_ids: tuple[str, ...]
    membership_ids: tuple[int, ...]
    content: str


def parse_coverage_id(value: str) -> tuple[int, int | None, int | None]:
    """Strict inverse of ``_Piece.coverage_id``; raises ``ValueError``."""

    match = COVERAGE_RE.fullmatch(value) if isinstance(value, str) else None
    if match is None:
        raise ValueError("coverage id is malformed")
    membership_id = int(match.group(1))
    if match.group(2) is None:
        return membership_id, None, None
    part, parts = int(match.group(2)), int(match.group(3))
    if not 1 <= part <= parts or parts < 2:
        raise ValueError("coverage part is out of range")
    return membership_id, part, parts


def _piece_json(piece: _Piece) -> str:
    return json.dumps(
        {
            "content": piece.content,
            "coverage_id": piece.coverage_id,
            "input_sequence": piece.input_sequence,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _render(
    attempt_id: int,
    pieces: tuple[_Piece, ...],
    *,
    ordinal: int,
    total: int,
) -> ContextChunk:
    body = "\n".join(_piece_json(piece) for piece in pieces)
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
    source_id = f"wppctx:v1:{attempt_id}:{ordinal:04d}:{total:04d}:{digest}"
    coverage = tuple(piece.coverage_id for piece in pieces)
    header = json.dumps(
        {
            "body_sha256": digest,
            "chunk": ordinal,
            "chunks": total,
            "coverage": coverage,
            "marker": source_id,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    membership_ids: list[int] = []
    for piece in pieces:
        if not membership_ids or membership_ids[-1] != piece.membership_id:
            membership_ids.append(piece.membership_id)
    return ContextChunk(
        ordinal=ordinal,
        total=total,
        source_id=source_id,
        body_sha256=digest,
        coverage_ids=coverage,
        membership_ids=tuple(membership_ids),
        content=f"{header}\n{body}",
    )


def _fits_alone(attempt_id: int, piece: _Piece, maximum_characters: int) -> bool:
    # Worst-case header: the largest ordinal/total this planner can emit.
    rendered = _render(attempt_id, (piece,), ordinal=MAX_CHUNKS, total=MAX_CHUNKS)
    return len(rendered.content) <= maximum_characters


def _split(
    attempt_id: int,
    record: LateInputRecord,
    maximum_characters: int,
) -> tuple[_Piece, ...]:
    """Greedy, deterministic code-point split; each part fits a note alone."""

    whole = _Piece(record.membership_id, record.input_sequence, record.content)
    if _fits_alone(attempt_id, whole, maximum_characters):
        return (whole,)
    content = record.content
    parts: list[str] = []
    start = 0
    while start < len(content):

        def part(length: int) -> _Piece:
            # Fixed-width part numbering: the probe's coverage length is final.
            return _Piece(
                record.membership_id,
                record.input_sequence,
                content[start:start + length],
                MAX_PARTS,
                MAX_PARTS,
            )

        if not _fits_alone(attempt_id, part(1), maximum_characters):
            raise ValueError("note budget is below the fixed header overhead")
        low, high = 1, len(content) - start
        while low < high:
            middle = (low + high + 1) // 2
            if _fits_alone(attempt_id, part(middle), maximum_characters):
                low = middle
            else:
                high = middle - 1
        parts.append(content[start:start + low])
        start += low
    if len(parts) > MAX_PARTS:
        raise ValueError("late-input record needs more parts than the planner supports")
    if "".join(parts) != content:
        raise AssertionError("split parts do not reconstruct the record")
    return tuple(
        _Piece(record.membership_id, record.input_sequence, part, index, len(parts))
        for index, part in enumerate(parts, start=1)
    )


def _pack(
    attempt_id: int,
    pieces: tuple[_Piece, ...],
    *,
    maximum_characters: int,
    total: int,
) -> tuple[tuple[_Piece, ...], ...]:
    batches: list[tuple[_Piece, ...]] = []
    current: tuple[_Piece, ...] = ()
    for piece in pieces:
        candidate = current + (piece,)
        if len(
            _render(attempt_id, candidate, ordinal=len(batches) + 1, total=total).content
        ) <= maximum_characters:
            current = candidate
            continue
        if not current:
            # Unreachable: every piece fits alone under the worst-case header.
            raise AssertionError("a split piece does not fit an empty note")
        batches.append(current)
        current = (piece,)
    if current:
        batches.append(current)
    return tuple(batches)


def plan_context_chunks(
    attempt_id: int,
    records: tuple[LateInputRecord, ...],
    *,
    maximum_characters: int = CHATWOOT_PRIVATE_NOTE_MAX_CHARACTERS,
) -> tuple[ContextChunk, ...]:
    """Pack complete rendered notes; preserve every code point and identity."""

    if attempt_id < 1 or maximum_characters < 1:
        raise ValueError("positive attempt ID and character limit are required")
    if not records:
        return ()
    identities = [record.membership_id for record in records]
    sequences = [record.input_sequence for record in records]
    if any(identity < 1 for identity in identities) or len(set(identities)) != len(
        identities
    ):
        raise ValueError("late-input membership IDs must be positive and unique")
    if sequences != sorted(sequences) or len(set(sequences)) != len(sequences):
        raise ValueError("late-input sequences must be strictly ordered and unique")

    pieces = tuple(
        piece
        for record in records
        for piece in _split(attempt_id, record, maximum_characters)
    )
    total = 1
    for _ in range(len(pieces) + 2):
        batches = _pack(attempt_id, pieces, maximum_characters=maximum_characters, total=total)
        if len(batches) == total:
            break
        if len(batches) < total:
            raise AssertionError("context chunk count must converge monotonically")
        total = len(batches)
    else:
        raise AssertionError("context chunk count did not converge")
    if total > MAX_CHUNKS:
        raise ValueError("late context needs more notes than the planner supports")

    chunks = tuple(
        _render(attempt_id, batch, ordinal=index, total=total)
        for index, batch in enumerate(batches, start=1)
    )
    flattened = [piece for batch in batches for piece in batch]
    rebuilt: dict[int, str] = {}
    for piece in flattened:
        rebuilt[piece.membership_id] = rebuilt.get(piece.membership_id, "") + piece.content
    ordered_ids: list[int] = []
    for piece in flattened:
        if not ordered_ids or ordered_ids[-1] != piece.membership_id:
            ordered_ids.append(piece.membership_id)
    if (
        ordered_ids != identities
        or any(rebuilt[record.membership_id] != record.content for record in records)
        or any(len(chunk.content) > maximum_characters for chunk in chunks)
    ):
        raise AssertionError("context chunks lost coverage or exceeded provider limit")
    return chunks
