#!/usr/bin/env python3
"""Static (no MySQL) check that each versioned guard digest matches its body.

`db/init.sql` keeps the canonical SHA-256 of every versioned immutability
trigger body in `validate_wpp_ai_bot_immutability_guards`. The installed
validator compares those constants with `SHA2(ACTION_STATEMENT, 256)`. This
check ties each constant to the `BEGIN ... END` text of its own
`CREATE TRIGGER` statement, taken byte-for-byte as the mysql client sends it
under `DELIMITER $$` (statement text up to the delimiter; the server stores the
routine body from its first token to the end of the statement).

No normalization is applied. Bodies containing comments or CR bytes are
rejected because the mysql client may transform them before sending, so the
offline digest would not model the stored bytes.
"""
from __future__ import annotations

import hashlib
import re
import subprocess
import sys
import tempfile
from pathlib import Path

DELIMITER = b"$$"
EXPECTED_GUARDS = (
    "trg_inbound_event_disposition_immutable_v2",
    "trg_outbox_authority_immutable_v3",
    "trg_turn_revision_seal_immutable_v2",
    "trg_turn_input_identity_immutable_v2",
    "trg_conversation_turn_identity_immutable_v2",
    "trg_generation_attempt_identity_immutable_v2",
    "trg_turn_revision_draft_immutable_v2",
    "trg_scheduled_handoff_immutable_v1",
    "trg_handoff_context_assignment_immutable_v1",
    "trg_turn_input_handoff_state_v1",
    "trg_outbox_context_payload_immutable_v1",
    "trg_conversation_turn_lifecycle_v1",
)
CREATE_RE = re.compile(rb"CREATE TRIGGER IF NOT EXISTS (trg_[a-z_]+_v[0-9]+)\b")
FOR_EACH_ROW_RE = re.compile(rb"\bFOR EACH ROW\b[ \t\n]*")
VALIDATOR_START = b"CREATE PROCEDURE validate_wpp_ai_bot_immutability_guards()"
CONSTANT_RE = re.compile(
    rb"'(trg_[a-z_]+_v[0-9]+)'(?: AS trigger_name)?,\s*'([0-9a-f]{64})'"
)


def extract_bodies(sql: bytes) -> tuple[dict[str, bytes], list[str]]:
    bodies: dict[str, bytes] = {}
    errors: list[str] = []
    for match in CREATE_RE.finditer(sql):
        name = match.group(1).decode()
        end = sql.find(DELIMITER, match.end())
        if end < 0:
            errors.append(f"{name}: CREATE TRIGGER has no {DELIMITER.decode()} delimiter")
            continue
        statement = sql[match.start():end]
        row = FOR_EACH_ROW_RE.search(statement)
        if row is None:
            errors.append(f"{name}: CREATE TRIGGER has no FOR EACH ROW clause")
            continue
        body = statement[row.end():]
        if not (body.startswith(b"BEGIN") and body.endswith(b"END")):
            errors.append(f"{name}: body is not an exact BEGIN...END block")
            continue
        if b"\r" in body:
            errors.append(f"{name}: body contains CR bytes; client transformation not modeled")
            continue
        if b"--" in body or b"/*" in body or b"#" in body:
            errors.append(f"{name}: body contains a comment; client transformation not modeled")
            continue
        if name in bodies:
            errors.append(f"{name}: duplicate CREATE TRIGGER statement")
            continue
        bodies[name] = body
    return bodies, errors


def extract_constants(sql: bytes) -> tuple[dict[str, str], list[str]]:
    errors: list[str] = []
    start = sql.find(VALIDATOR_START)
    if start < 0:
        return {}, ["validate_wpp_ai_bot_immutability_guards procedure not found"]
    end = sql.find(DELIMITER, start)
    constants: dict[str, str] = {}
    for match in CONSTANT_RE.finditer(sql[start:end]):
        name = match.group(1).decode()
        if name in constants:
            errors.append(f"{name}: duplicate digest constant")
            continue
        constants[name] = match.group(2).decode()
    return constants, errors


def check(path: Path) -> int:
    sql = path.read_bytes()
    bodies, errors = extract_bodies(sql)
    constants, constant_errors = extract_constants(sql)
    errors.extend(constant_errors)
    for name in EXPECTED_GUARDS:
        if name not in bodies and not any(e.startswith(f"{name}:") for e in errors):
            errors.append(f"{name}: CREATE TRIGGER statement not found")
        if name not in constants:
            errors.append(f"{name}: digest constant not found")
    for name in sorted((set(bodies) | set(constants)) - set(EXPECTED_GUARDS)):
        errors.append(f"{name}: unexpected versioned guard")
    for name in EXPECTED_GUARDS:
        if name not in bodies or name not in constants:
            continue
        actual = hashlib.sha256(bodies[name]).hexdigest()
        if actual != constants[name]:
            errors.append(
                f"{name}: body digest mismatch constant={constants[name]} body={actual}"
            )
        else:
            print(f"TRIGGER_DIGEST → PASS ({name}): {actual}")
    if errors:
        for error in errors:
            print(f"TRIGGER_DIGEST → FAIL ({error})", file=sys.stderr)
        return 1
    print(f"TRIGGER_DIGEST → PASS: {len(EXPECTED_GUARDS)}/{len(EXPECTED_GUARDS)} bodies match constants")
    return 0


def run_copy(sql: bytes, workdir: Path, label: str) -> tuple[int, str]:
    copy = workdir / f"{label}.sql"
    copy.write_bytes(sql)
    result = subprocess.run(
        [sys.executable, __file__, str(copy)],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode, result.stdout + result.stderr


def self_test(path: Path) -> int:
    sql = path.read_bytes()
    bodies, errors = extract_bodies(sql)
    constants, constant_errors = extract_constants(sql)
    if errors or constant_errors or set(bodies) != set(EXPECTED_GUARDS):
        print("TRIGGER_DIGEST_SELFTEST → FAIL: canonical file did not parse", file=sys.stderr)
        return 1
    failures = 0
    with tempfile.TemporaryDirectory(prefix="trigger-digest-selftest-") as tmp:
        workdir = Path(tmp)
        rc, output = run_copy(sql, workdir, "baseline")
        if rc != 0:
            print("TRIGGER_DIGEST_SELFTEST → FAIL: unmodified copy rejected", file=sys.stderr)
            print(output, file=sys.stderr)
            return 1
        print("TRIGGER_DIGEST_SELFTEST → PASS (baseline): unmodified copy accepted")
        for name in EXPECTED_GUARDS:
            body = bodies[name]
            body_start = sql.index(body, sql.index(name.encode()))
            # Flip one byte in the middle of the body, keeping length constant.
            offset = body_start + len(body) // 2
            while not chr(sql[offset]).isalpha():
                offset += 1
            replacement = b"X" if sql[offset:offset + 1] != b"X" else b"Y"
            mutated = sql[:offset] + replacement + sql[offset + 1:]
            stale = sql.replace(
                constants[name].encode(),
                constants[name][:-1].encode()
                + (b"0" if constants[name][-1] != "0" else b"1"),
                1,
            )
            for kind, candidate in (("body-byte", mutated), ("stale-constant", stale)):
                rc, output = run_copy(candidate, workdir, f"{name}-{kind}")
                expected = f"TRIGGER_DIGEST → FAIL ({name}: body digest mismatch"
                if rc == 0 or expected not in output:
                    failures += 1
                    print(
                        f"TRIGGER_DIGEST_SELFTEST → FAIL ({name}/{kind}): mismatch not detected",
                        file=sys.stderr,
                    )
                    print(output, file=sys.stderr)
                else:
                    print(f"TRIGGER_DIGEST_SELFTEST → PASS ({name}/{kind}): named mismatch")
    if failures:
        return 1
    print(f"TRIGGER_DIGEST_SELFTEST → PASS: {2 * len(EXPECTED_GUARDS)} negative cases rejected")
    return 0


def main(argv: list[str]) -> int:
    args = argv[1:]
    run_self_test = "--self-test" in args
    paths = [arg for arg in args if arg != "--self-test"]
    path = Path(paths[0]) if paths else Path(__file__).resolve().parent.parent / "db" / "init.sql"
    if run_self_test:
        return self_test(path)
    return check(path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
