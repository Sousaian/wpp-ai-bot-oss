#!/usr/bin/env python3
"""FU-3 real-MySQL byte-exact maintenance, tamper and fix-removal proofs.

Called by run_inbox_fu3_mysql_matrix.sh. All DBs belong to that private server;
MYSQL_PWD is forwarded by name, never put in process arguments or logs.
"""
from __future__ import annotations

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
OLD = "12213a6acf680656113841970e1ec05c2a3ab972725a469af6936615af3c0c9a"
NEW = "0308f61fbc7eacfab2b60cc48797863b0be253ca9fc7afae7aea361fe4097e13"
CHECK = "ck_scheduled_handoff_summary_source"
ERROR = "ticket05 handoff check shape requires audited repair"
CHARSETS = ("latin1", "utf8mb4", "utf8mb3")
VALIDATED_CHECKS = {
    "scheduled_handoffs": [CHECK],
    "handoff_context_assignments": ["ck_handoff_context_coverage_ordinal"],
    "outbox_deliveries": ["ck_outbox_delivery_authority_v2"],
    "replica_heartbeats": [
        "ck_replica_heartbeat_kind", "ck_replica_heartbeat_delivery_mode",
        "ck_replica_heartbeat_auth_mode", "ck_replica_heartbeat_schema_version",
    ],
}

PREDECESSORS = {
    "a961a1e": "ticket07_populate.sql",
    "1a7e1ed": "ticket06_populate.sql",
    "f59c32c": "ticket05_populate.sql",
    "d52a495": "ticket05_populate.sql",
    "35f9c07": "ticket05_populate.sql",
    "148dd57": "ticket07_populate.sql",
    "52c6aac": "ticket07_populate.sql",
    "61a9f73": "ticket07_populate.sql",
    "f57ad6e": "ticket07_populate.sql",
    "a3fe23f": "ticket07_populate.sql",
}


class Matrix:
    def __init__(self, container: str, log_dir: Path) -> None:
        self.container = container
        self.logs = log_dir
        self.logs.mkdir(parents=True, exist_ok=True)
        self.current = (ROOT / "db/init.sql").read_text()
        self.counts: Counter[str] = Counter()
        self.seq = 0

    def sql(self, db: str | None, sql: str, charset: str, label: str,
            error: str | None = None) -> str:
        self.seq += 1
        cmd = ["docker", "exec", "-i", "--env", "MYSQL_PWD", self.container,
               "mysql", "-uroot", "--protocol=socket", "-N", "-B",
               f"--default-character-set={charset}"]
        if db:
            cmd.append(db)
        result = subprocess.run(cmd, input=sql, text=True, capture_output=True, timeout=300)
        log = self.logs / f"{self.seq:04d}-{label}.log"
        log.write_text(result.stdout + result.stderr)
        if error is not None:
            assert result.returncode != 0 and error in result.stderr, (log, result.stderr)
        else:
            assert result.returncode == 0, (log, result.stderr)
        return result.stdout

    def db(self, name: str, charset: str, source: str | None = None,
           fixture: str | None = None) -> None:
        assert re.fullmatch(r"fu3_[a-z0-9_]+", name)
        self.sql(None, f"CREATE DATABASE {name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;",
                 charset, "create")
        self.sql(name, source or self.current, charset, "install")
        if fixture:
            self.sql(name, (ROOT / "scripts/fixtures" / fixture).read_text(), charset, "populate")
            # Existing fixtures omit modelo_llm to exercise legacy backfill.
            # Maintenance proof uses already-migrated rows, not a pending backfill.
            self.sql(name, "UPDATE agentes SET modelo_llm='openai/gpt-4o-mini' "
                     "WHERE modelo_llm IS NULL;", charset, "fixture-model")

    def pass_(self, kind: str, label: str) -> None:
        self.counts[kind] += 1
        print(f"FU3_{kind.upper()} PASS {label}", flush=True)

    def checks(self, db: str, charset: str) -> dict[str, tuple[str, str, str]]:
        output = self.sql(db, """
SELECT tc.constraint_name, tc.table_name, tc.enforced, SHA2(cc.check_clause,256)
FROM information_schema.table_constraints tc
JOIN information_schema.check_constraints cc
 ON cc.constraint_schema=tc.constraint_schema AND cc.constraint_name=tc.constraint_name
WHERE tc.constraint_schema=DATABASE() ORDER BY tc.constraint_name;
""", charset, "checks")
        return {parts[0]: tuple(parts[1:]) for line in output.splitlines()
                if (parts := line.split("\t"))}

    def authority(self, db: str, charset: str) -> str:
        # No GROUP_CONCAT truncation; metadata values as individual ordered rows.
        return self.sql(db, """
SELECT 'column', table_name, ordinal_position, column_name, column_type, is_nullable,
 column_default, character_set_name, collation_name, extra
FROM information_schema.columns WHERE table_schema=DATABASE()
ORDER BY table_name,ordinal_position;
SELECT 'index', table_name, index_name, non_unique, seq_in_index, column_name,
 collation, sub_part, index_type, is_visible, expression
FROM information_schema.statistics WHERE table_schema=DATABASE()
ORDER BY table_name,index_name,seq_in_index;
SELECT 'fk', k.table_name,k.constraint_name,k.ordinal_position,k.column_name,
 k.referenced_table_name,k.referenced_column_name,r.update_rule,r.delete_rule,
 (k.referenced_table_schema=DATABASE())
FROM information_schema.key_column_usage k
JOIN information_schema.referential_constraints r
 ON r.constraint_schema=k.constraint_schema AND r.constraint_name=k.constraint_name
 AND r.table_name=k.table_name
WHERE k.constraint_schema=DATABASE() ORDER BY k.table_name,k.constraint_name,k.ordinal_position;
SELECT 'trigger',trigger_name,event_object_table,action_timing,event_manipulation,
 action_orientation,SHA2(action_statement,256),sql_mode,character_set_client,collation_connection
FROM information_schema.triggers WHERE trigger_schema=DATABASE() ORDER BY trigger_name;
""", charset, "authority")

    def rows(self, db: str) -> str:
        cmd = ["docker", "exec", "--env", "MYSQL_PWD", self.container, "mysqldump",
               "-uroot", "--protocol=socket", "--no-create-info", "--skip-extended-insert",
               "--skip-comments", "--compact", "--skip-add-locks", "--skip-disable-keys",
               "--skip-lock-tables", "--skip-triggers", "--set-gtid-purged=OFF",
               "--order-by-primary", db]
        result = subprocess.run(cmd, text=True, capture_output=True, timeout=60)
        assert result.returncode == 0, result.stderr
        return result.stdout

    def audit(self, charset: str) -> None:
        db = f"fu3_audit_{charset}"
        self.db(db, charset, fixture="ticket07_populate.sql")
        before_checks = self.checks(db, charset)
        assert before_checks[CHECK][2] == OLD
        authority = self.authority(db, charset)
        rows = self.rows(db)
        (self.logs / f"audit-rows-{charset}.sql").write_text(rows)
        tables = self.sql(db, """SELECT table_name FROM information_schema.tables
WHERE table_schema=DATABASE() AND table_type='BASE TABLE' ORDER BY table_name;""",
                          charset, "tables").splitlines()
        # All tables, not only the subset with digest-validated objects.
        assert len(tables) == 22, tables
        assert len(before_checks) == 57, len(before_checks)
        assert authority.count("trigger\t") == 12, authority
        (self.logs / f"audit-before-{charset}.json").write_text(
            json.dumps(before_checks, indent=2) + "\n")
        deltas: list[dict[str, str]] = []
        previous_checks = before_checks
        validated_names = {name for names in VALIDATED_CHECKS.values() for name in names}
        assert len(validated_names) == 7
        for operation in ("OPTIMIZE", "COMMENT"):
            for table in tables:
                assert re.fullmatch("[a-z_]+", table)
                sql = (f"OPTIMIZE TABLE {table};" if operation == "OPTIMIZE" else
                       f"ALTER TABLE {table} COMMENT='fu3 audited maintenance';")
                result = self.sql(db, sql, charset, f"{operation}-{table}")
                if operation == "OPTIMIZE":
                    assert result.rstrip().endswith("status\tOK"), result
                after = self.checks(db, charset)
                assert set(after) == set(before_checks)
                for name, value in after.items():
                    if name == CHECK:
                        assert value[:2] == before_checks[name][:2] and value[2] in (OLD, NEW)
                    elif name in validated_names:
                        assert value == before_checks[name], (charset, operation, table, name, value)
                    else:
                        # These clauses are NOT compared by digest in init.sql.
                        # Preserve name/table/enforcement; record serializer
                        # deltas without pretending their old bytes are stable.
                        assert value[:2] == before_checks[name][:2]
                    if value != previous_checks[name]:
                        delta = {"charset": charset, "operation": operation, "table": table,
                                 "check": name, "before": previous_checks[name][2], "after": value[2]}
                        deltas.append(delta)
                        print("FU3_SERIALIZER_DELTA " + json.dumps(delta, sort_keys=True), flush=True)
                previous_checks = after
                assert self.authority(db, charset) == authority, (charset, operation, table)
                assert self.rows(db) == rows, (charset, operation, table, "data changed")
                self.sql(db, self.current, charset, "migrate-after-maintenance")
                assert self.checks(db, charset) == after
                assert self.authority(db, charset) == authority
                assert self.rows(db) == rows
                self.pass_("maintenance", f"{charset} {operation} {table} migrate=0 "
                           f"checks={len(after)} triggers=12 rows=unchanged")
        assert self.checks(db, charset)[CHECK][2] == NEW
        (self.logs / f"serializer-deltas-{charset}.json").write_text(
            json.dumps(deltas, indent=2) + "\n")
        # Additional common rebuild entrypoints on the original affected table.
        self.sql(db, "ALTER TABLE scheduled_handoffs FORCE;", charset, "force")
        self.sql(db, self.current, charset, "migrate-after-force")
        assert self.authority(db, charset) == authority and self.rows(db) == rows
        self.pass_("rebuild", f"{charset} FORCE migrate=0")
        (self.logs / f"audit-after-{charset}.json").write_text(
            json.dumps(self.checks(db, charset), indent=2) + "\n")

    def negative(self, charset: str) -> None:
        base = subprocess.check_output(["git", "show", "a3fe23f:db/init.sql"],
                                       cwd=ROOT, text=True)
        db = f"fu3_red_{charset}"
        self.db(db, charset, base, "ticket07_populate.sql")
        self.sql(db, "OPTIMIZE TABLE scheduled_handoffs;", charset, "base-optimize")
        assert self.checks(db, charset)[CHECK][2] == NEW
        self.sql(db, base, charset, "base-red", ERROR)
        self.pass_("base_red", f"{charset} a3fe23f OPTIMIZE + init.sql ERROR1644")
        self.sql(db, self.current, charset, "fix-green")
        self.sql(db, self.current, charset, "fix-repeat")
        self.pass_("fix_green", f"{charset} same DB migration x2 rc=0")
        # Fix removal against the same real DB, not a grep assertion.
        assert self.current.count(NEW) == 1
        mutant = self.current.replace(NEW, OLD)
        self.sql(db, mutant, charset, "fix-removal-mutant", ERROR)
        self.sql(db, self.current, charset, "fix-removal-control")
        self.pass_("mutation", f"{charset} remove ascii digest -> ERROR1644; control rc=0")

    def upgrades(self, charset: str) -> None:
        for ref, fixture in PREDECESSORS.items():
            db = f"fu3_up_{ref}_{charset}"
            base = subprocess.check_output(["git", "show", f"{ref}:db/init.sql"],
                                           cwd=ROOT, text=True)
            # Predecessors themselves require latin1; current must handle all clients.
            self.db(db, "latin1", base, fixture)
            # Pre-ticket05 predecessors have no scheduled_handoffs yet.
            has_handoff = CHECK in self.checks(db, charset)
            if has_handoff:
                result = self.sql(db, "OPTIMIZE TABLE scheduled_handoffs;",
                                  charset, "upgrade-optimize")
                assert result.rstrip().endswith("status\tOK"), result
            before = self.rows(db)
            # Some released predecessors add/classify immutable columns; compare
            # their rows after first upgrade, plus invariant fixture row counts.
            counts_sql = """SELECT COUNT(*) FROM agentes; SELECT COUNT(*) FROM sessoes;
SELECT COUNT(*) FROM conversation_turns; SELECT COUNT(*) FROM handoff_attempts;"""
            counts = self.sql(db, counts_sql, charset, "before-counts")
            self.sql(db, self.current, charset, "upgrade")
            upgraded_rows = self.rows(db)
            assert self.sql(db, counts_sql, charset, "after-counts") == counts
            if ref in ("a3fe23f", "f57ad6e", "61a9f73"):
                # Marker changes 2->3 for 61a9f73; other rows checked by counts
                # and existing T083/SEC matrices' exact row_state checks.
                if ref == "a3fe23f":
                    assert before == upgraded_rows
            if not has_handoff:
                assert self.checks(db, charset)[CHECK][2] == OLD
                result = self.sql(db, "OPTIMIZE TABLE scheduled_handoffs;",
                                  charset, "new-table-optimize")
                assert result.rstrip().endswith("status\tOK"), result
            self.sql(db, self.current, charset, "upgrade-repeat")
            assert self.rows(db) == upgraded_rows
            assert self.checks(db, charset)[CHECK][2] == NEW
            self.pass_("upgrade", f"{charset} {ref} populated upgrade x2 "
                       f"OPTIMIZE={'before-upgrade' if has_handoff else 'after-create'}")

        # No new DDL in FU-3. Re-enter around existing table-create/validate
        # boundaries; old CHECK remains enforced, including an interrupted CALL.
        for index, marker in enumerate((
            "-- ticket05-schema-boundary: scheduled-handoffs-created",
            "-- ticket05-schema-boundary: exact-schema-validated",
        )):
            db = f"fu3_int_{charset}_{index}"
            base = subprocess.check_output(["git", "show", "a3fe23f:db/init.sql"],
                                           cwd=ROOT, text=True)
            self.db(db, "latin1", base, "ticket07_populate.sql")
            self.sql(db, "ALTER TABLE scheduled_handoffs COMMENT='fu3 interrupted';",
                     charset, "interruption-alter")
            before = self.rows(db)
            partial = self.current[:self.current.index(marker)] + "\nDELIMITER ;\n"
            self.sql(db, partial, charset, "interrupted")
            assert self.checks(db, charset)[CHECK][1:] == ("YES", NEW)
            self.sql(db, self.current, charset, "resume")
            self.sql(db, self.current, charset, "resume-repeat")
            assert self.rows(db) == before
            self.pass_("interruption", f"{charset} boundary={index} CHECK retained resume x2")

    def tampers(self, charset: str) -> None:
        checks = VALIDATED_CHECKS
        ordinal = 0
        for table, names in checks.items():
            for name in names:
                for tamper in ("changed", "missing", "unenforced", "extra"):
                    db = f"fu3_tamper_{charset}_{ordinal}"
                    ordinal += 1
                    self.db(db, charset)
                    # Snapshot the real altered metadata; migration must not repair it.
                    if tamper == "extra":
                        ddl = f"ALTER TABLE {table} ADD CONSTRAINT fu3_extra CHECK (1=1);"
                    elif tamper == "unenforced":
                        ddl = f"ALTER TABLE {table} ALTER CHECK {name} NOT ENFORCED;"
                    else:
                        ddl = f"ALTER TABLE {table} DROP CHECK {name};"
                        if tamper == "changed":
                            ddl += f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK (1=1);"
                    self.sql(db, ddl, charset, "tamper-ddl")
                    before = self.checks(db, charset)
                    expected = (ERROR if table in ("scheduled_handoffs", "handoff_context_assignments")
                                else "ticket06 automated authority check shape requires audited repair"
                                if table == "outbox_deliveries" else
                                "t083 replica heartbeat check shape requires audited repair")
                    # Ticket06 validates the exact named successor; unrelated
                    # additional CHECKs are outside that validator's contract.
                    if table == "outbox_deliveries" and tamper == "extra":
                        self.sql(db, self.current, charset, "outbox-extra-control")
                        self.pass_("neighbor_control", f"{charset} outbox unrelated extra CHECK unchanged")
                    else:
                        self.sql(db, self.current, charset, "tamper-rejected", expected)
                        self.pass_("tamper", f"{charset} {name} {tamper} rejected")
                    assert self.checks(db, charset) == before, (db, "tamper rewritten")


        # Same expression shape with altered literal bytes (including whitespace/
        # case), or an unrecognized introducer, must not join the allowlist.
        for index, literal in enumerate((
            "_ascii'technical_fallback '",
            "_ascii'TECHNICAL_FALLBACK'",
            "_utf8mb4'technical_fallback'",
        )):
            db = f"fu3_literal_{charset}_{index}"
            self.db(db, charset)
            ddl = f"""ALTER TABLE scheduled_handoffs DROP CHECK {CHECK},
ADD CONSTRAINT {CHECK} CHECK (
 (summary_kind = _ascii'generated' AND summary_generation_attempt_id IS NOT NULL)
 OR summary_kind IN (_ascii'soft_limit_fallback', {literal}));"""
            self.sql(db, ddl, charset, "literal-tamper")
            before = self.checks(db, charset)
            self.sql(db, self.current, charset, "literal-rejected", ERROR)
            assert self.checks(db, charset) == before
            self.pass_("literal_tamper", f"{charset} literal={index} rejected")
        db = f"fu3_trigger_{charset}"
        self.db(db, charset)
        trigger = "trg_scheduled_handoff_immutable_v1"
        body = bytes.fromhex(self.sql(
            db, f"SELECT HEX(action_statement) FROM information_schema.triggers "
            f"WHERE trigger_schema=DATABASE() AND trigger_name='{trigger}';",
            charset, "trigger-body").strip()).decode()
        # A behaviorally different body, not an absent trigger that IF NOT EXISTS
        # would recreate. Rest of the trigger metadata remains canonical.
        mutant = body.replace(
            "scheduled handoff authority is immutable",
            "scheduled handoff authority is MUTATED",
        )
        assert mutant != body
        self.sql(db, f"DROP TRIGGER {trigger};\nDELIMITER $$\n"
                 f"CREATE TRIGGER {trigger} BEFORE UPDATE ON scheduled_handoffs "
                 f"FOR EACH ROW {mutant}$$\nDELIMITER ;\n",
                 charset, "trigger-tamper")
        before_authority = self.authority(db, charset)
        self.sql(db, self.current, charset, "trigger-rejected",
                 "immutability guard definition requires audited repair")
        assert self.authority(db, charset) == before_authority
        self.pass_("trigger_tamper", f"{charset} changed body rejected")

    def run(self) -> None:
        for charset in CHARSETS:
            self.negative(charset)
            self.audit(charset)
            self.upgrades(charset)
            self.tampers(charset)
        expected = {
            "base_red": 3, "fix_green": 3, "mutation": 3,
            "maintenance": 132, "rebuild": 3, "upgrade": 30, "interruption": 6,
            "tamper": 81, "neighbor_control": 3, "literal_tamper": 9, "trigger_tamper": 3,
        }
        assert dict(self.counts) == expected, (dict(self.counts), expected)
        (self.logs / "counts.json").write_text(json.dumps(dict(self.counts), indent=2) + "\n")
        print("FU3_SCHEMA PASS " + json.dumps(dict(self.counts), sort_keys=True), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--container", required=True)
    parser.add_argument("--log-dir", type=Path, required=True)
    args = parser.parse_args()
    assert os.environ.get("MYSQL_PWD"), "disposable server credentials required"
    Matrix(args.container, args.log_dir).run()
