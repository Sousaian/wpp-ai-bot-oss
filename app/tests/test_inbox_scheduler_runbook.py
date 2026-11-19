import os
from pathlib import Path
import re
import shlex
import unittest

from src import delivery_cli


class InboxSchedulerRunbookTests(unittest.TestCase):
    def test_every_documented_delivery_cli_command_matches_real_argparse(self) -> None:
        repo = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
        doc = (repo / "docs/inbox-scheduler-runbook.md").read_text()
        commands: list[list[str]] = []
        for block in re.findall(r"```bash\n(.*?)```", doc, re.DOTALL):
            flat = block.replace("\\\n", " ")
            for line in flat.splitlines():
                if "python -m src.delivery_cli" not in line:
                    continue
                argv = shlex.split(line.split("python -m src.delivery_cli", 1)[1])
                replacements = {
                    "$SCHEDULER_EPOCH": "11111111-1111-4111-8111-111111111111",
                    "$NEXT_SCHEDULER_EPOCH": "22222222-2222-4222-8222-222222222222",
                    "$DELIVERY_EPOCH": "33333333-3333-4333-8333-333333333333",
                    "$ATTEMPT_KEY": "44444444-4444-4444-8444-444444444444",
                    "$VERSION": "1", "$AGENT_ID": "1", "$CONSUMO_ID": "1",
                    "$ACTOR": "operator", "$REASON": "approved",
                }
                commands.append([replacements.get(item, item) for item in argv])
        self.assertEqual(len(commands), 17)
        parser = delivery_cli._parser()
        for argv in commands:
            with self.subTest(argv=argv):
                parser.parse_args(argv)
        observed = {(a[0], a[1] if len(a) > 1 else None) for a in commands}
        self.assertTrue({
            ("scheduler-state", "show"), ("scheduler-state", "enabling"),
            ("scheduler-state", "enabled"), ("scheduler-state", "draining"),
            ("scheduler-state", "disabled"), ("agent-scheduler", "show"),
            ("agent-scheduler", "list"),
            ("agent-scheduler", "opt-in"), ("agent-scheduler", "opt-out"),
            ("quarantine-list", "--agente-id"),
            ("quarantine-inspect", "44444444-4444-4444-8444-444444444444"),
            ("quarantine-resolve", "44444444-4444-4444-8444-444444444444"),
            ("health", None),
        } <= observed)

    def test_runbook_keeps_audited_maintenance_and_execution_boundary(self) -> None:
        repo = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
        doc = (repo / "docs/inbox-scheduler-runbook.md").read_text()
        # FU-3: known re-serialization is allowed, real shape drift is not.
        self.assertIn("allowlist fechada de duas formas byte-exatas", doc)
        self.assertIn("não são mais proibidos pelo risco de reserialização", doc)
        self.assertIn("Mudança real de shape continua proibida", doc)
        self.assertIn("exigem rehearsal próprio", doc)
        # H: AGENTS.md and the exact SQL allowlist describe the same contract.
        agents = (repo / "AGENTS.md").read_text()
        schema = (repo / "db/init.sql").read_text()
        for digest, occurrences in (
            ("12213a6acf680656113841970e1ec05c2a3ab972725a469af6936615af3c0c9a", 1),
            ("0308f61fbc7eacfab2b60cc48797863b0be253ca9fc7afae7aea361fe4097e13", 1),
        ):
            self.assertEqual(agents.count(digest), 1)
            self.assertEqual(schema.count(digest), occurrences)
        self.assertIn("actual_shape NOT IN (", schema)
        self.assertIn("--attest-old-replicas-removed", doc)
        self.assertIn("sem aprovação operacional separada", doc)
        self.assertIn("Com scheduler `disabled`, incompatibilidade de réplica", doc)
        self.assertIn("Em `enabling`, `enabled` ou `draining`", doc)
        self.assertIn("gate de enablement\nrecusa frota incompatível", doc)
        self.assertIn("`scheduler_runtime_state.state` lido do DB", doc)
        self.assertIn("python -m src.turn_worker health", doc)
        for command in (
            "`ALTER TABLE`", "`OPTIMIZE TABLE`", "`mysqlcheck -o`",
            "`ALTER TABLE … FORCE`", "`pt-online-schema-change`", "`gh-ost`",
        ):
            self.assertIn(command, doc)
        self.assertIn("ERROR 1644", doc)
        self.assertIn("db-migrate", doc)

    def test_drain_lists_and_opts_out_agents_before_disabled(self) -> None:
        repo = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
        doc = (repo / "docs/inbox-scheduler-runbook.md").read_text()
        drain = doc.split("## 3. Drain e desativação", 1)[1].split("## 4.", 1)[0]
        self.assertIn("agent-scheduler list", drain)
        self.assertIn("agent-scheduler opt-out", drain)
        self.assertLess(
            drain.index("agent-scheduler list"), drain.index("scheduler-state disabled")
        )
        self.assertLess(
            drain.index("agent-scheduler opt-out"), drain.index("scheduler-state disabled")
        )
        self.assertIn("HTTP 503", drain)
