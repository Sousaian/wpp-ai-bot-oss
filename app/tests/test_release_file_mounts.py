"""Compose-resolved file mounts, not a second YAML parser or payload reader."""
from copy import deepcopy
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))


class ReleaseFileMountTests(unittest.TestCase):
    def test_resolved_configs_secrets_matrix(self):
        spec = importlib.util.spec_from_file_location("file_mount_guard", ROOT/"scripts/release_preflight.py")
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); (root/"app/src").mkdir(parents=True)
            (root/"alias").symlink_to(root/"app", target_is_directory=True)
            count = 0
            for kind in ("configs", "secrets"):
                base = {"name":"p53-unit", kind:{"value":{"file":"/unrelated/payload"}},
                        "services":{n:{} for n in module.PYTHON_SERVICES}}
                def model(reference=None, definition=None, *, source="value"):
                    value=deepcopy(base)
                    value[kind][source] = {"file":"/unrelated/payload"} if definition is None else definition
                    value["services"][service][kind] = [{"source":source} if reference is None else reference]
                    return value
                for service in module.PYTHON_SERVICES:
                    cases=[]
                    for target in ("/app", "/app/src/config.py", "/app/logs", "//app/src/x.py",
                                   "/other/../app/src/x.py", "/app//src/x.py", "../../app/src/x.py"):
                        cases.append((model({"source":"value","target":target}),False))
                    for target in ("/etc/p53/value", "/application/value", "/app-other/value", "", "value"):
                        cases.append((model({"source":"value","target":target}),True))
                    cases += [(model(),True), (model(source="app"),kind=="secrets"),
                              (model({"source":"value","target":"app/src/x.py"}),kind=="secrets")]
                    for reference in (None, "value", 7, [], {}, {"source":None}, {"source":"missing"},
                                      {"source":"value","target":False}, {"source":"value","target":None},
                                      {"source":"value","target":"\0"}, {"source":"../app"},
                                      {"source":"value","uid":12}, {"source":"value","gid":"no"}, {"source":"value","uid":"١"},
                                      {"source":"value","mode":True}, {"source":"value","mode":"0899"},
                                      {"source":"value","mode":0o10000}, {"source":"value","unknown":True}):
                        m=deepcopy(base);m["services"][service][kind]=[reference];cases.append((m,False))
                    for definition in ({}, False, [], "invalid", {"file":None}, {"file":False},
                                       {"file":""}, {"file":"\0"}, {"environment":False},
                                       {"file":"/outside","environment":"P53_VALUE"},
                                       {"external":"true"}, {"external":True,"file":"/outside"},
                                       {"file":"/outside","labels":[]}, {"file":"/outside","driver_opts":[]},
                                       {"file":"/outside","driver_opts":{"x":False}},
                                       {"file":"/outside","name":False}, {"file":"/outside","unknown":1},
                                       {"file":"./app/src/config.py"}, {"file":str(root/"alias/src/config.py")}):
                        cases.append((model(definition=definition),False))
                    m=model();m[kind]["value"]=None;cases.append((m,False))
                    for definitions in (None, [], False):
                        m=model();m[kind]=definitions;cases.append((m,False))
                    for references in (None, {}, "value", 1):
                        m=model();m["services"][service][kind]=references;cases.append((m,False))
                    for definition in ({"environment":"P53_VALUE"}, {"external":True,"name":"p53-external"},
                                       {"file":"/outside","external":False,"name":"p53-name","labels":{"k":"v"}},
                                       {"file":"/outside","x-extension":False}):
                        cases.append((model(definition=definition),True))
                    cases.append((model({"source":"value","uid":"1","gid":"2","mode":0o444}),True))
                    cases.append((model({"source":"value","uid":"103","gid":"104","mode":"0444"}),True))
                    cases.append((model(definition={"content":""}),kind=="configs"))
                    # No payload is opened, even for file/inline/environment sources.
                    with patch.object(Path,"read_bytes",side_effect=AssertionError("payload_read")), \
                         patch.object(Path,"open",side_effect=AssertionError("payload_open")):
                        for value,expected in cases:
                            self.assertEqual(module.python_mounts_safe(value,root),expected)
                            count+=1;print("P53_FILE_MOUNT_CASE PASS",kind,service)
                    self.assertEqual(len(cases), 67)
            self.assertEqual(count,804)

    def test_readme_certified_release_sequence(self):
        source=(ROOT/"README.md").read_text()
        release=source.split("## Release certificado",1)[1].split("## Endpoints",1)[0]
        commands=['scripts/release_build.sh --project', 'scripts/release_preflight.sh .env',
                  'docker compose up -d --no-build']
        for command in commands:
            self.assertIn(command,release)
        self.assertEqual(sorted(release.index(c) for c in commands),[release.index(c) for c in commands])
        for line in source.splitlines():
            if line.startswith("docker compose") and " up " in line:
                self.assertIn("--no-build",line)
        self.assertIn("-f docker-compose.dev.yml build app",source)
        for path in ("AGENTS.md", "docs/durable-delivery-runbook.md", "docs/inbox-scheduler-runbook.md"):
            self.assertIn(path,source)
        print("P53_README_CONTRACT PASS")
