"""Release image contract; runtime code is never mounted from the host."""
import os
from pathlib import Path
import re
import unittest


class ReleaseImageTests(unittest.TestCase):
    def test_dependency_and_build_contract(self) -> None:
        root = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
        dockerfile = (root / "app/Dockerfile").read_text()
        self.assertRegex(dockerfile, r"FROM python:3\.12-slim@sha256:[a-f0-9]{64}")
        self.assertIn("--require-hashes -r requirements.lock", dockerfile)
        self.assertIn("--only-binary=:all:", dockerfile)
        self.assertNotIn("apt-get", dockerfile)
        for name in ("AGENTS.md", "docs/durable-delivery-runbook.md", "docs/inbox-scheduler-runbook.md", "scripts/provision_provider_auth.sh"):
            self.assertIn("release_build.sh", (root/name).read_text(), name)
        self.assertNotIn("docker compose build app", (root/"scripts/provision_provider_auth.sh").read_text())
        self.assertIn('CMD ["python", "-m", "src.app_server"]', dockerfile)
        direct = re.findall(r"^([\w-]+)(?:\[[^]]+\])?==([^\s]+)$",
                            (root / "app/requirements.in").read_text(), re.M)
        self.assertEqual(len(direct), 12)
        lock = (root / "app/requirements.lock").read_text()
        self.assertIn("pip-tools==7.5.1", lock)
        for name, version in direct:
            self.assertRegex(lock, rf"(?m)^{name}(?:\[[^]]+\])?=={re.escape(version)} \\")
        alias = (root / "app/requirements.txt").read_text()
        self.assertIn("--require-hashes", alias)
        self.assertIn("-r requirements.lock", alias)
        ignore = (root / "app/.dockerignore").read_text()
        for pattern in ("tests/", "**/__pycache__/", "**/*.pyc", ".env*", "logs/"):
            self.assertIn(pattern, ignore)


def validate_script_builds(path: Path, source: str) -> int:
    """Inspect shell commands and Python argv lists, not line-presence greps."""
    import ast
    import shlex
    commands = []
    if path.suffix == ".sh":
        for match in re.finditer(r"\bdocker[ \t]+build\b[^\n]*(?:\\\n[^\n]*)*", source):
            commands.append(shlex.split(match.group().replace("\\\n", " ")))
    elif path.suffix == ".py":
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, (ast.List, ast.Tuple)) and len(node.elts) > 2:
                first = node.elts[:2]
                if all(isinstance(n, ast.Constant) for n in first) and [n.value for n in first] == ["docker", "build"]:
                    commands.append([n.value if isinstance(n, ast.Constant) else
                        n.left.value + "<expression>" if isinstance(n, ast.BinOp) and isinstance(n.left, ast.Constant)
                        and isinstance(n.left.value, str) else "<expression>" for n in node.elts])
    for argv in commands:
        assert any((token == "--build-arg" and index + 1 < len(argv)
                    and argv[index + 1].startswith("DELIVERY_WORKER_CODE_VERSION="))
                   or token.startswith("--build-arg=DELIVERY_WORKER_CODE_VERSION=")
                   for index, token in enumerate(argv)), f"{path.name}: image version build arg required"
    return len(commands)


class ReleaseBuildScriptTests(unittest.TestCase):
    def test_all_script_app_builds_pass_version_arg(self) -> None:
        root = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
        count = sum(validate_script_builds(path, path.read_text()) for path in (root / "scripts").iterdir()
                    if path.suffix in (".py", ".sh"))
        self.assertEqual(count, 17)  # 15 shell + Nginx proof + certified release builder.
        print("R5_BUILD_ARG_MATRIX PASS commands=17")

    def test_each_shell_build_arg_removal_is_detected(self) -> None:
        root = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
        count = 0
        for path in (root / "scripts").glob("*.sh"):
            source = path.read_text()
            if not re.search(r"\bdocker build\b", source):
                continue
            mutated = source.replace('--build-arg "DELIVERY_WORKER_CODE_VERSION=${build_code_version}" ', "", 1)
            self.assertNotEqual(source, mutated)
            with self.assertRaisesRegex(AssertionError, "image version build arg required"):
                validate_script_builds(path, mutated)
            count += 1
            print("R3_BUILD_ARG_MUTATION PASS script=" + path.name)
        self.assertEqual(count, 15)


def validate_compose_builds(source: str) -> int:
    import yaml
    model = yaml.safe_load(source)
    count = 0
    for service, config in model.get("services", {}).items():
        build = config.get("build")
        if not build:
            continue
        context = build if isinstance(build, str) else build.get("context", "")
        if context not in ("app", "./app") and not str(context).endswith("/app"):
            continue
        assert isinstance(build, dict), f"{service}: image version build arg required"
        args = build.get("args", {})
        if isinstance(args, list):
            args = dict(item.split("=", 1) for item in args if "=" in item)
        assert args.get("DELIVERY_WORKER_CODE_VERSION"), f"{service}: image version build arg required"
        count += 1
    return count


class ReleaseComposeBuildTests(unittest.TestCase):
    def test_all_compose_builds_pass_version(self) -> None:
        root = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
        count = sum(validate_compose_builds(path.read_text()) for path in root.glob("docker-compose*.yml"))
        self.assertEqual(count, 6)  # Release app + all five validation builders.
        print("R4_COMPOSE_BUILD_MATRIX PASS contexts=6 files=3")

    def test_each_compose_arg_removal_is_detected(self) -> None:
        import yaml
        from copy import deepcopy
        root = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
        count = 0
        for path in root.glob("docker-compose*.yml"):
            source = yaml.safe_load(path.read_text())
            for service, config in source.get("services", {}).items():
                if not config.get("build"):
                    continue
                model = deepcopy(source)
                model["services"][service]["build"] = {"context": "./app"}
                with self.assertRaisesRegex(AssertionError, "image version build arg required"):
                    validate_compose_builds(yaml.safe_dump(model))
                count += 1
                print("R4_COMPOSE_BUILD_MUTATION PASS service="+service+" file="+path.name)
        self.assertEqual(count, 6)

    def test_test_build_version_marks_dirty_sources(self) -> None:
        import subprocess
        import tempfile
        root = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
        # Runtime image intentionally has no git/toolchain. Model the git CLI
        # contract here; the host proof additionally executes actual git states.
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            git = repo / "git"
            git.write_text('#!/bin/sh\ncase "$3" in\nrev-parse) printf "abcdef0\\n";;\nstatus) cat "$2/status";;\nesac\n')
            git.chmod(0o700)
            env = {"PATH": directory + os.pathsep + os.defpath, "LANG": "C.UTF-8"}
            for mode, status in (("clean", ""), ("tracked", " M source.py"),
                                 ("staged", "M  source.py"), ("untracked", "?? new.py")):
                (repo / "status").write_text(status)
                result = subprocess.run(["sh", "-c", '. "$1"; image_test_build_version "$2"', "sh",
                    str(root / "scripts/test_image_version.sh"), directory], env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.strip(), "abcdef0" + ("" if mode == "clean" else "-dirty"))
                print("R4_DIRTY_VERSION PASS mode="+mode)
