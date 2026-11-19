"""Trusted local release builder receipt, not independent host attestation."""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import tempfile

from release_process import run

CONTEXT_CONTRACT = "git-blobs-v1"


class IdentityError(ValueError):
    pass


def git(root: Path, *arguments: str) -> bytes:
    environment = {"PATH": os.environ.get("PATH", os.defpath), "LC_ALL": "C",
                   "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull,
                   "GIT_OPTIONAL_LOCKS": "0"}
    return run(["git", "-c", "core.fsmonitor=false", "-c", "core.hooksPath=/dev/null",
                "-C", str(root), *arguments], env=environment,
               capture_output=True, check=True, timeout=30).stdout


def clean_head(root: Path, expected: str | None = None) -> str:
    try:
        top = Path(git(root, "rev-parse", "--show-toplevel").decode().strip()).resolve()
        head = git(root, "rev-parse", "HEAD").decode().strip()
        if top != root.resolve() or not re.fullmatch(r"[a-f0-9]{40}", head):
            raise IdentityError("release_repository_invalid")
        if expected is not None and head != expected:
            raise IdentityError("release_head_mismatch")
        # Effective config includes config.worktree when extensions.worktreeConfig
        # is enabled. Ignore external Git env/global config, as for object reads.
        sparse = git(root, "config", "--type=bool", "--get", "--default=false", "core.sparseCheckout").strip()
        if sparse != b"false":
            raise IdentityError("release_sparse_checkout_forbidden")
        flags = git(root, "ls-files", "-v", "-z").split(b"\0")
        if any(record and (record[:1].islower() or record[:1] == b"S") for record in flags):
            raise IdentityError("release_index_flags_forbidden")
        if git(root, "status", "--porcelain=v1", "--untracked-files=all"):
            raise IdentityError("release_checkout_dirty")
        return head
    except (OSError, UnicodeError, subprocess.SubprocessError):
        raise IdentityError("release_git_unavailable") from None


def context_files(root: Path, commit: str) -> list[tuple[str, int, bytes]]:
    """Raw object reads: no archive attributes, filters, smudge or worktree data."""
    if not re.fullmatch(r"[a-f0-9]{40}", commit):
        raise IdentityError("release_commit_invalid")
    records = git(root, "ls-tree", "-rz", commit, "--", "app").split(b"\0")
    entries = []
    for record in filter(None, records):
        try:
            metadata, raw_path = record.split(b"\t", 1)
            mode, kind, oid = metadata.split(b" ")
            path = raw_path.decode("utf-8")
        except (ValueError, UnicodeError):
            raise IdentityError("release_context_entry_invalid") from None
        relative = PurePosixPath(path)
        if (not path.startswith("app/") or relative.is_absolute() or ".." in relative.parts
                or "\\" in path or "\n" in path or "\r" in path):
            raise IdentityError("release_context_path_invalid")
        if mode not in (b"100644", b"100755") or kind != b"blob":
            raise IdentityError("release_context_entry_unsupported")
        content = git(root, "cat-file", "blob", oid.decode("ascii"))
        if content.startswith(b"version https://git-lfs.github.com/spec/v1"):
            raise IdentityError("release_context_lfs_unsupported")
        entries.append((path[4:], 0o755 if mode == b"100755" else 0o644, content))
    names = {path for path, _, _ in entries}
    if not {"Dockerfile", ".dockerignore", "requirements.lock"} <= names or not any(p.startswith("src/") for p in names):
        raise IdentityError("release_context_incomplete")
    return sorted(entries)


def context_hash(files: list[tuple[str, int, bytes]]) -> str:
    manifest = [[name, mode, hashlib.sha256(content).hexdigest()] for name, mode, content in files]
    canonical = json.dumps([CONTEXT_CONTRACT, manifest], ensure_ascii=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


def export_context(files: list[tuple[str, int, bytes]], destination: Path) -> None:
    # Caller owns a new private empty directory. No tar extraction or links.
    if list(destination.iterdir()):
        raise IdentityError("release_context_destination_not_empty")
    for name, mode, content in files:
        path = PurePosixPath(name)
        if path.is_absolute() or ".." in path.parts or not path.parts or "\\" in name:
            raise IdentityError("release_context_path_invalid")
        target = destination.joinpath(*path.parts)
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with target.open("xb") as stream:
            stream.write(content)
        target.chmod(mode)


def valid_image_reference(project: str, version: str) -> bool:
    # Familiar names normalize to docker.io/library/<project>-app. Docker's
    # supported parser limits normalized remote path (library/ included), not the domain or tag.
    return bool(len("library/" + project + "-app") <= 255
                and re.fullmatch(r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*", project + "-app")
                and re.fullmatch(r"[a-z0-9][a-z0-9_-]*", project)
                and re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", version))


def receipt_path(root: Path, project: str) -> Path:
    if not valid_image_reference(project, "probe"):
        raise IdentityError("invalid_docker_image_reference")
    path = git(root, "rev-parse", "--git-path", "wpp-release/" + project + ".json").decode().strip()
    return (root / path).resolve()


@contextmanager
def build_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.with_suffix(".lock").open("a+b") as stream:
        os.fchmod(stream.fileno(), 0o600)
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise IdentityError("release_build_locked") from None
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def inspect_image(reference: str) -> tuple[str, dict]:
    try:
        model = json.loads(run(["docker", "image", "inspect", reference], capture_output=True,
                               text=True, check=True, timeout=15).stdout)[0]
        image_id = model["Id"]
        labels = model["Config"].get("Labels") or {}
        if not re.fullmatch(r"sha256:[a-f0-9]{64}", image_id):
            raise ValueError()
        return image_id, labels
    except (OSError, ValueError, KeyError, IndexError, TypeError, subprocess.SubprocessError):
        raise IdentityError("release_image_unavailable") from None


def expected_receipt(root: Path, project: str, commit: str, image_id: str) -> dict:
    return {"contract": CONTEXT_CONTRACT, "project": project, "commit": commit,
            "version": git(root, "rev-parse", "--short", commit).decode().strip(),
            "context_sha256": context_hash(context_files(root, commit)), "image_id": image_id}


def publish_receipt(path: Path, receipt: dict) -> None:
    descriptor, temporary = tempfile.mkstemp(prefix=".receipt-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(receipt, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def verify_receipt(root: Path, project: str, version: str) -> None:
    commit = clean_head(root)
    path = receipt_path(root, project)
    try:
        receipt = json.loads(path.read_text())
    except (OSError, ValueError):
        raise IdentityError("release_receipt_missing_or_invalid") from None
    image_id, labels = inspect_image(project + "-app:" + version)
    expected = expected_receipt(root, project, commit, image_id)
    if receipt != expected or version != expected["version"]:
        raise IdentityError("release_receipt_mismatch")
    if labels.get("org.opencontainers.image.revision") != version:
        raise IdentityError("release_image_revision_mismatch")
    if inspect_image(project + "-app:" + version)[0] != image_id:
        raise IdentityError("release_image_changed")
    clean_head(root, commit)
