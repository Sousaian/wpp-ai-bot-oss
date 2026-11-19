"""Build release ONLY from committed Git blobs; publish a local Image ID receipt."""
from __future__ import annotations

import argparse
import io
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent))
from release_identity import (IdentityError, build_lock, clean_head, context_files,
    expected_receipt, export_context, git, inspect_image, publish_receipt,
    receipt_path, valid_image_reference)
from release_process import Cancelled, cancellation, private_directory, run, shield_cleanup

ROOT = Path(__file__).resolve().parent.parent


def check_baked(image_id: str, version: str, project: str) -> None:
    name = project + "-release-probe-" + uuid.uuid4().hex[:12]
    created = False
    try:
        run(["docker", "create", "--name", name, "--network", "none", "--entrypoint", "/bin/true", image_id],
            capture_output=True, check=True, timeout=30)
        created = True
        data = run(["docker", "cp", name + ":/etc/wpp-app/IMAGE_CODE_VERSION", "-"],
                   capture_output=True, check=True, timeout=30).stdout
        with tarfile.open(fileobj=io.BytesIO(data)) as archive:
            members = archive.getmembers()
            if len(members) != 1 or not members[0].isfile():
                raise IdentityError("release_baked_version_invalid")
            value = archive.extractfile(members[0]).read().decode("ascii").strip()
        if value != version:
            raise IdentityError("release_baked_version_mismatch")
    finally:
        # A failed/cancelled `create` can still have created this exact name.
        with shield_cleanup():
            result = run(["docker", "rm", "-f", name], capture_output=True, timeout=30)
            if result.returncode and created:
                raise IdentityError("release_probe_cleanup_failed")
            if result.returncode == 0:
                print("RELEASE_BUILD_RESOURCE_REMOVED " + name, flush=True)


def build(root: Path, project: str, expected_commit: str, *, no_cache=False, pull=False) -> None:
    commit = clean_head(root, expected_commit)
    version = git(root, "rev-parse", "--short", commit).decode().strip()
    if not valid_image_reference(project, version):
        raise IdentityError("invalid_docker_image_reference")
    path = receipt_path(root, project)
    with build_lock(path):
        # Invalidate any previous receipt before a new attempt can replace its tag.
        path.unlink(missing_ok=True)
        try:
            files = context_files(root, commit)
            with private_directory("wpp-release-build-") as temporary:
                context = temporary / "context"
                context.mkdir(mode=0o700)
                export_context(files, context)
                iidfile = temporary / "image.id"
                reference = project + "-app:" + version
                command = ["docker", "build", "--iidfile", str(iidfile), "--build-arg",
                           "DELIVERY_WORKER_CODE_VERSION=" + version, "--tag", reference]
                if no_cache: command.append("--no-cache")
                if pull: command.append("--pull")
                # Build logs are not printed: malformed images/Dockerfiles must not
                # leak environment or arbitrary source data into gate diagnostics.
                run(command + [str(context)], capture_output=True, check=True, timeout=1800)
                image_id = iidfile.read_text().strip()
                if not re.fullmatch(r"sha256:[a-f0-9]{64}", image_id):
                    raise IdentityError("release_build_id_invalid")
                actual_id, labels = inspect_image(reference)
                if actual_id != image_id or labels.get("org.opencontainers.image.revision") != version:
                    raise IdentityError("release_build_identity_mismatch")
                check_baked(image_id, version, project)
                clean_head(root, commit)
                receipt = expected_receipt(root, project, commit, image_id)
                if inspect_image(reference)[0] != image_id:
                    raise IdentityError("release_image_changed")
            # Publish only AFTER the owned context has been cleaned successfully.
            clean_head(root, commit)
            if inspect_image(reference)[0] != image_id:
                raise IdentityError("release_image_changed")
            publish_receipt(path, receipt)
            if inspect_image(reference)[0] != image_id:
                raise IdentityError("release_image_changed")
        except BaseException:
            with shield_cleanup():
                path.unlink(missing_ok=True)
            raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--repo", type=Path, default=ROOT)
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--pull", action="store_true")
    args = parser.parse_args()
    try:
        build(args.repo.resolve(), args.project, args.expected_commit, no_cache=args.no_cache, pull=args.pull)
    except (IdentityError, OSError, ValueError, subprocess.SubprocessError):
        # Only fixed named reasons, never exception text from git/docker/files.
        error = sys.exc_info()[1]
        reason = str(error) if isinstance(error, IdentityError) else "release_build_failed"
        print("RELEASE_BUILD_FAIL " + reason)
        return 1
    print("RELEASE_BUILD_PASS receipt_published")
    return 0


if __name__ == "__main__":
    try:
        with cancellation():
            code = main()
    except Cancelled as error:
        print("RELEASE_BUILD_FAIL cancelled")
        code = error.code
    except RuntimeError:
        print("RELEASE_BUILD_FAIL private_directory_cleanup_failed")
        code = 1
    raise SystemExit(code)
