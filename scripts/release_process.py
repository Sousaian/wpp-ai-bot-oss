"""Bounded child ownership and CLI-only cancellation; no global import handlers."""
from contextlib import contextmanager
import os
from pathlib import Path
import signal
import subprocess
import tempfile


class Cancelled(BaseException):
    def __init__(self, signum: int):
        self.code = 128 + signum


@contextmanager
def cancellation():
    def stop(signum, frame):
        raise Cancelled(signum)
    previous = {s: signal.signal(s, stop) for s in (signal.SIGTERM, signal.SIGINT)}
    try:
        yield
    finally:
        for s, handler in previous.items():
            signal.signal(s, handler)


@contextmanager
def shield_cleanup():
    previous = {s: signal.signal(s, signal.SIG_IGN) for s in (signal.SIGTERM, signal.SIGINT)}
    try:
        yield
    finally:
        for s, handler in previous.items():
            signal.signal(s, handler)


def run(args, *, input=None, timeout=30, check=False, capture_output=False, **kwargs):
    if capture_output:
        kwargs.update(stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if input is not None:
        kwargs["stdin"] = subprocess.PIPE
    process = subprocess.Popen(args, start_new_session=True, **kwargs)
    try:
        stdout, stderr = process.communicate(input, timeout=timeout)
    except BaseException:
        with shield_cleanup():
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.communicate(timeout=0.5)
            except subprocess.TimeoutExpired:
                pass
            finally:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.communicate(timeout=5)
        raise
    result = subprocess.CompletedProcess(args, process.returncode, stdout, stderr)
    if check:
        result.check_returncode()
    return result


@contextmanager
def private_directory(prefix: str):
    directory = tempfile.TemporaryDirectory(prefix=prefix)
    try:
        yield Path(directory.name)
    finally:
        with shield_cleanup():
            try:
                directory.cleanup()
            except OSError:
                raise RuntimeError("private_directory_cleanup_failed") from None


def private_copy(source: Path, target: Path):
    # Exclusive creation with 0600, no permissive interval even inside 0700.
    with source.open("rb") as src, os.fdopen(os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as dst:
        while block := src.read(65536):
            dst.write(block)
