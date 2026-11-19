"""Explicit mechanism doubles. Never certify this uncommitted checkout as HEAD."""
import importlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(ROOT / "scripts"))
identity = importlib.import_module("release_identity")
builder = importlib.import_module("release_build")
COMMIT = "a" * 40
IMAGE_ID = "sha256:" + "b" * 64
LABELS = {"org.opencontainers.image.revision": "aaaaaaa"}


class ReleaseOriginTests(unittest.TestCase):
    def test_clean_matrix(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for state in (b"", b" M app/src/x.py\0", b"M  app/src/x.py\0", b"?? new.py\0"):
                def git(repo, *args):
                    if args[0] == "config": return b"false\n"
                    if args[0] == "ls-files": return b"H file\0"
                    return (str(root)+"\n").encode() if args[-1] == "--show-toplevel" else state if args[0] == "status" else COMMIT.encode()
                with patch.object(identity, "git", git):
                    if state:
                        with self.assertRaisesRegex(identity.IdentityError, "release_checkout_dirty"):
                            identity.clean_head(root, COMMIT)
                    else:
                        self.assertEqual(identity.clean_head(root, COMMIT), COMMIT)
                print("R5_CLEAN_CASE PASS")

    def test_index_flags_and_sparse_matrix(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            cases=[("H",b"false",None),*( (tag,b"false","release_index_flags_forbidden") for tag in ("h","S","s","m","r","c","k") ),("H",b"true","release_sparse_checkout_forbidden")]
            for tag,sparse,failure in cases:
                def git(repo,*args):
                    if args[0]=="config": return sparse
                    if args[0]=="ls-files": return tag.encode()+b" hidden-file\0"
                    if args[0]=="status": return b""
                    return (str(root)+"\n").encode() if args[-1]=="--show-toplevel" else COMMIT.encode()
                with patch.object(identity,"git",git):
                    if failure:
                        with self.assertRaisesRegex(identity.IdentityError,failure): identity.clean_head(root,COMMIT)
                    else: self.assertEqual(identity.clean_head(root,COMMIT),COMMIT)
                print("P2R5_INDEX_CASE PASS")
            self.assertEqual(len(cases),9)

    def test_raw_context_matrix_and_unsafe_entries(self):
        blobs = {"1": b"FROM python", "2": b"tests/", "3": b"locked", "4": b"print('committed')"}
        records = [("100644", "blob", n, p) for n, p in (("1","Dockerfile"),("2",".dockerignore"),("3","requirements.lock"),("4","src/app.py"))]
        def git(repo, *args):
            if args[0] == "ls-tree":
                return b"\0".join(f"{m} {k} {n}\tapp/{p}".encode() for m,k,n,p in records)+b"\0"
            return blobs[args[-1]]
        with tempfile.TemporaryDirectory() as directory, patch.object(identity, "git", git):
            root = Path(directory)
            files = identity.context_files(root, COMMIT)
            identity.export_context(files, root)
            self.assertEqual((root/"src/app.py").read_bytes(), blobs["4"])
            self.assertEqual(len(identity.context_hash(files)), 64)
            self.assertEqual({p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()}, {r[3] for r in records})
            print("R5_CONTEXT_CASE PASS raw_blobs")
            for mode, kind, path in (("120000","blob","link"),("160000","commit","submodule"),("100644","blob","../escape"),("100644","blob","src/../../escape"),("100644","blob","src/x\\y")):
                records.append((mode,kind,"4",path))
                with self.assertRaises(identity.IdentityError): identity.context_files(root, COMMIT)
                records.pop(); print("R5_CONTEXT_CASE PASS unsafe_entry")
            blobs["4"] = b"version https://git-lfs.github.com/spec/v1\n"
            with self.assertRaisesRegex(identity.IdentityError, "lfs_unsupported"):
                identity.context_files(root, COMMIT)
            print("R5_CONTEXT_CASE PASS lfs")

    def test_receipt_matrix(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); path = root/"receipt.json"
            expected = {"commit":COMMIT,"project":"p50-fixture","version":"aaaaaaa","contract":identity.CONTEXT_CONTRACT,"context_sha256":"c"*64,"image_id":IMAGE_ID}
            with patch.object(identity,"clean_head", return_value=COMMIT), patch.object(identity,"receipt_path",return_value=path), patch.object(identity,"inspect_image", return_value=(IMAGE_ID,LABELS)), patch.object(identity,"expected_receipt",return_value=expected):
                for key in (None,"missing","invalid",*expected):
                    path.unlink(missing_ok=True)
                    if key == "invalid": path.write_text("not json")
                    elif key != "missing": path.write_text(json.dumps(expected if key is None else expected | {key:"different"}))
                    if key is None: identity.verify_receipt(root,"p50-fixture","aaaaaaa")
                    else:
                        with self.assertRaises(identity.IdentityError): identity.verify_receipt(root,"p50-fixture","aaaaaaa")
                    print("R5_RECEIPT_CASE PASS")
                path.write_text(json.dumps(expected))
                with patch.object(identity,"inspect_image",side_effect=[(IMAGE_ID,LABELS),("sha256:"+"d"*64,LABELS)]):
                    with self.assertRaisesRegex(identity.IdentityError,"release_image_changed"):
                        identity.verify_receipt(root,"p50-fixture","aaaaaaa")
                print("R5_RECEIPT_CASE PASS tag_race")

    def test_build_failure_and_cancel_invalidate_receipt(self):
        from release_process import Cancelled
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"project.json"
            for error in (ValueError("failure"), Cancelled(signal.SIGTERM)):
                path.write_text("old")
                with patch.object(builder,"clean_head",return_value=COMMIT), patch.object(builder,"git",return_value=b"aaaaaaa"), patch.object(builder,"receipt_path",return_value=path), patch.object(builder,"context_files",side_effect=error):
                    with self.assertRaises(type(error)): builder.build(Path(directory),"p50-fixture",COMMIT)
                self.assertFalse(path.exists())
                print("R5_BUILD_FAILURE_CASE PASS")

    def test_builder_success_and_failure_sequence(self):
        from contextlib import contextmanager
        from release_process import Cancelled
        sample=[("Dockerfile",0o644,b"FROM synthetic"),("src/x.py",0o644,b"committed")]
        cases=("success","build_error","baked_error","wrong_id","wrong_label","race_before","race_after","cancel_after","publish_error","head_changed","cleanup_error")
        for case in cases:
            with tempfile.TemporaryDirectory() as directory:
                root=Path(directory); path=root/"receipt.json"; path.write_text("old")
                receipt={"version":"aaaaaaa","image_id":IMAGE_ID}
                def docker(command, **kwargs):
                    self.assertNotEqual(Path(command[-1]), root/"app")
                    self.assertEqual((Path(command[-1])/"src/x.py").read_bytes(),b"committed")
                    if case=="build_error": raise subprocess.CalledProcessError(1,command)
                    Path(command[command.index("--iidfile")+1]).write_text(IMAGE_ID)
                inspections=[(IMAGE_ID,LABELS)]*4
                if case=="wrong_id": inspections[0]=("sha256:"+"c"*64,LABELS)
                if case=="wrong_label": inspections[0]=(IMAGE_ID,{})
                if case=="race_before": inspections[2]=("sha256:"+"c"*64,LABELS)
                if case=="race_after": inspections[3]=("sha256:"+"c"*64,LABELS)
                if case=="cancel_after": inspections[3]=Cancelled(signal.SIGTERM)
                @contextmanager
                def private(prefix):
                    with tempfile.TemporaryDirectory() as d: yield Path(d)
                    if case=="cleanup_error": raise RuntimeError("private_directory_cleanup_failed")
                with patch.object(builder,"clean_head",side_effect=[COMMIT,identity.IdentityError("release_checkout_dirty")] if case=="head_changed" else None,return_value=COMMIT), patch.object(builder,"git",return_value=b"aaaaaaa"), patch.object(builder,"receipt_path",return_value=path), patch.object(builder,"context_files",return_value=sample), patch.object(builder,"run",side_effect=docker), patch.object(builder,"inspect_image",side_effect=inspections), patch.object(builder,"check_baked",side_effect=identity.IdentityError("release_baked_version_mismatch") if case=="baked_error" else None), patch.object(builder,"expected_receipt",return_value=receipt), patch.object(builder,"private_directory",private):
                    if case=="success":
                        builder.build(root,"p50-fixture",COMMIT)
                        self.assertEqual(json.loads(path.read_text()),receipt)
                    else:
                        if case=="publish_error":
                            with patch.object(builder,"publish_receipt",side_effect=OSError()):
                                with self.assertRaises(BaseException): builder.build(root,"p50-fixture",COMMIT)
                        else:
                            with self.assertRaises(BaseException): builder.build(root,"p50-fixture",COMMIT)
                        self.assertFalse(path.exists(),case)
                print("R5_BUILDER_SEQUENCE PASS case="+case)

    def test_lock_and_atomic_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"project.json"
            with identity.build_lock(path):
                with self.assertRaisesRegex(identity.IdentityError,"release_build_locked"):
                    with identity.build_lock(path): pass
            identity.publish_receipt(path,{"v":1})
            self.assertEqual(json.loads(path.read_text()),{"v":1})
            self.assertEqual(path.stat().st_mode & 0o777,0o600)
            with patch.object(identity.os,"replace",side_effect=OSError()):
                with self.assertRaises(OSError): identity.publish_receipt(path,{"v":2})
            self.assertEqual(json.loads(path.read_text()),{"v":1})
            self.assertFalse(list(Path(directory).glob(".receipt-*")))
            print("R5_ATOMIC_CASE PASS lock_and_rename")


class ReleaseCancellationTests(unittest.TestCase):
    def test_timeout_and_child_error_cleanup(self):
        from release_process import private_directory, run
        echoed = run([sys.executable,"-c","import sys;print(sys.stdin.read())"], input="synthetic-input", capture_output=True, text=True, check=True)
        self.assertEqual(echoed.stdout, "synthetic-input\n")
        print("R5_EXECUTOR_STDIN PASS")
        for mode in ("timeout","error"):
            owned=None
            with self.assertRaises(subprocess.SubprocessError):
                with private_directory("wpp-release-test-") as owned:
                    if mode=="timeout":
                        run([sys.executable,"-c","import time;time.sleep(60)"],capture_output=True,timeout=.05,check=True)
                    else:
                        run([sys.executable,"-c","raise SystemExit(3)"],capture_output=True,timeout=5,check=True)
            self.assertFalse(owned.exists())
            print("R5_CANCEL_CASE PASS "+mode)

    def test_signals_cleanup_owned_snapshot_and_children(self):
        # Real subprocesses/signals; fake Docker only blocks and reports the
        # exact owned snapshot. No real candidate/env or Docker resource needed.
        adapter = '''import sys,signal
sys.path.insert(0,sys.argv[1])
import release_preflight as p
from release_process import cancellation,Cancelled
from pathlib import Path
p.ROOT=Path(sys.argv[2])
try:
 with cancellation(): p.deploy_config(Path(sys.argv[3]))
except Cancelled as e: raise SystemExit(e.code)
'''
        fake = '''#!REPLACE_PYTHON
import os,signal,sys,time
from pathlib import Path
snapshot=Path(sys.argv[sys.argv.index("--project-directory")+1])
child=os.fork()
if child==0:
 while True: time.sleep(1)
def stop(s,f):
 os.waitpid(child,0)
 raise SystemExit(143)
signal.signal(signal.SIGTERM,stop)
signal.signal(signal.SIGINT,stop)
Path(os.environ["SPY"]).write_text(str(snapshot)+"\\n"+str(os.getpid())+"\\n"+str(child))
while True: time.sleep(1)
'''.replace("REPLACE_PYTHON",sys.executable)
        for signum in (signal.SIGTERM,signal.SIGINT,signal.SIGKILL):
            with tempfile.TemporaryDirectory() as directory:
                root=Path(directory); (root/"docker-compose.yml").write_text("services: {}\n")
                candidate=root/"candidate";candidate.write_text("SYNTHETIC_SECRET=do-not-print\n");candidate.chmod(0o600)
                (root/"docker").write_text(fake);(root/"docker").chmod(0o700)
                spy=root/"spy"
                env={"PATH":directory+os.pathsep+os.defpath,"SPY":str(spy),"LANG":"C.UTF-8"}
                proc=subprocess.Popen([sys.executable,"-c",adapter,str(ROOT/"scripts"),str(root),str(candidate)],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                snapshot=None;leader=None
                try:
                    deadline=time.monotonic()+10
                    while not spy.exists() and time.monotonic()<deadline: time.sleep(.02)
                    if not spy.exists():
                        out,err=proc.communicate(timeout=2)
                        self.fail("blocking child did not start: "+out+err)
                    snapshot_text,leader,child=spy.read_text().splitlines();snapshot=Path(snapshot_text)
                    self.assertEqual(snapshot.stat().st_mode & 0o777,0o700)
                    self.assertEqual((snapshot/".env").stat().st_mode & 0o777,0o600)
                    proc.send_signal(signum);out,err=proc.communicate(timeout=10)
                    self.assertNotIn("do-not-print",out+err)
                    if signum==signal.SIGKILL:
                        self.assertEqual(proc.returncode,-signal.SIGKILL);self.assertTrue(snapshot.exists())
                        os.killpg(int(leader),signal.SIGTERM)
                        print("R5_CANCEL_CASE PASS sigkill_limit")
                    else:
                        self.assertEqual(proc.returncode,128+signum,err)
                        self.assertFalse(snapshot.exists())
                        for pid in (leader,child):
                            with self.assertRaises(ProcessLookupError): os.kill(int(pid),0)
                        print("R5_CANCEL_CASE PASS unwind")
                finally:
                    if proc.poll() is None: proc.kill()
                    proc.communicate(timeout=10)
                    if leader:
                        try: os.killpg(int(leader),signal.SIGKILL)
                        except ProcessLookupError: pass
                    if snapshot is not None and snapshot.exists():
                        # Exact fixture-reported path only, never a /tmp scan.
                        import shutil
                        shutil.rmtree(snapshot)
