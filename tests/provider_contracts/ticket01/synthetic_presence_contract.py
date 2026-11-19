#!/usr/bin/env python3
import json
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


ALLOWED_PRESENCE = {"unavailable", "available", "composing", "recording", "paused"}


class PresenceFixture(ThreadingHTTPServer):
    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), PresenceHandler)
        self.events: list[tuple[str, str]] = []
        self.finished = threading.Event()


class PresenceHandler(BaseHTTPRequestHandler):
    server: PresenceFixture

    def log_message(self, *_args: object) -> None:
        return

    def respond(self, status: int, body: dict[str, object]) -> None:
        encoded = json.dumps(body).encode()
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
        except BrokenPipeError:
            pass

    def do_POST(self) -> None:
        if self.path != "/chat/sendPresence/synthetic-instance":
            self.respond(404, {"error": "not found"})
            return
        try:
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        except (ValueError, TypeError):
            self.respond(400, {"error": "invalid json"})
            return
        if (
            not isinstance(payload, dict)
            or not isinstance(payload.get("number"), str)
            or not payload["number"]
            or payload.get("presence") not in ALLOWED_PRESENCE
            or not isinstance(payload.get("delay"), (int, float))
        ):
            self.respond(400, {"error": "invalid presence payload"})
            return
        if payload["number"] == "force-error":
            self.respond(400, {"error": "synthetic downstream error"})
            return

        self.server.events.append(("presence", str(payload["presence"])))
        # Exact 2.3.7 source semantics for planned 2-12 second values: request
        # remains open for delay milliseconds, then provider emits paused.
        time.sleep(max(float(payload["delay"]), 0.0) / 1000.0)
        self.server.events.append(("presence", "paused"))
        self.server.finished.set()
        self.respond(201, {"presence": payload["presence"]})


def post(port: int, payload: dict[str, object], timeout: float = 2.0) -> tuple[int, dict[str, object]]:
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/chat/sendPresence/synthetic-instance",
        data=json.dumps(payload).encode(),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def main() -> None:
    fixture = PresenceFixture()
    thread = threading.Thread(target=fixture.serve_forever, daemon=True)
    thread.start()
    port = fixture.server_address[1]
    try:
        started = time.monotonic()
        status, body = post(
            port,
            {"number": "synthetic-contact", "presence": "composing", "delay": 60},
        )
        elapsed_ms = (time.monotonic() - started) * 1000
        assert status == 201 and body == {"presence": "composing"}
        assert elapsed_ms >= 50
        assert fixture.events[-2:] == [
            ("presence", "composing"),
            ("presence", "paused"),
        ]

        status, _ = post(
            port,
            {"number": "synthetic-contact", "presence": "paused", "delay": 0},
        )
        assert status == 201
        assert fixture.events[-2:] == [("presence", "paused"), ("presence", "paused")]

        status, _ = post(
            port,
            {"number": "synthetic-contact", "presence": "invalid", "delay": 1},
        )
        assert status == 400
        status, _ = post(
            port,
            {"number": "force-error", "presence": "composing", "delay": 1},
        )
        assert status == 400

        fixture.finished.clear()
        before_timeout_events = len(fixture.events)
        try:
            post(
                port,
                {"number": "synthetic-contact", "presence": "composing", "delay": 180},
                timeout=0.025,
            )
            raise AssertionError("client timeout expected")
        except TimeoutError:
            pass
        assert fixture.finished.wait(1.0)
        assert fixture.events[before_timeout_events:] == [
            ("presence", "composing"),
            ("presence", "paused"),
        ]

        report = {
            "evidence": "SYNTHETIC_PRESENCE",
            "root_payload": True,
            "delay_unit": "milliseconds",
            "response_blocks": True,
            "auto_pause": True,
            "explicit_pause": True,
            "validation_error": 400,
            "downstream_error": 400,
            "client_timeout_does_not_cancel_server_pause": True,
            "combined_text_send_exercised": False,
        }
        print(json.dumps(report, sort_keys=True))
    finally:
        fixture.shutdown()
        fixture.server_close()
        thread.join(timeout=2)


if __name__ == "__main__":
    main()

