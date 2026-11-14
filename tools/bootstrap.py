#!/usr/bin/env python3
"""Explicit API bootstrap; existing definitions are preserved. Python 3 only.

Use SEATH_USERNAME/SEATH_PASSWORD for an existing administrator. Manifest
credentials use username_env/password_env/token_env, never literal secrets.
"""
import argparse
import hashlib
import http.cookiejar
import json
import os
from pathlib import Path
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise RuntimeError("Unexpected redirect; use the final HTTPS URL")


def validate_manifest(manifest):
    """Reject ambiguous input before authenticating or changing server state."""
    if not isinstance(manifest, dict) or set(manifest) - {"modules", "presets", "mappings"}:
        raise RuntimeError("Manifest must contain only modules, presets, and mappings")
    modules, presets, mappings = manifest.get("modules", []), manifest.get("presets", []), manifest.get("mappings", {})
    if not isinstance(modules, list) or not isinstance(presets, list) or not isinstance(mappings, dict):
        raise RuntimeError("Invalid manifest collection types")
    for entries, required, optional, identity in (
        (modules, {"path", "version", "descriptor_file"}, set(), "path"),
        (presets, {"name", "url"}, {"username_env", "password_env", "token_env"}, "name"),
    ):
        seen = set()
        for entry in entries:
            if not isinstance(entry, dict) or not required <= set(entry) or set(entry) - required - optional:
                raise RuntimeError("Invalid manifest entry fields; credentials require environment references")
            if any(not isinstance(v, str) or not v.strip() or v != v.strip() or "\r" in v or "\n" in v for v in entry.values()):
                raise RuntimeError("Manifest fields must be nonempty single-line strings without outer whitespace")
            if entry[identity] in seen:
                raise RuntimeError("Duplicate manifest identity")
            seen.add(entry[identity])
    for preset in presets:
        url = urllib.parse.urlsplit(preset["url"])
        if url.username is not None or url.password is not None or url.query or url.fragment:
            raise RuntimeError("Preset URL must not contain credentials, query, or fragment")
    if any(not isinstance(k, str) or not k.strip() or not isinstance(v, str) or not v.strip() for k, v in mappings.items()):
        raise RuntimeError("Mappings require nonempty topic and type names")
    return modules, presets, mappings


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise RuntimeError("Duplicate JSON key")
        result[key] = value
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--url", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--activate", action="store_true")
    args = parser.parse_args()
    origin = urllib.parse.urlsplit(args.url)
    if origin.username or origin.password or origin.query or origin.fragment:
        raise RuntimeError("URL must not contain credentials, query, or fragment")
    if origin.scheme != "https" and not (origin.scheme == "http" and origin.hostname in ("localhost", "127.0.0.1", "::1")):
        raise RuntimeError("Use HTTPS or a loopback port-forward")
    base = args.url.rstrip("/")
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()), NoRedirect())

    def request(method, path, body=None, headers=None):
        headers = dict(headers or {})
        if body is not None and not isinstance(body, bytes):
            body = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(base + path, data=body, method=method, headers=headers)
        try:
            with opener.open(req, timeout=60) as response:
                data = response.read()
                return json.loads(data) if data else None
        except urllib.error.HTTPError as error:
            raise RuntimeError(f"{method} {path} returned HTTP {error.code}") from None

    manifest = json.loads(args.manifest.read_text(), object_pairs_hook=unique_object)
    modules, presets, mappings = validate_manifest(manifest)
    request("POST", "/api/login", {"username": os.environ["SEATH_USERNAME"], "password": os.environ["SEATH_PASSWORD"]})
    try:
        history = request("GET", "/api/proto/modules")["history"] or []
        known_modules = {(m["path"], m["version"]): m for m in history}
        known_presets = {p["name"]: p for p in request("GET", "/api/presets")["presets"] or []}
        known_mappings = {m["topic"]: m["proto_name"] for m in request("GET", "/api/topic-mappings")["mappings"] or []}
        pending_modules, pending_presets, pending_mappings = [], [], []
        for module in modules:
            data = (args.manifest.parent / module["descriptor_file"]).read_bytes()
            if not data or len(data) > 32 * 1024 * 1024:
                raise RuntimeError("Descriptor bundle must be between 1 byte and 32 MiB")
            existing = known_modules.get((module["path"], module["version"]))
            if existing and existing["sha256"] != hashlib.sha256(data).hexdigest():
                raise RuntimeError("Descriptor version conflicts with stored content")
            pending_modules.append((module, data, existing))
        for preset in presets:
            existing = known_presets.get(preset["name"])
            if existing:
                if existing["url"] != preset["url"]:
                    raise RuntimeError("Existing environment URL conflicts with manifest")
                continue
            body = {"name": preset["name"], "url": preset["url"]}
            for field in ("username", "password", "token"):
                if field + "_env" in preset:
                    body[field] = os.environ[preset[field + "_env"]]
            pending_presets.append(body)
        for topic, name in mappings.items():
            if topic in known_mappings:
                if known_mappings[topic] != name:
                    raise RuntimeError("Existing topic mapping conflicts with manifest")
            else:
                pending_mappings.append({"topic": topic, "proto_name": name})
        needs_activation = args.activate and any(not m or not m["active"] for _, _, m in pending_modules)
        print(json.dumps({"new_modules": sum(m is None for _, _, m in pending_modules), "new_presets": len(pending_presets),
                          "new_mappings": len(pending_mappings), "activation_requested": bool(needs_activation), "apply": args.apply}))
        if not args.apply:
            return
        for module, data, existing in pending_modules:
            if not existing:
                boundary = uuid.uuid4().hex
                parts = []
                for key, value in (("module_path", module["path"]), ("version", module["version"])):
                    parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode())
                parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="descriptor_set"; filename="events.protoset"\r\nContent-Type: application/octet-stream\r\n\r\n'.encode() + data + b"\r\n")
                parts.append(f"--{boundary}--\r\n".encode())
                existing = request("POST", "/api/proto/modules", b"".join(parts), {"Content-Type": "multipart/form-data; boundary=" + boundary})
            if args.activate and not existing["active"]:
                request("PUT", f'/api/proto/modules/{existing["id"]}/activate')
        active_types = {t["name"] for t in request("GET", "/api/proto/types")["types"] or []}
        if any(m["proto_name"] not in active_types for m in pending_mappings):
            raise RuntimeError("Mappings require active types; stage/activate bundles first")
        for preset in pending_presets:
            request("POST", "/api/presets", preset, {"If-None-Match": "*"})
        for mapping in pending_mappings:
            request("POST", "/api/topic-mappings", mapping)
        print("Bootstrap complete. Existing definitions preserved.")
    finally:
        request("POST", "/api/logout")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, KeyError, ValueError, OSError) as error:
        print("Bootstrap failed: " + (str(error) if isinstance(error, RuntimeError) else type(error).__name__), file=sys.stderr)
        sys.exit(1)
