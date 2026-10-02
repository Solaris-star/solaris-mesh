#!/usr/bin/env python3
"""Compare Bubblewrap executable identities without changing host policy.

These probes execute only /usr/bin/true. They diagnose namespace startup, not
Solaris sandbox enforcement; the strict Linux integration suite remains the gate.
"""

import errno
import fcntl
import hashlib
import os
from pathlib import Path
import platform
import stat
import subprocess


SAFE_ENVIRONMENT = {"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C"}
PROBE_TIMEOUT_SECONDS = 15
MAX_OUTPUT_BYTES = 8192
MFD_EXEC = 0x0010
# Linux UAPI values also support Python builds that omit the fcntl names.
F_ADD_SEALS = getattr(fcntl, "F_ADD_SEALS", 1033)
F_GET_SEALS = getattr(fcntl, "F_GET_SEALS", 1034)
REQUIRED_SEALS = 0x0001 | 0x0002 | 0x0004 | 0x0008


def read_diagnostic(path, *, matching=None):
    try:
        lines = Path(path).read_text().splitlines()
    except OSError as error:
        print(f"{path}: unavailable ({error.strerror})", flush=True)
        return
    if matching is not None:
        lines = [line for line in lines if matching(line)]
    text = "\n".join(lines).replace("\0", "")
    print(f"{path}:\n{text[:MAX_OUTPUT_BYTES] or '(no matching entries)'}", flush=True)


def print_host_diagnostics():
    print(f"kernel: {platform.release()}; uid: {os.getuid()}; gid: {os.getgid()}", flush=True)
    for path in (
        "/proc/self/attr/current",
        "/sys/kernel/security/lsm",
        "/proc/sys/kernel/unprivileged_userns_clone",
        "/proc/sys/kernel/apparmor_restrict_unprivileged_userns",
        "/proc/sys/kernel/apparmor_restrict_unprivileged_unconfined",
        "/proc/sys/user/max_user_namespaces",
        "/proc/sys/user/max_net_namespaces",
        "/etc/apparmor.d/bwrap",
        "/etc/apparmor.d/bwrap-userns-restrict",
        "/etc/apparmor.d/unprivileged_userns",
    ):
        read_diagnostic(path)
    read_diagnostic(
        "/proc/self/status",
        matching=lambda line: line.startswith(("Cap", "NoNewPrivs:", "Seccomp")),
    )
    read_diagnostic(
        "/sys/kernel/security/apparmor/profiles",
        matching=lambda line: "bwrap" in line or "userns" in line,
    )


def sealed_snapshot(source):
    flags = os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING
    try:
        descriptor = os.memfd_create("solaris-bwrap-diagnostic", flags | MFD_EXEC)
    except OSError as error:
        if error.errno != errno.EINVAL:
            raise
        descriptor = os.memfd_create("solaris-bwrap-diagnostic", flags)
    try:
        with os.fdopen(os.dup(descriptor), "wb") as output:
            output.write(source)
        os.fchmod(descriptor, 0o500)
        fcntl.fcntl(descriptor, F_ADD_SEALS, REQUIRED_SEALS)
        if fcntl.fcntl(descriptor, F_GET_SEALS) & REQUIRED_SEALS != REQUIRED_SEALS:
            raise RuntimeError("The executable snapshot is not fully sealed")
        os.lseek(descriptor, 0, os.SEEK_SET)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def describe_executable(label, descriptor):
    metadata = os.fstat(descriptor)
    digest = hashlib.sha256()
    offset = 0
    while chunk := os.pread(descriptor, 65536, offset):
        digest.update(chunk)
        offset += len(chunk)
    print(
        f"{label}: fd_target={os.readlink(f'/proc/self/fd/{descriptor}')} "
        f"device={metadata.st_dev} inode={metadata.st_ino} "
        f"uid={metadata.st_uid} gid={metadata.st_gid} "
        f"mode={stat.S_IMODE(metadata.st_mode):04o} sha256={digest.hexdigest()}",
        flush=True,
    )


def run_probe(label, executable, arguments, descriptors):
    print(f"probe {label}: executable={executable}", flush=True)
    try:
        result = subprocess.run(
            ["bwrap", *arguments],
            executable=executable,
            env=SAFE_ENVIRONMENT,
            cwd="/",
            pass_fds=descriptors,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=PROBE_TIMEOUT_SECONDS,
            check=False,
        )
        output = result.stdout
        status = f"exit={result.returncode}"
    except subprocess.TimeoutExpired as error:
        output = error.stdout or b""
        status = f"timed out after {PROBE_TIMEOUT_SECONDS}s"
    except OSError as error:
        output = b""
        status = f"launch failed: {error}"
    print(f"probe {label}: {status}", flush=True)
    if output:
        print(output[:MAX_OUTPUT_BYTES].decode("utf-8", errors="replace").rstrip(), flush=True)
    return status


def main():
    print_host_diagnostics()
    path = next((path for path in ("/usr/bin/bwrap", "/bin/bwrap") if Path(path).is_file()), None)
    if path is None:
        raise RuntimeError("Bubblewrap is absent after the prerequisite installation step")
    arguments = [
        "--unshare-user", "--unshare-pid", "--unshare-net", "--unshare-ipc", "--unshare-uts",
        "--disable-userns", "--assert-userns-disabled", "--cap-drop", "ALL",
        "--new-session", "--die-with-parent", "--hostname", "solaris",
        "--ro-bind", "/", "/", "--proc", "/proc", "--dev", "/dev",
        "--", "/usr/bin/true",
    ]
    print(f"identical namespace probe arguments: {arguments}", flush=True)
    with open(path, "rb") as installed:
        metadata = os.fstat(installed.fileno())
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & (stat.S_ISUID | stat.S_ISGID):
            raise RuntimeError("Diagnostics require a regular, non-setuid, non-setgid Bubblewrap binary")
        snapshot = sealed_snapshot(installed.read())
        try:
            descriptors = (installed.fileno(), snapshot)
            describe_executable("installed inode", installed.fileno())
            describe_executable("sealed snapshot", snapshot)
            results = {}
            for label, executable in (
                ("installed pathname", path),
                ("retained installed inode", f"/proc/self/fd/{installed.fileno()}"),
                ("sealed anonymous snapshot", f"/proc/self/fd/{snapshot}"),
            ):
                results[label] = run_probe(label, executable, arguments, descriptors)
            print(f"namespace startup comparison: {results}", flush=True)
            print("Diagnostic results do not replace or relax the strict Linux integration tests.", flush=True)
        finally:
            os.close(snapshot)


if __name__ == "__main__":
    main()
