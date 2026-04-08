"""Shared pytest fixtures for backend-driven charmnumeric tests."""

from __future__ import annotations

from pathlib import Path
import gc
import os
import sys

import pytest


HERE = Path(__file__).resolve().parent
CHARMNUMERIC_ROOT = HERE.parent
REPO_ROOT = CHARMNUMERIC_ROOT.parent.parent
DEFAULT_SERVER_BINARY = CHARMNUMERIC_ROOT / "src" / "build" / "server.out"


for build_dir in (
    REPO_ROOT / "build" / "lib",
    CHARMNUMERIC_ROOT / "build" / "lib",
):
    build_dir_str = str(build_dir)
    while build_dir_str in sys.path:
        sys.path.remove(build_dir_str)

repo_root_str = str(REPO_ROOT)
while repo_root_str in sys.path:
    sys.path.remove(repo_root_str)
sys.path.insert(0, repo_root_str)

charmnumeric_root_str = str(CHARMNUMERIC_ROOT)
while charmnumeric_root_str in sys.path:
    sys.path.remove(charmnumeric_root_str)
sys.path.insert(1, charmnumeric_root_str)


def pytest_addoption(parser):
    group = parser.getgroup("charmnumeric")
    group.addoption(
        "--charmnumeric-server",
        action="store",
        default=None,
        help="Path to a built charmnumeric server.out binary.",
    )
    group.addoption(
        "--charmnumeric-host",
        action="store",
        default=None,
        help="Reuse an already running charmnumeric backend at this host.",
    )
    group.addoption(
        "--charmnumeric-port",
        action="store",
        type=int,
        default=None,
        help="Port for the charmnumeric backend connection.",
    )
    group.addoption(
        "--charmnumeric-odf",
        action="store",
        type=int,
        default=None,
        help="Objects-per-PE factor used during backend connect.",
    )
    group.addoption(
        "--charmnumeric-pes",
        action="store",
        type=int,
        default=None,
        help="Number of PEs to launch for the local charmnumeric backend.",
    )
    group.addoption(
        "--charmnumeric-charmrun",
        action="store",
        default=None,
        help="Optional path to charmrun for launching the local backend.",
    )
    group.addoption(
        "--charmnumeric-startup-timeout",
        action="store",
        type=float,
        default=None,
        help="Seconds to wait for a launched charmnumeric backend to accept connections.",
    )


def _get_str_option(pytestconfig, option_name, env_name):
    value = pytestconfig.getoption(option_name)
    if value:
        return value
    return os.environ.get(env_name)


def _get_int_option(pytestconfig, option_name, env_name, default):
    value = pytestconfig.getoption(option_name)
    if value is not None:
        return value
    env_value = os.environ.get(env_name)
    if env_value is not None:
        return int(env_value)
    return default


def _get_float_option(pytestconfig, option_name, env_name, default):
    value = pytestconfig.getoption(option_name)
    if value is not None:
        return value
    env_value = os.environ.get(env_name)
    if env_value is not None:
        return float(env_value)
    return default


def _resolve_server_binary(pytestconfig):
    value = _get_str_option(pytestconfig, "charmnumeric_server", "CHARMNUMERIC_SERVER")
    if value:
        path = Path(value).expanduser().resolve()
        if not path.is_file():
            pytest.exit(f"charmnumeric server not found: {path}")
        return path

    if DEFAULT_SERVER_BINARY.is_file():
        return DEFAULT_SERVER_BINARY
    return None


@pytest.fixture(autouse=True)
def _collect_garbage():
    gc.collect()
    yield
    gc.collect()


@pytest.fixture(autouse=True)
def _reset_frontend_state():
    from charmtyles.core import reset_frontend_state

    reset_frontend_state()
    yield
    reset_frontend_state()


@pytest.fixture(scope="session")
def interface(pytestconfig, tmp_path_factory):
    from charmnumeric.interface import LocalCluster, CharmNumericInterface

    server_binary = _resolve_server_binary(pytestconfig)
    host = _get_str_option(pytestconfig, "charmnumeric_host", "CHARMNUMERIC_HOST")
    port = _get_int_option(pytestconfig, "charmnumeric_port", "CHARMNUMERIC_PORT", 1234)
    odf = _get_int_option(pytestconfig, "charmnumeric_odf", "CHARMNUMERIC_ODF", 4)
    pes = _get_int_option(pytestconfig, "charmnumeric_pes", "CHARMNUMERIC_PES", 1)
    charmrun = _get_str_option(pytestconfig, "charmnumeric_charmrun", "CHARMNUMERIC_CHARMRUN")
    startup_timeout = _get_float_option(
        pytestconfig,
        "charmnumeric_startup_timeout",
        "CHARMNUMERIC_STARTUP_TIMEOUT",
        20.0,
    )

    if server_binary is not None:
        cluster = LocalCluster(
            server_binary=str(server_binary),
            server_port=port,
            odf=odf,
            max_pes=pes,
            charmrun=charmrun,
            workdir=tmp_path_factory.mktemp("charmnumeric-backend"),
            startup_timeout=startup_timeout,
        )
        try:
            yield cluster
        finally:
            cluster.close()
        return

    if host:
        iface = CharmNumericInterface()
        iface.connect(host, port, odf)
        yield iface
        return

    pytest.skip(
        "Build example/charmnumeric/src/build/server.out first, or pass "
        "--charmnumeric-server / CHARMNUMERIC_SERVER."
    )
