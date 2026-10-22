"""The data generator needs no database: it must be deterministic and its manifest must add up."""
import json

import pytest

from src.contracts import CONTRACTS
from src.generate_data import generate


def test_generator_is_deterministic(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    manifest_a = generate(a, n_bookings=500, n_guests=150, seed=11)
    manifest_b = generate(b, n_bookings=500, n_guests=150, seed=11)
    assert manifest_a == manifest_b
    for contract in CONTRACTS.values():
        assert (a / contract.csv_file).read_bytes() == (b / contract.csv_file).read_bytes()


def test_different_seed_gives_different_data(tmp_path):
    generate(tmp_path / "a", n_bookings=300, n_guests=100, seed=1)
    generate(tmp_path / "b", n_bookings=300, n_guests=100, seed=2)
    assert (tmp_path / "a" / "bookings.csv").read_bytes() != (tmp_path / "b" / "bookings.csv").read_bytes()


def test_manifest_arithmetic_and_headers(tmp_path):
    manifest = generate(tmp_path, n_bookings=800, n_guests=200, defect_rate=0.05, seed=3)
    for name, t in manifest["tables"].items():
        assert t["expected_valid_rows"] == t["rows_written"] - t["duplicates"] - sum(t["defects"].values()), name
        header = (tmp_path / CONTRACTS[name].csv_file).read_text(encoding="utf-8").splitlines()[0]
        assert header.split(",") == list(CONTRACTS[name].columns)
    on_disk = json.loads((tmp_path / "defects_manifest.json").read_text(encoding="utf-8"))
    assert on_disk == manifest


def test_zero_defect_rate_injects_no_rejects(tmp_path):
    manifest = generate(tmp_path, n_bookings=400, n_guests=100, defect_rate=0.0, seed=5)
    for t in manifest["tables"].values():
        assert t["defects"] == {}
        assert t["duplicates"] == 0


@pytest.mark.parametrize("rate", [-0.1, 0.9])
def test_invalid_defect_rate_is_rejected(tmp_path, rate):
    with pytest.raises(ValueError):
        generate(tmp_path, defect_rate=rate)
