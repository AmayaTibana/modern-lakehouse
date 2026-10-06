import json
import re

from src.ingest import common


def test_today_partition_format():
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", common.today_partition())


def test_write_json_local(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "STORAGE_MODE", "local")
    monkeypatch.setattr(common, "LOCAL_ROOT", tmp_path)

    common.write_json("pokeapi", "pokemon_1.json", {"id": 1})

    saved = tmp_path / "raw" / "pokeapi" / f"dt={common.today_partition()}" / "pokemon_1.json"
    assert saved.exists()
    assert json.loads(saved.read_text()) == {"id": 1}