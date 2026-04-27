from pathlib import Path

from etl.dotenv_local import merge_dotenv_into_environ


def test_merge_dotenv_maps_tushare_api_key_to_token(tmp_path: Path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text("TUSHARE_API_KEY=abc123\n", encoding="utf-8")
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.delenv("TUSHARE_API_KEY", raising=False)

    merge_dotenv_into_environ(tmp_path)

    assert "abc123" == __import__("os").environ.get("TUSHARE_API_KEY")
    assert "abc123" == __import__("os").environ.get("TUSHARE_TOKEN")
