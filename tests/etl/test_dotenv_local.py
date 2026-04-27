"""etl.dotenv_local merges .env into os.environ for blank keys only."""

from __future__ import annotations

import os

from etl.dotenv_local import merge_dotenv_into_environ


def test_merge_dotenv_fills_blank_key(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        'FOO_FROM_ENV=bar\n# comment\nEMPTY_WILL_SET=baz\n',
        encoding="utf-8",
    )
    monkeypatch.delenv("FOO_FROM_ENV", raising=False)
    monkeypatch.delenv("EMPTY_WILL_SET", raising=False)
    merge_dotenv_into_environ(tmp_path)
    assert os.environ["FOO_FROM_ENV"] == "bar"
    assert os.environ["EMPTY_WILL_SET"] == "baz"


def test_merge_dotenv_does_not_override_existing(tmp_path, monkeypatch):
    (tmp_path / ".env").write_text("FOO_FROM_ENV=from_file\n", encoding="utf-8")
    monkeypatch.setenv("FOO_FROM_ENV", "already_set")
    merge_dotenv_into_environ(tmp_path)
    assert os.environ["FOO_FROM_ENV"] == "already_set"
