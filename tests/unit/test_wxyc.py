"""Unit tests for lib/wxyc.py."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from lib.wxyc import connect_mysql


class TestConnectMysql:
    """connect_mysql parses URL components and passes them to pymysql.connect."""

    @patch("lib.wxyc.pymysql")
    def test_standard_url(self, mock_pymysql) -> None:
        connect_mysql("mysql://user:pass@dbhost:3307/wxycmusic")
        mock_pymysql.connect.assert_called_once_with(
            host="dbhost",
            port=3307,
            user="user",
            password="pass",
            database="wxycmusic",
            charset="utf8",
        )

    @patch("lib.wxyc.pymysql")
    def test_default_port(self, mock_pymysql) -> None:
        connect_mysql("mysql://user:pass@dbhost/mydb")
        call_kwargs = mock_pymysql.connect.call_args[1]
        assert call_kwargs["port"] == 3306

    @patch("lib.wxyc.pymysql")
    def test_no_password(self, mock_pymysql) -> None:
        connect_mysql("mysql://user@dbhost:3306/mydb")
        call_kwargs = mock_pymysql.connect.call_args[1]
        assert call_kwargs["user"] == "user"
        assert call_kwargs["password"] == ""

    @patch("lib.wxyc.pymysql")
    def test_returns_connection(self, mock_pymysql) -> None:
        result = connect_mysql("mysql://user:pass@dbhost:3306/mydb")
        assert result == mock_pymysql.connect.return_value
