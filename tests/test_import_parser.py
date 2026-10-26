from app.ai.tasks.import_parser import parse_delimited


def test_pipe_format_one_line_per_account():
    hint = "ID|PASS|MAIL|PASSMAIL|REFRESH_TOKEN|CLIENTID|MAIL KP|COOKIE"
    content = (
        "user_a|secret1|a@x.com|mailpass1|tok1|client1|kp1|ttwid=aaa; sessionid=s1\n"
        "user_b|secret2|b@x.com|mailpass2|tok2|client2|kp2|ttwid=bbb; sessionid=s2\n"
    )
    rows = parse_delimited("accounts", hint, content)
    assert rows is not None
    assert len(rows) == 2
    assert rows[0]["account_id"] == "user_a"
    assert rows[0]["password"] == "secret1"
    assert rows[0]["email"] == "a@x.com"
    assert rows[0]["email_password"] == "mailpass1"
    assert rows[0]["token"] == "tok1"
    assert rows[0]["cookie"] == "ttwid=aaa; sessionid=s1"
    assert rows[1]["account_id"] == "user_b"


def test_prose_format_falls_through_to_kira():
    assert parse_delimited("accounts", "each line is email then password", "a@x.com secret") is None


def test_line_with_more_fields_than_format_is_rejected_not_merged_into_last_column():
    """26/10/2026: FORMAT 5 cột, dữ liệu 10 trường -> ô MAIL từng nhận "token|email|mật khẩu mail|||tên"."""
    import pytest

    hint = "UID|PASS|2FA|COOKIE|MAIL"
    content = "6159|pw|C2KX|c_user=1; xs=2|EAAD123|a@gmail.com|mailpw|||Tên\n"
    with pytest.raises(ValueError, match="Dòng 1 có 10 trường nhưng FORMAT chỉ có 5 cột"):
        parse_delimited("accounts", hint, content)


def test_trailing_empty_fields_are_fine():
    rows = parse_delimited("accounts", "UID|PASS|MAIL", "u1|p1|a@x.com||\n")
    assert rows is not None and rows[0]["email"] == "a@x.com"
