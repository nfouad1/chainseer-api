from chainseer import _safe_rpc_label


def test_rpc_log_label_omits_path_query_and_credentials():
    rpc_url = "https://user:password@provider.invalid:8443/v2/secret-key?token=also-secret"

    label = _safe_rpc_label(rpc_url)

    assert label == "https://provider.invalid:8443"
    assert "secret" not in label
    assert "password" not in label


def test_rpc_log_label_fails_closed_for_unparseable_value():
    assert _safe_rpc_label("not a URL or secret-key") == "configured"
