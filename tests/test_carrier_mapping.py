from app.services.sf_automation import (
    CARRIER_CN_NAME,
    CARRIER_MAP,
    CARRIER_REVERSE_MAP,
    DELIVERY_WAY_CARRIER_MAP,
    _normalize_carrier_code,
)


def test_yunda_uses_confirmed_kingdee_delivery_way() -> None:
    assert CARRIER_MAP["1059"] == ("YUNDA", "YUNDA", "1")
    assert CARRIER_REVERSE_MAP["YUNDA"] == "1059"
    assert CARRIER_REVERSE_MAP["YD"] == "1059"
    assert DELIVERY_WAY_CARRIER_MAP["1059"] == "YUNDA"


def test_yunda_alias_is_normalized_and_named() -> None:
    assert _normalize_carrier_code("yd") == "YUNDA"
    assert _normalize_carrier_code("yunda") == "YUNDA"
    assert CARRIER_CN_NAME["YD"] == "韵达快递"
    assert CARRIER_CN_NAME["YUNDA"] == "韵达快递"


def test_zhongtong_does_not_reuse_yunda_delivery_way() -> None:
    assert CARRIER_REVERSE_MAP.get("ZTO") != "1059"
