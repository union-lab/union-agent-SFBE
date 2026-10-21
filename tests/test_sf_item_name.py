import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager
from copy import deepcopy

import pytest

from app.services import sf_automation, sf_item_push
from app.services.sf_item_name import build_sf_item_name


@pytest.mark.parametrize("name", [
    "软性亲水接触镜Soft (Hydrophilic) Contact Lenskalifilcon A SVS",
    "软性亲水接触镜SofLens 59 (hilafilcon B) Visibility Tinted Contact Lenses",
    "软性亲水接触镜SofLens 59 (hilafilcon ,B) Visibility Tinted Contact Lenses",
    "软性亲水接触镜SofLens Daily Disposable (hilafilcon B) Visibility Tinted Contact Lenses  ",
    "软性亲水接触镜（Soft (Hydrophilic) Contact Lens）",
    "软性亲水接触镜Ｓｏｆｔ Ｌｅｎｓ",
    "软性亲水接触镜",
])
def test_bilingual_lens_name_removes_english_and_model_digits(name):
    spec = "日抛型，2片装，后顶焦度-6.00"
    assert build_sf_item_name("博士伦", name, spec) == f"博士伦软性亲水接触镜，{spec}"


@pytest.mark.parametrize("color", ["瓷泡泡", "济州茶"])
def test_preserves_brand_and_chinese_color(color):
    assert build_sf_item_name(
        "HanGee韩姬儿", f"软性亲水接触镜（{color}） Realcon Color", "日抛，10片，-8.00"
    ) == f"HanGee韩姬儿软性亲水接触镜（{color}），日抛，10片，-8.00"


@pytest.mark.parametrize("spec", [
    "后顶焦度-6.00", "后顶焦度+2.00", "0度，10片装",
    "BC8.6，DIA14.2，CYL-1.25，AX180，2片/盒",
    "38彩日，日抛，三彩（茶歇棕，125度，10片装）",
    "Soft SVS，-6.00，BC<9 & DIA>14",
])
def test_never_changes_specification(spec):
    assert build_sf_item_name("博士伦", "软性亲水接触镜 Soft Lens", spec).endswith("，" + spec)


@pytest.mark.parametrize("brand,name,spec,expected", [
    (" 稳健 ", " 医用纱布片 Soft ", " 10cmX10cm 50片/盒 ", "稳健 医用纱布片 Soft 10cmX10cm 50片/盒"),
    ("", "隐形眼镜护理液", "适用于软性亲水接触镜 BC8.6", "隐形眼镜护理液 适用于软性亲水接触镜 BC8.6"),
    (None, None, None, ""),
    ("品牌", "", "规格", "品牌 规格"),
    ("", "软性亲水接触镜 Soft", "", "软性亲水接触镜"),
])
def test_unaffected_products_and_missing_fields(brand, name, spec, expected):
    assert build_sf_item_name(brand, name, spec) == expected


def run_mocked(coro):
    try:
        coro.send(None)
    except StopIteration as exc:
        return exc.value
    finally:
        coro.close()
    raise AssertionError("Unexpected real async I/O")


@pytest.mark.parametrize("name,spec", [
    ("软性亲水接触镜Soft (Hydrophilic) Contact Lenskalifilcon A SVS", "日抛型，2片装，后顶焦度-6.00"),
    ("软性亲水接触镜（济州茶） Realcon Color", "BC<9 & DIA>14，-6.00"),
    ("医用纱布片 Soft", "10cmX10cm 50片/盒"),
])
def test_both_item_service_paths_use_same_name_and_keep_source(monkeypatch, name, spec):
    source = dict(sku="TEST_LENS", name=name, spec=spec, brand="博士伦", unit="盒", barcode="694239318728")
    original = deepcopy(source)
    manual = ET.fromstring(sf_item_push._build_item_xml(source))
    captured = []
    writes = []

    class Conn:
        async def execute(self, sql, *args):
            writes.append((sql, args))

    class Pool:
        @asynccontextmanager
        async def acquire(self):
            yield Conn()

    async def get_pool():
        return Pool()

    async def send(service, body):
        assert service == "ITEM_SERVICE"
        captured.append(ET.fromstring(body).find(".//Item"))
        return True, "", "OK"

    monkeypatch.setattr(sf_automation, "get_pool", get_pool)
    monkeypatch.setattr(sf_automation, "_sf_send", send)
    assert run_mocked(sf_automation._sf_sync_item(**source)) is True
    assert source == original
    assert len(captured) == 1
    auto = captured[0]
    assert auto is not None
    for tag in ("ItemName", "ItemSpecifications", "SkuNo", "ItemUom", "BarCode1"):
        assert auto.findtext(tag) == manual.findtext(tag)
    assert manual.findtext("ItemName") == build_sf_item_name(source["brand"], name, spec)
    assert manual.findtext("ItemSpecifications") == spec
    assert manual.findtext("SkuNo") == source["sku"]
    assert manual.findtext("BarCode1") == source["barcode"]
    # 日志仍保留原始名称/规格，清理只作用于发送给顺丰的展示名称。
    assert len(writes) == 2
    assert writes[1][1][0:3] == (source["sku"], name, spec)
