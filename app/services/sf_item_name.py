"""顺丰商品档案展示名称；仅变换发送值，不修改金蝶名称/规格。"""

import re


_LENS_NAME = "软性亲水接触镜"
# 包含英文型号数字（例如 SofLens 59），但不跨越中文或括号边界。
_ENGLISH_NAME = re.compile(
    r"[A-Za-zＡ-Ｚａ-ｚ][A-Za-zＡ-Ｚａ-ｚ0-9０-９®™]*"
    r"(?:[ \t,，./+&'’:\-]+[A-Za-zＡ-Ｚａ-ｚ0-9０-９®™]+)*"
)
_EMPTY_BRACKETS = re.compile(r"\([\s,，、;；]*\)|（[\s,，、;；]*）|\[[\s,，、;；]*\]")


def build_sf_item_name(brand: str | None, name: str | None, spec: str | None) -> str:
    """仅缩短软性亲水接触镜的英文品名；品牌、中文花色、规格完整保留。

    其他商品沿用原来的空格拼接。不能对拼接结果删英文，否则会误删品牌
    和规格中的 BC/DIA 等参数；也不能按长度截断，否则可能丢失末尾度数。
    """
    brand = (brand or "").strip()
    name = (name or "").strip()
    spec = (spec or "").strip()
    if _LENS_NAME not in name:
        return " ".join(part for part in (brand, name, spec) if part)

    short_name = _ENGLISH_NAME.sub("", name)
    # 重复清理支持英文名称的嵌套括号，保留（济州茶）等中文花色。
    while True:
        cleaned = _EMPTY_BRACKETS.sub("", short_name)
        if cleaned == short_name:
            break
        short_name = cleaned
    short_name = re.sub(r"\s+", " ", short_name).strip(" ,，、;；")
    return f"{brand}{short_name}" + (f"，{spec}" if spec else "")
