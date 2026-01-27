import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from io import BytesIO
from typing import Any, Iterable, Tuple

import pandas as pd

JST = timezone(timedelta(hours=9))

BUILDING_KEYWORDS = [
    "ビル", "ﾋﾞﾙ", "ビルディング", "ﾋﾞﾙﾃﾞｨﾝｸﾞ", "マンション", "ﾏﾝｼｮﾝ", "アパート", "ｱﾊﾟｰﾄ",
    "ハイツ", "ﾊｲﾂ", "コーポ", "ｺｰﾎﾟ", "レジデンス", "ﾚｼﾞﾃﾞﾝｽ", "タワー", "ﾀﾜｰ", "プラザ", "ﾌﾟﾗｻﾞ",
    "会館", "会舘", "センター", "ｾﾝﾀｰ", "オフィス", "ｵﾌｨｽ", "工場", "寮", "荘", "コート", "ｺｰﾄ",
    "ハウス", "ﾊｳｽ", "ホーム", "ﾎｰﾑ", "館", "庁舎", "棟", "ヒルズ", "ﾋﾙｽﾞ", "タウン", "ﾀｳﾝ",
]

DASH_CHARS = r"[‐-‒–—―−－ｰー－-]"
HYPHEN = r"-"

TITLE_KEYWORDS = [
    "代表取締役", "取締役", "執行役員", "監査役", "会長", "社長", "専務", "常務",
    "本部長", "部長", "次長", "課長", "室長", "所長", "支店長", "マネージャー",
    "担当者",
]

OLD_KANJI_MAP = {"髙": "FBFC", "邊": "F6E2", "﨑": "FB99"}
KANJI_REPLACE_MAP = {"髙": "高", "邊": "辺", "﨑": "崎"}

FULL_KATA_MAP = {
    "ガ": "ｶﾞ", "ギ": "ｷﾞ", "グ": "ｸﾞ", "ゲ": "ｹﾞ", "ゴ": "ｺﾞ",
    "ザ": "ｻﾞ", "ジ": "ｼﾞ", "ズ": "ｽﾞ", "ゼ": "ｾﾞ", "ゾ": "ｿﾞ",
    "ダ": "ﾀﾞ", "ヂ": "ﾁﾞ", "ヅ": "ﾂﾞ", "デ": "ﾃﾞ", "ド": "ﾄﾞ",
    "バ": "ﾊﾞ", "ビ": "ﾋﾞ", "ブ": "ﾌﾞ", "ベ": "ﾍﾞ", "ボ": "ﾎﾞ",
    "パ": "ﾊﾟ", "ピ": "ﾋﾟ", "プ": "ﾌﾟ", "ペ": "ﾍﾟ", "ポ": "ﾎﾟ",
    "ヴ": "ｳﾞ", "ワ": "ﾜ", "ヰ": "ｲ", "ヱ": "ｴ", "ヲ": "ｦ",
    "ア": "ｱ", "イ": "ｲ", "ウ": "ｳ", "エ": "ｴ", "オ": "ｵ",
    "カ": "ｶ", "キ": "ｷ", "ク": "ｸ", "ケ": "ｹ", "コ": "ｺ",
    "サ": "ｻ", "シ": "ｼ", "ス": "ｽ", "セ": "ｾ", "ソ": "ｿ",
    "タ": "ﾀ", "チ": "ﾁ", "ツ": "ﾂ", "テ": "ﾃ", "ト": "ﾄ",
    "ナ": "ﾅ", "ニ": "ﾆ", "ヌ": "ﾇ", "ネ": "ﾈ", "ノ": "ﾉ",
    "ハ": "ﾊ", "ヒ": "ﾋﾞ" if False else "ﾋ", "フ": "ﾌ", "ヘ": "ﾍ", "ホ": "ﾎ",
    "マ": "ﾏ", "ミ": "ﾐ", "ム": "ﾑ", "メ": "ﾒ", "モ": "ﾓ",
    "ヤ": "ﾔ", "ユ": "ﾕ", "ヨ": "ﾖ",
    "ラ": "ﾗ", "リ": "ﾘ", "ル": "ﾙ", "レ": "ﾚ", "ロ": "ﾛ",
    "ヮ": "ﾜ", "ン": "ﾝ",
    "ァ": "ｧ", "ィ": "ｨ", "ゥ": "ｩ", "ェ": "ｪ", "ォ": "ｫ",
    "ッ": "ｯ", "ャ": "ｬ", "ュ": "ｭ", "ョ": "ｮ",
    "、": "､", "。": "｡", "ー": "ｰ", "「": "｢", "」": "｣", "・": "･",
}

MULTI_NAME_PATTERNS = [
    r"[、，,・/／＆&＋+]", r"\b(?:と|and|AND|＆)\b",
]
multi_regex = re.compile("|".join(MULTI_NAME_PATTERNS))

ALIAS_CLIENT = ["クライアント名", "得意先名称", "得意先名称1", "得意先名称２", "得意先名称１", "得意先名", "取引先名", "顧客名"]
ALIAS_CONTACT = ["ご担当者名", "担当者", "担当者名", "氏名", "連絡先", "ご担当者"]
ALIAS_POST = ["郵便番号", "郵便", "〒"]
ALIAS_ADDR1 = ["住所１", "住所1", "住所_1", "住所"]
ALIAS_ADDR2 = ["住所２", "住所2", "住所_2"]
ALIAS_ADDR3 = ["住所３", "住所3", "住所_3"]
ALIAS_TEL = ["電話番号", "TEL", "Tel", "電話"]
ALIAS_TANTO_NAME = ["担当者名称", "担当者名（社内）", "担当者名社内"]
ALIAS_BUKA = ["部課名", "部課"]
ALIAS_BUMON = ["部門", "分類２名", "分類2名", "分類３名", "分類3名"]
ALIAS_HQ = ["本社所在地名", "本社所在地", "本社所在地名 "]

DEPT_SUFFIXES = (
    "部", "課", "室", "センター", "本部", "支店", "営業所", "事業部", "事務所", "店",
)

COMPANY_MARKERS = ("株式会社", "有限会社", "合同会社", "（株）", "(株)", "㈱", "（有）", "(有)", "㈲")


@dataclass(frozen=True)
class ProcessConfig:
    addr_split_mode: str = "auto"
    surname_whitelist: set[str] = None
    app_version: str = ""


def load_config_from_env() -> ProcessConfig:
    v = str(os.getenv("APP_VERSION", "") or "").strip()
    addr_mode = str(os.getenv("ADDR_SPLIT_MODE", "auto") or "auto").strip() or "auto"
    wl_raw = str(os.getenv("SURNAME_WHITELIST", "") or "")
    wl = set()
    for x in wl_raw.split(","):
        t = str(x).strip()
        if t:
            wl.add(t)
    return ProcessConfig(addr_split_mode=addr_mode, surname_whitelist=wl, app_version=v)


def _is_na(x: Any) -> bool:
    return x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x)


def safe_text(x: Any) -> str:
    if _is_na(x):
        return ""
    return str(x)


def normalize_space_to_fullwidth(s: Any) -> str:
    t = safe_text(s).strip()
    if not t:
        return ""
    t = re.sub(r"[ \t]+", "　", t)
    t = re.sub(r"　{2,}", "　", t)
    return t


def normalize_katakana_to_half(s: Any) -> str:
    t = safe_text(s)
    if not t:
        return ""
    pattern = "|".join(map(re.escape, FULL_KATA_MAP.keys()))
    def rep(m):
        return FULL_KATA_MAP.get(m.group(0), m.group(0))
    return re.sub(f"({pattern})", rep, t)


def contains_parentheses_any_width(text: Any) -> bool:
    t = safe_text(text)
    return ("(" in t or ")" in t or "（" in t or "）" in t)


def is_tantou_placeholder(text: Any) -> bool:
    t = safe_text(text)
    return ("ご担当者様" in t) or ("ご担当者" in t)


def strip_honorific_suffix(text: Any) -> str:
    t = normalize_space_to_fullwidth(text)
    if not t:
        return ""
    t = re.sub(r"[ 　]*(様|さま|殿)\s*$", "", t)
    return t


def is_multi_name_cell(text: Any) -> bool:
    t = safe_text(text)
    if not t:
        return False
    return bool(multi_regex.search(t))


def check_old_kanji(text: Any) -> str:
    t = safe_text(text)
    if not t:
        return ""
    memos = []
    for old_char, code in OLD_KANJI_MAP.items():
        if old_char in t:
            memos.append(f"{old_char}={code}")
    return " / ".join(memos)


def apply_kanji_conversion(text: Any) -> str:
    t = safe_text(text)
    if not t:
        return ""
    for old, new in KANJI_REPLACE_MAP.items():
        t = t.replace(old, new)
    return t


def split_client_and_dept(client_text: Any) -> tuple[str, str]:
    raw = safe_text(client_text).strip()
    if not raw:
        return ("", "")
    raw = re.sub(r"[ \t　]+", " ", raw).strip()
    parts = raw.split(" ")
    if len(parts) < 2:
        return (raw, "")
    tail = parts[-1].strip()
    if tail and len(tail) <= 24 and tail.endswith(DEPT_SUFFIXES):
        client = " ".join(parts[:-1]).strip()
        if client:
            return (client, tail)
    return (raw, "")


def extract_last_fullname(text: Any, surname_whitelist: set[str]) -> str | None:
    raw = safe_text(text).strip()
    if not raw:
        return None

    raw = strip_honorific_suffix(raw)

    m = re.search(r"([^\s　]+)[\s　]+([^\s　]+)\s*$", raw)
    if not m:
        return None

    last_family = m.group(1).strip(" 　")
    last_given = m.group(2).strip(" 　")

    if not last_family or not last_given:
        return None

    last_given = re.sub(r"(様|さま|殿)$", "", last_given)
    last_family = re.sub(r"(様|さま|殿)$", "", last_family)

    if not last_family or not last_given:
        return None

    if any(mark in last_family or mark in last_given for mark in COMPANY_MARKERS):
        return None

    wl = surname_whitelist or set()

    if any(k in last_family for k in TITLE_KEYWORDS) or any(k in last_given for k in TITLE_KEYWORDS):
        return None

    if last_given not in wl and len(last_given) >= 2 and last_given.endswith(DEPT_SUFFIXES):
        return None

    if last_family not in wl and len(last_family) >= 3 and last_family.endswith(DEPT_SUFFIXES):
        return None

    if len(last_family) > 12 or len(last_given) > 12:
        return None

    extracted_part = m.group(0)
    prefix_raw = raw[:-len(extracted_part)].strip()
    if len(prefix_raw) > 30:
        return None

    return f"{last_family}　{last_given}"


def is_valid_fullname(fullname: Any) -> bool:
    t = safe_text(fullname)
    if not t:
        return False
    parts = t.split("　")
    return len(parts) == 2 and all(p.strip() for p in parts)


def separate_contact_details(raw_contact: Any, extracted_fullname: str | None) -> tuple[str, str, str]:
    raw = normalize_space_to_fullwidth(raw_contact)
    if not extracted_fullname:
        return (raw, "", "")

    full_name = normalize_space_to_fullwidth(extracted_fullname)
    prefix = ""
    if raw.endswith(full_name):
        prefix = raw[:-len(full_name)].strip(" 　")
    elif full_name in raw:
        prefix = raw.split(full_name, 1)[0].strip(" 　")
    else:
        return (raw, "", full_name)

    title = ""
    temp_prefix = prefix
    for kw in sorted(TITLE_KEYWORDS, key=len, reverse=True):
        m = re.search(re.escape(kw) + r"[\s　]*[\.、,]*$", temp_prefix)
        if m:
            title = kw
            temp_prefix = temp_prefix[:m.start()].strip(" 　")

    branch_dept = temp_prefix
    return (branch_dept, title, full_name)


def _norm_col_key(s: Any) -> str:
    t = safe_text(s)
    t = t.replace(" ", "").replace("　", "")
    t = unicodedata.normalize("NFKC", t)
    t = re.sub(r"[‐-‒–—―－-]", "-", t)
    t = t.replace("_", "").replace("（", "(").replace("）", ")")
    t = t.replace("１", "1").replace("２", "2").replace("３", "3")
    t = t.replace("ｺｰﾄﾞ", "コード")
    return t.lower()


def _find_col(cols: Iterable[str], aliases: list[str]) -> str | None:
    keys = {_norm_col_key(c): c for c in cols}
    for a in aliases:
        k = _norm_col_key(a)
        if k in keys:
            return keys[k]
    for c in cols:
        ck = _norm_col_key(c)
        for a in aliases:
            ak = _norm_col_key(a)
            if ak and (ak in ck or ck in ak):
                return c
    return None


def _detect_header_row(excel_bytes: bytes, sheet_name: int | str = 0, max_scan_rows: int = 50) -> int:
    df0 = pd.read_excel(
        BytesIO(excel_bytes),
        sheet_name=sheet_name,
        engine="openpyxl",
        header=None,
        nrows=max_scan_rows,
        dtype=object,
    )
    need_any = [
        ("client", ["得意先名称１", "得意先名称1", "クライアント名", "得意先名称", "得意先名"]),
        ("contact", ["得意先名称２", "得意先名称2", "ご担当者名", "担当者"]),
        ("post", ["郵便番号", "〒"]),
        ("addr1", ["住所１", "住所1"]),
    ]
    for i in range(len(df0)):
        row = df0.iloc[i].tolist()
        row_keys = {_norm_col_key(x) for x in row if not _is_na(x)}
        hit = 0
        for _, aliases in need_any:
            if any(_norm_col_key(a) in row_keys for a in aliases):
                hit += 1
        if hit >= 3:
            return i
    return 0


def _read_input(excel_bytes: bytes, sheet_name: int | str = 0) -> pd.DataFrame:
    header_row = _detect_header_row(excel_bytes, sheet_name=sheet_name)
    df = pd.read_excel(BytesIO(excel_bytes), sheet_name=sheet_name, engine="openpyxl", header=header_row, dtype=object)
    df = df.loc[:, ~df.columns.astype(str).str.contains("^Unnamed", na=False)]
    df = df.dropna(how="all")
    return df


def _build_canonical_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, str]]:
    cols = list(df.columns)

    col_client_direct = _find_col(cols, ["クライアント名"])
    col_tok1 = _find_col(cols, ["得意先名称１", "得意先名称1"])
    col_tok2 = _find_col(cols, ["得意先名称２", "得意先名称2"])
    col_contact_generic = _find_col(cols, ALIAS_CONTACT)
    col_post = _find_col(cols, ALIAS_POST)
    col_addr1 = _find_col(cols, ALIAS_ADDR1)
    col_addr2 = _find_col(cols, ALIAS_ADDR2)
    col_addr3 = _find_col(cols, ALIAS_ADDR3)
    col_tel = _find_col(cols, ALIAS_TEL)
    col_tanto = _find_col(cols, ALIAS_TANTO_NAME)
    col_buka = _find_col(cols, ALIAS_BUKA)
    col_hq = _find_col(cols, ALIAS_HQ)

    col_bumon = None
    for cand in ["部門", "分類２名", "分類2名", "分類３名", "分類3名"]:
        c = _find_col(cols, [cand])
        if c:
            col_bumon = c
            break

    mapping: dict[str, str] = {}

    if col_client_direct:
        df["クライアント名"] = df[col_client_direct]
        mapping["クライアント名"] = col_client_direct
    elif col_tok1:
        df["クライアント名"] = df[col_tok1]
        mapping["クライアント名"] = col_tok1
    else:
        df["クライアント名"] = ""
        mapping["クライアント名"] = ""

    col_contact = col_tok2 if col_tok2 else col_contact_generic
    if col_contact:
        df["ご担当者名"] = df[col_contact]
        mapping["ご担当者名"] = col_contact
    else:
        df["ご担当者名"] = ""
        mapping["ご担当者名"] = ""

    if col_post:
        df["郵便番号"] = df[col_post]
        mapping["郵便番号"] = col_post
    else:
        df["郵便番号"] = ""
        mapping["郵便番号"] = ""

    if col_addr1:
        df["住所１"] = df[col_addr1]
        mapping["住所１"] = col_addr1
    else:
        df["住所１"] = ""
        mapping["住所１"] = ""

    if col_addr2:
        df["住所２"] = df[col_addr2]
        mapping["住所２"] = col_addr2
    else:
        df["住所２"] = ""
        mapping["住所２"] = ""

    if col_addr3:
        df["住所３"] = df[col_addr3]
        mapping["住所３"] = col_addr3
    else:
        df["住所３"] = ""
        mapping["住所３"] = ""

    if col_tel:
        df["電話番号"] = df[col_tel]
        mapping["電話番号"] = col_tel
    else:
        df["電話番号"] = ""
        mapping["電話番号"] = ""

    if col_tanto:
        df["担当者名称"] = df[col_tanto]
        mapping["担当者名称"] = col_tanto
    else:
        df["担当者名称"] = ""
        mapping["担当者名称"] = ""

    if col_buka:
        df["部課名"] = df[col_buka]
        mapping["部課名"] = col_buka
    else:
        df["部課名"] = ""
        mapping["部課名"] = ""

    if col_bumon:
        df["部門"] = df[col_bumon]
        mapping["部門"] = col_bumon
    else:
        df["部門"] = ""
        mapping["部門"] = ""

    if col_hq:
        df["本社所在地名"] = df[col_hq]
        mapping["本社所在地名"] = col_hq
    else:
        df["本社所在地名"] = ""
        mapping["本社所在地名"] = ""

    return df, mapping


def _pick_prefer_hq(group: pd.DataFrame) -> pd.DataFrame:
    if "本社所在地名" in group.columns:
        s = group["本社所在地名"].map(safe_text)
        hq = group[s.str.contains("本社", na=False)]
        if len(hq) > 0:
            return hq.iloc[[0]]
        non_empty = group[s.str.strip() != ""]
        if len(non_empty) > 0:
            return non_empty.iloc[[0]]
    return group.iloc[[0]]


def _norm_key_text(s: Any) -> str:
    t = unicodedata.normalize("NFKC", safe_text(s))
    t = re.sub(r"[\s　]+", "", t).strip()
    t = re.sub(DASH_CHARS, "-", t)
    return t


def _sanitize_addr_text(s: Any) -> str:
    t = safe_text(s)
    if not t:
        return ""
    t = unicodedata.normalize("NFKC", t)
    t = t.replace("\u0000", "")
    t = re.sub(r"[\r\n\t]", "", t)
    t = re.sub(r"[\x00-\x1f\x7f]", "", t)
    t = re.sub(DASH_CHARS, "-", t)
    t = t.replace("‐", "-").replace("-", "-").replace("‒", "-").replace("–", "-").replace("—", "-").replace("―", "-").replace("−", "-").replace("－", "-").replace("ｰ", "-").replace("ー", "-")
    t = t.replace("　", " ").strip()
    t = re.sub(r"[ ]+", " ", t)
    return t


def _compact(s: Any) -> str:
    t = _sanitize_addr_text(s)
    t = re.sub(r"[ \t　]+", "", t)
    return t.strip()


def _contains_blocklot(s: str) -> bool:
    if not s:
        return False
    if re.search(rf"\d{{1,4}}{HYPHEN}\d{{1,4}}", s):
        return True
    if re.search(r"(丁目|番地|番|号)", s):
        return True
    return False


def _is_buildingish(s: str) -> bool:
    if not s:
        return False
    if any(k in s for k in BUILDING_KEYWORDS):
        return True
    if re.search(r"(?:地下?\d{1,3}階|B\d{1,3}F|B\d{1,3}Ｆ)\s*$", s):
        return True
    if re.search(r"(?:\d{1,4}F|\d{1,4}Ｆ|[０-９]{1,4}Ｆ|階)\s*$", s):
        return True
    if re.search(r"(?:\d{1,5}(?:号室|号|室))\s*$", s):
        return True
    if re.search(r"(?:内|構内|団地内|工業団地|国有林|小班内|附属棟|旅客|ターミナル)\s*$", s):
        return True
    if "・" in s or "･" in s:
        return True
    if re.search(r"[A-Za-z]", s):
        return True
    if re.search(r"[ァ-ヶｦ-ﾟ]{3,}", s):
        return True
    if re.search(r"[一-龥]{2,}.*(?:内|構内|団地|工業団地)", s):
        return True
    return False


def _addr_join(a: Any, b: Any) -> str:
    a1 = safe_text(a).strip()
    b1 = safe_text(b).strip()
    if not a1 and not b1:
        return ""
    if a1 and b1:
        return a1 + b1
    return a1 or b1


def _split_last_segment_floor_three_hyphen(s: str) -> Tuple[str, str, bool, bool]:
    m = re.search(rf"(\d{{1,4}}(?:{HYPHEN}\d{{1,4}})+){HYPHEN}(\d{{1,4}}[FＦ階].*)$", s)
    if m:
        return (m.group(1), m.group(2), True, False)

    m = re.match(rf"^(.*?)(\d{{1,4}}){HYPHEN}(\d{{1,4}}){HYPHEN}(\d{{2,4}})([FＦ])(.+)?$", s)
    if not m:
        return ("", "", False, False)

    prefix = m.group(1) or ""
    a = m.group(2)
    b = m.group(3)
    c = m.group(4)
    ff = m.group(5)
    tail = m.group(6) or ""

    best = None
    for cut in range(1, len(c)):
        head = c[:cut]
        floor = c[cut:]
        try:
            head_i = int(head)
            floor_i = int(floor)
        except Exception:
            continue
        if head_i <= 0:
            continue
        if not (1 <= floor_i <= 59):
            continue
        cand = (head, floor)
        if best is None:
            best = cand
        else:
            b_floor = int(best[1])
            if floor_i <= b_floor:
                best = cand

    if best is None:
        return ("", "", False, False)

    head, floor = best
    base = f"{prefix}{a}-{b}-{head}"
    bld = f"{floor}{ff}{tail}"
    return (base, bld, True, True)


def _split_by_blocklot_then_building(t: str) -> Tuple[str, str, bool]:
    if not t:
        return ("", "", False)

    m = re.match(rf"^(.+?\d{{1,4}}(?:{HYPHEN}\d{{1,4}}){{1,3}})(.+)$", t)
    if m:
        base = m.group(1).strip()
        rest = m.group(2).strip()
        return (base, rest, False)

    m = re.match(r"^(.+?\d{1,4}番(?:地)?\d{1,4}号)(.+)$", t)
    if m:
        base = m.group(1).strip()
        rest = m.group(2).strip()
        has_two_go = len(re.findall(r"号", t)) >= 2
        amb = False
        if has_two_go and not any(k in rest for k in BUILDING_KEYWORDS) and not re.search(r"[ァ-ヶｦ-ﾟ]{3,}", rest) and not re.search(r"[A-Za-z]", rest):
            amb = True
        return (base, rest, amb)

    m = re.match(r"^([一二三四五六七八九十百千0-9]+丁目[一二三四五六七八九十百千0-9]+(?:番地|番)?[一二三四五六七八九十百千0-9]+号?)(.+)$", t)
    if m:
        base = m.group(1).strip()
        rest = m.group(2).strip()
        has_two_go = len(re.findall(r"号", t)) >= 2
        amb = False
        if has_two_go and not any(k in rest for k in BUILDING_KEYWORDS) and not re.search(r"[ァ-ヶｦ-ﾟ]{3,}", rest) and not re.search(r"[A-Za-z]", rest):
            amb = True
        return (base, rest, amb)

    return ("", "", False)


def _split_base_building_general(s: str) -> Tuple[str, str, bool]:
    t = _compact(s)
    if not t:
        return ("", "", False)

    base, bld, did, amb_regex = _split_last_segment_floor_three_hyphen(t)
    if did and bld:
        return (base, bld, amb_regex)

    b_base, b_bld, b_did = _split_by_blocklot_then_building(t)
    if b_did and (b_base or b_bld):
        return (b_base, b_bld, False)

    has_blocklot = bool(re.search(r"(丁目|番地|番|号)|\d{1,4}" + HYPHEN + r"\d{1,4}", t))
    has_two_go = len(re.findall(r"号", t)) >= 2
    buildingish = _is_buildingish(t)

    m = re.match(r"^([^\s　]*?\d{1,4}番(?:地)?\d{1,4}号)(.+)$", t)
    if m:
        prefix = m.group(1).strip()
        rest = m.group(2).strip()
        if not rest:
            return (prefix, "", False)
        amb = False
        if has_two_go and not any(k in rest for k in BUILDING_KEYWORDS) and not re.search(r"[ァ-ヶｦ-ﾟ]{3,}", rest) and not re.search(r"[A-Za-z]", rest):
            amb = True
        return (prefix, rest, amb)

    m = re.match(rf"^(.+?\d{{1,4}}{HYPHEN}\d{{1,4}}(?:{HYPHEN}\d{{1,4}})?)(.+)$", t)
    if m:
        prefix = m.group(1).strip()
        rest = m.group(2).strip()
        if not rest:
            return (prefix, "", False)
        amb = False
        if has_two_go and not any(k in rest for k in BUILDING_KEYWORDS) and not re.search(r"[ァ-ヶｦ-ﾟ]{3,}", rest) and not re.search(r"[A-Za-z]", rest):
            amb = True
        return (prefix, rest, amb)

    m = re.match(r"^([一二三四五六七八九十百千0-9]+丁目[一二三四五六七八九十百千0-9]+(?:番地|番)?[一二三四五六七八九十百千0-9]+号?)(.+)$", t)
    if m:
        prefix = m.group(1).strip()
        rest = m.group(2).strip()
        if not rest:
            return (prefix, "", False)
        amb = False
        if has_two_go and not any(k in rest for k in BUILDING_KEYWORDS) and not re.search(r"[ァ-ヶｦ-ﾟ]{3,}", rest) and not re.search(r"[A-Za-z]", rest):
            amb = True
        return (prefix, rest, amb)

    if buildingish and not has_blocklot:
        return ("", t, False)

    if has_two_go and has_blocklot and not any(k in t for k in BUILDING_KEYWORDS) and not re.search(r"[ァ-ヶｦ-ﾟ]{3,}", t) and not re.search(r"[A-Za-z]", t):
        return ("", t, True)

    return (t, "", False)


def _finalize_for_export(df_in: pd.DataFrame) -> pd.DataFrame:
    df = df_in.copy()
    base = df.get("住所１（番地まで）", "").map(safe_text)
    bld = df.get("住所２（建物名）", "").map(safe_text)
    full = (base.str.strip() + " " + bld.str.strip()).str.replace(r"\s{2,}", " ", regex=True).str.strip()
    df["住所（全文）"] = full
    df["住所１"] = base
    df["住所２"] = bld
    df["住所３"] = ""
    return df


def process_excel_bytes(input_bytes: bytes, input_filename: str, config: ProcessConfig) -> tuple[bytes, dict[str, Any]]:
    df_raw = _read_input(input_bytes, sheet_name=0)
    df, mapping = _build_canonical_columns(df_raw)

    for c in ["ご担当者名", "住所１", "住所２", "住所３", "クライアント名"]:
        df[c] = df[c].map(apply_kanji_conversion)

    obj_cols = [c for c in df.columns if df[c].dtype == object]
    for c in obj_cols:
        df[c] = df[c].map(normalize_katakana_to_half)

    df["旧字メモ（氏名）"] = df["ご担当者名"].map(check_old_kanji)
    df["旧字メモ（住所）"] = (
        df["住所１"].map(check_old_kanji) + " / " + df["住所２"].map(check_old_kanji) + " / " + df["住所３"].map(check_old_kanji)
    )
    df["旧字メモ（住所）"] = df["旧字メモ（住所）"].str.replace(r"(\s/\s){2,}", " / ", regex=True).str.strip(" /")

    client_split = df["クライアント名"].map(split_client_and_dept)
    df["_client_clean"] = client_split.map(lambda t: safe_text(t[0]).strip())
    df["_client_dept"] = client_split.map(lambda t: safe_text(t[1]).strip())

    df["_client_raw"] = df["_client_clean"].map(safe_text)
    df["_client_norm"] = df["_client_raw"].map(lambda x: _norm_key_text(x))
    df["_client_has_paren"] = df["_client_raw"].map(contains_parentheses_any_width)

    df["_contact_raw"] = df["ご担当者名"].map(strip_honorific_suffix)
    wl = config.surname_whitelist or set()
    df["抽出氏名"] = df["_contact_raw"].map(lambda x: extract_last_fullname(x, wl))

    contact_details = df.apply(
        lambda r: separate_contact_details(r["_contact_raw"], r["抽出氏名"]),
        axis=1,
        result_type="expand",
    )
    contact_details.columns = ["_contact_branch_dept", "_title", "抽出氏名（クリーン）"]
    df = pd.concat([df.reset_index(drop=True), contact_details.reset_index(drop=True)], axis=1)

    df["肩書"] = df["_title"].map(safe_text)

    def _dept_merge(row: pd.Series) -> str:
        a = safe_text(row.get("_client_dept", "")).strip()
        if a:
            return a
        b = safe_text(row.get("_contact_branch_dept", "")).strip()
        if b and len(b) <= 24 and b.endswith(DEPT_SUFFIXES):
            return b
        return ""

    df["部署"] = df.apply(_dept_merge, axis=1)

    def _addr_row_split(row: pd.Series) -> Tuple[str, str, bool]:
        a1 = _compact(row.get("住所１", ""))
        a2 = _compact(row.get("住所２", ""))
        a3 = _compact(row.get("住所３", ""))

        amb = False

        if a3:
            base0 = _addr_join(a1, a2)
            b3_base, b3_bld, b3_amb = _split_base_building_general(a3)
            amb = amb or b3_amb
            base = _compact(base0 + (b3_base or ""))
            bld = _compact(b3_bld or "")
            if not bld and not b3_base and _is_buildingish(a3):
                bld = _compact(a3)
            return (base, bld, amb)

        if a2 and _is_buildingish(a2) and not _contains_blocklot(a2):
            base = _compact(a1)
            bld = _compact(a2)
            return (base, bld, False)

        combined = _addr_join(a1, a2)
        b_base, b_bld, b_amb = _split_base_building_general(combined)
        amb = amb or b_amb

        if b_bld:
            return (_compact(b_base), _compact(b_bld), amb)

        return (_compact(combined), "", amb)

    addr_split = df.apply(_addr_row_split, axis=1, result_type="expand")
    addr_split.columns = ["住所１（番地まで）", "住所２（建物名）", "_addr_ambiguous"]
    df = pd.concat([df.reset_index(drop=True), addr_split.reset_index(drop=True)], axis=1)

    df["_dedupe_addr"] = (df["住所１（番地まで）"].map(_norm_key_text) + "|" + df["住所２（建物名）"].map(_norm_key_text))
    df["_dedupe_client"] = df["_client_norm"].map(_norm_key_text)
    df["_dedupe_tanto"] = df["担当者名称"].map(_norm_key_text)

    df = (
        df.sort_values(["_dedupe_client", "_dedupe_tanto", "_dedupe_addr", "_contact_raw"])
        .groupby(["_dedupe_client", "_dedupe_tanto", "_dedupe_addr"], as_index=False, group_keys=False)
        .apply(_pick_prefer_hq)
        .reset_index(drop=True)
    )

    df_addr_amb = df[df["_addr_ambiguous"]].copy()
    if len(df_addr_amb) > 0:
        df_addr_amb["理由"] = "住所の分割が曖昧（番地と部屋番/建物判定が区別困難）"
    df = df[~df["_addr_ambiguous"]].copy()

    mask_tantou_placeholder = df["_contact_raw"].map(is_tantou_placeholder)
    df_tantou = df[mask_tantou_placeholder].copy()
    if len(df_tantou) > 0:
        df_tantou["理由"] = "氏名が『ご担当者／ご担当者様』表記"
    df_rest = df[~mask_tantou_placeholder].copy()

    valid_name_mask = df_rest["抽出氏名"].map(is_valid_fullname)
    is_multi_mask = df_rest["_contact_raw"].map(is_multi_name_cell)

    df_valid = df_rest[valid_name_mask & ~is_multi_mask].copy()
    df_multi_name_all = df_rest[is_multi_mask].copy()
    df_extraction_failure = df_rest[~valid_name_mask & ~is_multi_mask].copy()

    if len(df_extraction_failure) > 0:
        df_extraction_failure["理由"] = "氏名抽出不可（2語形式でない/組織名誤認/役職誤認/前置き過長/会社名混入/部署混入）"

    if len(df_multi_name_all) > 0:
        df_multi_name_all["理由"] = "複数名記号あり"
        df_multi_name = (
            df_multi_name_all.sort_values(["_dedupe_client", "_dedupe_addr", "_contact_raw"])
            .groupby("_dedupe_client", as_index=False, group_keys=False)
            .apply(_pick_prefer_hq)
            .reset_index(drop=True)
        )
    else:
        df_multi_name = df_multi_name_all

    client_nuniques = (
        df_valid.groupby("抽出氏名")["_dedupe_client"]
        .nunique(dropna=False)
        .reset_index(name="client_distinct")
    )
    multi_client_names = set(client_nuniques.loc[client_nuniques["client_distinct"] > 1, "抽出氏名"])

    mask_name_conflict = df_valid["抽出氏名"].isin(multi_client_names)
    mask_paren_conflict = df_valid["_client_has_paren"]

    df_name_conflict = df_valid[mask_name_conflict].copy()
    df_paren_conflict = df_valid[mask_paren_conflict].copy()
    df_main_base = df_valid[~(mask_name_conflict | mask_paren_conflict)].copy()

    if len(df_name_conflict) > 0:
        df_name_conflict["理由"] = "氏名同一でクライアント相違"
    if len(df_paren_conflict) > 0:
        df_paren_conflict["理由"] = "クライアント名に括弧あり"

    selected_by_name = (
        df_main_base.sort_values(["抽出氏名", "_contact_raw"])
        .groupby("抽出氏名", as_index=False, group_keys=False)
        .apply(_pick_prefer_hq)
    )

    selected_by_client = (
        selected_by_name.sort_values(["_dedupe_client", "_contact_raw", "抽出氏名"])
        .groupby("_dedupe_client", as_index=False, group_keys=False)
        .apply(_pick_prefer_hq)
    )

    is_hr = selected_by_client["部門"].map(safe_text).str.contains(r"(人材|派遣)", na=False, regex=True)
    df_hr = selected_by_client[is_hr].copy()
    if len(df_hr) > 0:
        df_hr["理由"] = "除外（人材派遣関連部門）"
    
    selected_by_client = selected_by_client[~is_hr].copy()

    is_agency = selected_by_client["部門"].map(safe_text).str.contains(r"代理店", na=False)
    df_agency = selected_by_client[is_agency].copy()
    if len(df_agency) > 0:
        df_agency["理由"] = "要確認（代理店部/許可リスト判定待ち）"

    selected_by_client = selected_by_client[~is_agency].copy()

    def _split_name(x: Any) -> tuple[str, str]:
        t = safe_text(x)
        if "　" in t:
            p = t.split("　", 1)
            return (p[0].strip(), p[1].strip())
        return ("", "")

    name_pairs = selected_by_client["抽出氏名（クリーン）"].map(_split_name)
    selected_by_client["姓"] = name_pairs.map(lambda p: p[0])
    selected_by_client["名"] = name_pairs.map(lambda p: p[1])
    selected_by_client["送付氏名（姓　名）"] = selected_by_client["抽出氏名（クリーン）"].map(safe_text)

    df_main = _finalize_for_export(selected_by_client)
    df_name_conflict_out = _finalize_for_export(df_name_conflict)
    df_paren_conflict_out = _finalize_for_export(df_paren_conflict)
    df_tantou_out = _finalize_for_export(df_tantou)
    df_extraction_failure_out = _finalize_for_export(df_extraction_failure)
    df_multi_name_out = _finalize_for_export(df_multi_name)
    df_addr_amb_out = _finalize_for_export(df_addr_amb)
    df_hr_out = _finalize_for_export(df_hr)
    df_agency_out = _finalize_for_export(df_agency)

    export_cols_main = [
        "クライアント名",
        "部署",
        "肩書",
        "送付氏名（姓　名）", "姓", "名",
        "郵便番号",
        "住所１", "住所２", "住所３",
        "住所（全文）",
        "電話番号",
        "旧字メモ（氏名）",
        "旧字メモ（住所）",
    ]
    export_cols_check = ["理由"] + export_cols_main

    def _ensure_export_base(d: pd.DataFrame) -> pd.DataFrame:
        x = d.copy()
        x["クライアント名"] = x.get("_client_clean", x.get("クライアント名", "")).map(safe_text)
        if "部署" not in x.columns:
            x["部署"] = ""
        if "肩書" not in x.columns:
            x["肩書"] = ""
        if "送付氏名（姓　名）" not in x.columns:
            x["送付氏名（姓　名）"] = x.get("抽出氏名（クリーン）", x.get("抽出氏名", "")).map(safe_text)
        for c in ["姓", "名", "郵便番号", "電話番号", "旧字メモ（氏名）", "旧字メモ（住所）"]:
            if c not in x.columns:
                x[c] = ""
        for c in ["住所１", "住所２", "住所３", "住所（全文）"]:
            if c not in x.columns:
                x[c] = ""
        return x

    df_main = _ensure_export_base(df_main)[export_cols_main]

    df_name_conflict_out = _ensure_export_base(df_name_conflict_out)
    if "理由" not in df_name_conflict_out.columns:
        df_name_conflict_out["理由"] = ""
    df_name_conflict_out = df_name_conflict_out[export_cols_check]

    df_paren_conflict_out = _ensure_export_base(df_paren_conflict_out)
    if "理由" not in df_paren_conflict_out.columns:
        df_paren_conflict_out["理由"] = ""
    df_paren_conflict_out = df_paren_conflict_out[export_cols_check]

    df_tantou_out = _ensure_export_base(df_tantou_out)
    if "理由" not in df_tantou_out.columns:
        df_tantou_out["理由"] = ""
    df_tantou_out = df_tantou_out[export_cols_check]

    df_extraction_failure_out = _ensure_export_base(df_extraction_failure_out)
    if "理由" not in df_extraction_failure_out.columns:
        df_extraction_failure_out["理由"] = ""
    df_extraction_failure_out = df_extraction_failure_out[export_cols_check]

    df_multi_name_out = _ensure_export_base(df_multi_name_out)
    if "理由" not in df_multi_name_out.columns:
        df_multi_name_out["理由"] = ""
    df_multi_name_out = df_multi_name_out[export_cols_check]

    df_addr_amb_out = _ensure_export_base(df_addr_amb_out)
    if "理由" not in df_addr_amb_out.columns:
        df_addr_amb_out["理由"] = ""
    df_addr_amb_out = df_addr_amb_out[export_cols_check]

    df_hr_out = _ensure_export_base(df_hr_out)
    if "理由" not in df_hr_out.columns:
        df_hr_out["理由"] = ""
    df_hr_out = df_hr_out[export_cols_check]

    df_agency_out = _ensure_export_base(df_agency_out)
    if "理由" not in df_agency_out.columns:
        df_agency_out["理由"] = ""
    df_agency_out = df_agency_out[export_cols_check]

    out_buf = BytesIO()
    with pd.ExcelWriter(out_buf, engine="openpyxl") as writer:
        df_main.to_excel(writer, sheet_name="送付対象", index=False)
        df_agency_out.to_excel(writer, sheet_name="要確認（代理店部）", index=False)
        df_name_conflict_out.to_excel(writer, sheet_name="要確認（氏名重複）", index=False)
        df_paren_conflict_out.to_excel(writer, sheet_name="要確認（クライアント名括弧）", index=False)
        df_tantou_out.to_excel(writer, sheet_name="要確認（ご担当者表記）", index=False)
        df_addr_amb_out.to_excel(writer, sheet_name="要確認（住所分割曖昧）", index=False)
        df_hr_out.to_excel(writer, sheet_name="除外（人材派遣）", index=False)
        df_extraction_failure_out.to_excel(writer, sheet_name="除外（氏名抽出失敗）", index=False)
        df_multi_name_out.to_excel(writer, sheet_name="除外（複数名）", index=False)

    summary = {
        "app_version": config.app_version,
        "input_filename": input_filename,
        "rows_input": int(len(df_raw)),
        "rows_after_dedupe": int(len(df) + len(df_addr_amb)),
        "header_mapping": mapping,
        "counts": {
            "送付対象": int(len(df_main)),
            "要確認（代理店部）": int(len(df_agency_out)),
            "要確認（氏名重複）": int(len(df_name_conflict_out)),
            "要確認（クライアント名括弧）": int(len(df_paren_conflict_out)),
            "要確認（ご担当者表記）": int(len(df_tantou_out)),
            "要確認（住所分割曖昧）": int(len(df_addr_amb_out)),
            "除外（人材派遣）": int(len(df_hr_out)),
            "除外（氏名抽出失敗）": int(len(df_extraction_failure_out)),
            "除外（複数名）": int(len(df_multi_name_out)),
        },
        "generated_at": datetime.now(JST).isoformat(timespec="seconds"),
    }

    return out_buf.getvalue(), summary