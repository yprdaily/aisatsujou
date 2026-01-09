import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from io import BytesIO
from typing import Any, Iterable

import pandas as pd

JST = timezone(timedelta(hours=9))

BUILDING_KEYWORDS = [
    "ビル", "ビルディング", "マンション", "アパート", "ハイツ", "コーポ",
    "レジデンス", "タワー", "プラザ", "会館", "会舘", "センター", "オフィス",
    "工場", "寮", "荘", "コート", "ハウス", "ホーム", "館", "庁舎", "棟"
]
HYPHEN = r"[－-]"

TITLE_KEYWORDS = [
    "代表取締役", "取締役", "執行役員", "監査役", "会長", "社長", "専務", "常務",
    "本部長", "部長", "次長", "課長", "室長", "所長", "支店長", "マネージャー",
    "担当者", "様"
]

BANNED_IN_SURNAME = ["部", "課", "室", "所", "支", "営", "監", "会", "長", "役", "員"]

OLD_KANJI_MAP = {"髙": "FBFC", "邊": "F6E2", "﨑": "FB99"}
KANJI_REPLACE_MAP = {"髙": "高", "邊": "辺", "﨑": "崎"}

HALF_KATA_REGEX = re.compile(r"[\uFF61-\uFF9F]")

FULL_KATA_MAP = {
    'ガ': 'ｶﾞ', 'ギ': 'ｷﾞ', 'グ': 'ｸﾞ', 'ゲ': 'ｹﾞ', 'ゴ': 'ｺﾞ',
    'ザ': 'ｻﾞ', 'ジ': 'ｼﾞ', 'ズ': 'ｽﾞ', 'ゼ': 'ｾﾞ', 'ゾ': 'ｿﾞ',
    'ダ': 'ﾀﾞ', 'ヂ': 'ﾁﾞ', 'ヅ': 'ﾂﾞ', 'デ': 'ﾃﾞ', 'ド': 'ﾄﾞ',
    'バ': 'ﾊﾞ', 'ビ': 'ﾋﾞ', 'ブ': 'ﾌﾞ', 'ベ': 'ﾍﾞ', 'ボ': 'ﾎﾞ',
    'パ': 'ﾊﾟ', 'ピ': 'ﾋﾟ', 'プ': 'ﾌﾟ', 'ペ': 'ﾍﾟ', 'ポ': 'ﾎﾟ',
    'ヴ': 'ｳﾞ', 'ワ': 'ﾜ', 'ヰ': 'ｲ', 'ヱ': 'ｴ', 'ヲ': 'ｦ',
    'ア': 'ｱ', 'イ': 'ｲ', 'ウ': 'ｳ', 'エ': 'ｴ', 'オ': 'ｵ',
    'カ': 'ｶ', 'キ': 'ｷ', 'ク': 'ｸ', 'ケ': 'ｹ', 'コ': 'ｺ',
    'サ': 'ｻ', 'シ': 'ｼ', 'ス': 'ｽ', 'セ': 'ｾ', 'ソ': 'ｿ',
    'タ': 'ﾀ', 'チ': 'ﾁ', 'ツ': 'ﾂ', 'テ': 'ﾃ', 'ト': 'ﾄ',
    'ナ': 'ﾅ', 'ニ': 'ﾆ', 'ヌ': 'ﾇ', 'ネ': 'ﾈ', 'ノ': 'ﾉ',
    'ハ': 'ﾊ', 'ヒ': 'ﾋ', 'フ': 'ﾌ', 'ヘ': 'ﾍ', 'ホ': 'ﾎ',
    'マ': 'ﾏ', 'ミ': 'ﾐ', 'ム': 'ﾑ', 'メ': 'ﾒ', 'モ': 'ﾓ',
    'ヤ': 'ﾔ', 'ユ': 'ﾕ', 'ヨ': 'ﾖ',
    'ラ': 'ﾗ', 'リ': 'ﾘ', 'ル': 'ﾙ', 'レ': 'ﾚ', 'ロ': 'ﾛ',
    'ヮ': 'ﾜ', 'ン': 'ﾝ',
    'ァ': 'ｧ', 'ィ': 'ｨ', 'ゥ': 'ｩ', 'ェ': 'ｪ', 'ォ': 'ｫ',
    'ッ': 'ｯ', 'ャ': 'ｬ', 'ュ': 'ｭ', 'ョ': 'ｮ',
    '、': '､', '。': '｡', 'ー': 'ｰ', '「': '｢', '」': '｣', '・': '･',
}

MULTI_NAME_PATTERNS = [
    r"[、，,・/／＆&＋+]", r"\b(?:と|and|AND|＆)\b"
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


def normalize_half_katakana_only(s: Any) -> str:
    t = safe_text(s)
    if not t:
        return ""
    if HALF_KATA_REGEX.search(t):
        return unicodedata.normalize("NFKC", t)
    return t


def normalize_full_katakana_to_half(s: Any) -> str:
    t = unicodedata.normalize("NFKC", safe_text(s))
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


def extract_last_fullname(text: Any, surname_whitelist: set[str]) -> str | None:
    raw = safe_text(text).strip()
    if not raw:
        return None

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

    if len(last_family) > 5:
        return None

    wl = surname_whitelist or set()

    if any(b in last_family for b in BANNED_IN_SURNAME) and last_family not in wl:
        return None

    if any(k in last_family for k in TITLE_KEYWORDS):
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
        m = re.search(re.escape(kw) + r'[\s　]*[\.、,]*$', temp_prefix)
        if m:
            title = kw
            temp_prefix = temp_prefix[:m.start()].strip(" 　")
            if kw == "様":
                break

    branch_dept = temp_prefix
    return (branch_dept, title, full_name)


def join_address(addr1: Any, addr2: Any, addr3: Any) -> str:
    parts = []
    for x in (addr1, addr2, addr3):
        t = safe_text(x).strip()
        if t:
            parts.append(t)
    s = " ".join(parts)
    s = re.sub(r"[\s\r\n\t　]+", " ", s).strip()
    return s


def split_building(full_addr: Any) -> tuple[str, str]:
    s = safe_text(full_addr).strip()
    if not s:
        return ("", "")
    s_no_space = re.sub(r"[\s　]+", "", s)

    kw_pattern = "|".join(map(re.escape, BUILDING_KEYWORDS))
    kw_regex = re.compile(f"(?:{kw_pattern})", re.IGNORECASE)

    m1 = kw_regex.search(s_no_space)
    if m1:
        idx = m1.start()
        base = s_no_space[:idx]
        building = s_no_space[idx:]
        return (base, building)

    m2 = re.search(rf"(.+?\d{{1,4}}{HYPHEN}\d{{1,4}}(?:{HYPHEN}\d{{1,4}})?)(.+)", s_no_space)
    if m2:
        return (m2.group(1).strip(), m2.group(2).strip())

    m3 = re.search(r"(.*?丁目\d*(?:番地|番|号)?\d*)(.+)", s_no_space)
    if m3:
        return (m3.group(1).strip(), m3.group(2).strip())

    return (s_no_space, "")


def _norm_col_key(s: Any) -> str:
    t = safe_text(s)
    t = t.replace(" ", "").replace("　", "")
    t = unicodedata.normalize("NFKC", t)
    t = re.sub(r"[‐-‒–—―－-]", "-", t)
    t = t.replace("_", "").replace("（", "(").replace("）", ")")
    t = t.replace("１", "1").replace("２", "2").replace("３", "3")
    t = t.replace("ｺｰﾄﾞ", "コード").replace("ｺｰﾄﾞ", "コード")
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


def _detect_header_row(excel_bytes: bytes, sheet_name: int | str = 0, max_scan_rows: int = 30) -> int:
    df0 = pd.read_excel(BytesIO(excel_bytes), sheet_name=sheet_name, engine="openpyxl", header=None, nrows=max_scan_rows, dtype=object)
    need_any = [
        ("client", ["得意先名称１", "得意先名称1", "クライアント名", "得意先名称", "得意先名"]),
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
        if hit >= 2:
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
    col_contact = _find_col(cols, ALIAS_CONTACT)
    col_post = _find_col(cols, ALIAS_POST)
    col_addr1 = _find_col(cols, ALIAS_ADDR1)
    col_addr2 = _find_col(cols, ALIAS_ADDR2)
    col_addr3 = _find_col(cols, ALIAS_ADDR3)
    col_tel = _find_col(cols, ALIAS_TEL)
    col_tanto = _find_col(cols, ALIAS_TANTO_NAME)
    col_buka = _find_col(cols, ALIAS_BUKA)

    col_bumon = None
    for cand in ["部門", "分類２名", "分類2名", "分類３名", "分類3名"]:
        c = _find_col(cols, [cand])
        if c:
            col_bumon = c
            break

    mapping = {}

    if col_client_direct:
        df["クライアント名"] = df[col_client_direct]
        mapping["クライアント名"] = col_client_direct
    else:
        if col_tok1 or col_tok2:
            a = df[col_tok1] if col_tok1 else ""
            b = df[col_tok2] if col_tok2 else ""
            df["クライアント名"] = (
                a.map(safe_text).str.strip() + " " + b.map(safe_text).str.strip()
            ).str.replace(r"^\s+|\s+$", "", regex=True).str.replace(r"\s{2,}", " ", regex=True)
            mapping["クライアント名"] = "+".join([x for x in [col_tok1, col_tok2] if x])
        else:
            df["クライアント名"] = ""
            mapping["クライアント名"] = ""

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

    return df, mapping


def _pick_by_hq_in_group(group: pd.DataFrame, text_col: str) -> pd.DataFrame:
    def contains_hq(x: Any) -> bool:
        s = safe_text(x)
        return "本社" in s
    hq_rows = group[group[text_col].apply(contains_hq)]
    if len(hq_rows) > 0:
        return hq_rows.iloc[[0]]
    return group.iloc[[0]]


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

    df["旧字メモ（氏名）"] = df["ご担当者名"].map(check_old_kanji)
    df["旧字メモ（住所）"] = (
        df["住所１"].map(check_old_kanji) + " / " + df["住所２"].map(check_old_kanji) + " / " + df["住所３"].map(check_old_kanji)
    )
    df["旧字メモ（住所）"] = df["旧字メモ（住所）"].str.replace(r'(\s/\s){2,}', ' / ', regex=True).str.strip(' /')

    for c in ["ご担当者名", "住所１", "住所２", "住所３"]:
        df[c] = df[c].map(apply_kanji_conversion)

    obj_cols = [c for c in df.columns if df[c].dtype == object]
    for c in obj_cols:
        if c != "クライアント名":
            df[c] = df[c].map(normalize_half_katakana_only)

    df["_client_raw"] = df["クライアント名"].map(safe_text)
    df["_client_norm"] = df["_client_raw"].map(normalize_full_katakana_to_half)
    df["_client_has_paren"] = df["_client_raw"].map(contains_parentheses_any_width)

    df["_contact_raw"] = df["ご担当者名"].map(safe_text)
    wl = config.surname_whitelist or set()
    df["抽出氏名"] = df["_contact_raw"].map(lambda x: extract_last_fullname(x, wl))

    contact_details = df.apply(lambda r: separate_contact_details(r["_contact_raw"], r["抽出氏名"]), axis=1, result_type="expand")
    contact_details.columns = ["_contact_branch_dept", "_title", "抽出氏名（クリーン）"]
    df = pd.concat([df.reset_index(drop=True), contact_details.reset_index(drop=True)], axis=1)

    df["肩書"] = df["_title"].map(safe_text)
    df["支店名"] = df["_contact_branch_dept"].map(safe_text)

    df["住所連結（全文）"] = df.apply(lambda r: join_address(r["住所１"], r["住所２"], r["住所３"]), axis=1)
    base_and_building = df["住所連結（全文）"].map(split_building)
    df["住所１（番地まで）"] = base_and_building.map(lambda t: t[0])
    df["住所２（建物名）"] = base_and_building.map(lambda t: t[1])

    mask_tantou_placeholder = df["_contact_raw"].map(is_tantou_placeholder)
    df_tantou = df[mask_tantou_placeholder].copy()
    df_tantou["理由"] = "氏名が『ご担当者／ご担当者様』表記"
    df_rest = df[~mask_tantou_placeholder].copy()

    valid_name_mask = df_rest["抽出氏名"].map(is_valid_fullname)
    is_multi_mask = df_rest["_contact_raw"].map(is_multi_name_cell)

    df_valid = df_rest[valid_name_mask & ~is_multi_mask].copy()
    df_multi_name = df_rest[is_multi_mask].copy()
    df_extraction_failure = df_rest[~valid_name_mask & ~is_multi_mask].copy()

    if len(df_multi_name) > 0:
        df_multi_name["理由"] = "複数名記号あり"
    if len(df_extraction_failure) > 0:
        df_extraction_failure["理由"] = "氏名抽出不可（2語形式でない/組織名誤認/役職誤認/前置き過長/姓長すぎ）"

    client_nuniques = (
        df_valid.groupby("抽出氏名")["_client_norm"]
        .nunique(dropna=False)
        .reset_index(name="client_distinct")
    )
    multi_client_names = set(client_nuniques.loc[client_nuniques["client_distinct"] > 1, "抽出氏名"])

    mask_name_conflict = df_valid["抽出氏名"].isin(multi_client_names)
    mask_paren_conflict = df_valid["_client_has_paren"]

    df_name_conflict = df_valid[mask_name_conflict].copy()
    df_paren_conflict = df_valid[mask_paren_conflict].copy()
    df_main_base = df_valid[~(mask_name_conflict | mask_paren_conflict)].copy()

    df_name_conflict["理由"] = "氏名同一でクライアント相違"
    df_paren_conflict["理由"] = "クライアント名に括弧あり"

    selected_by_name = (
        df_main_base.sort_values(["抽出氏名", "_contact_raw"])
        .groupby("抽出氏名", as_index=False, group_keys=False)
        .apply(lambda g: _pick_by_hq_in_group(g, "クライアント名"))
    )

    selected_by_client = (
        selected_by_name.sort_values(["_client_norm", "_contact_raw", "抽出氏名"])
        .groupby("_client_norm", as_index=False, group_keys=False)
        .apply(lambda g: _pick_by_hq_in_group(g, "クライアント名"))
    )

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

    common_export_cols = [
        "クライアント名（半角カタカナ統一）",
        "支店名",
        "肩書",
        "送付氏名（姓　名）", "姓", "名",
        "郵便番号",
        "住所１", "住所２", "住所３",
        "住所（全文）",
        "電話番号",
        "旧字メモ（氏名）",
        "旧字メモ（住所）",
        "クライアント名", "ご担当者名", "担当者名称", "部課名", "部門",
        "住所連結（全文）",
    ]

    def _attach_common(df0: pd.DataFrame) -> pd.DataFrame:
        d = df0.copy()
        d["クライアント名（半角カタカナ統一）"] = d["_client_norm"].map(safe_text)
        if "送付氏名（姓　名）" not in d.columns:
            d["送付氏名（姓　名）"] = d.get("抽出氏名（クリーン）", d.get("抽出氏名", "")).map(safe_text)
        if "姓" not in d.columns:
            d["姓"] = ""
        if "名" not in d.columns:
            d["名"] = ""
        for c in ["支店名", "肩書", "郵便番号", "電話番号", "担当者名称", "部課名", "部門"]:
            if c not in d.columns:
                d[c] = ""
        return d

    df_main = _attach_common(df_main)[common_export_cols]

    common_check_cols = ["理由"] + [c for c in common_export_cols if c not in ["姓", "名"]]
    df_name_conflict_out = _attach_common(df_name_conflict_out)[common_check_cols]
    df_paren_conflict_out = _attach_common(df_paren_conflict_out)[common_check_cols]
    df_tantou_out = _attach_common(df_tantou_out)[[c for c in common_check_cols if c not in ["送付氏名（姓　名）", "姓", "名"]]]
    df_extraction_failure_out = _attach_common(df_extraction_failure_out)[["理由"] + [c for c in common_export_cols if c not in ["姓", "名", "送付氏名（姓　名）"]]]
    df_multi_name_out = _attach_common(df_multi_name_out)[["理由"] + [c for c in common_export_cols if c not in ["姓", "名", "送付氏名（姓　名）"]]]

    out_buf = BytesIO()
    with pd.ExcelWriter(out_buf, engine="openpyxl") as writer:
        df_main.to_excel(writer, sheet_name="送付対象", index=False)
        df_name_conflict_out.to_excel(writer, sheet_name="要確認（氏名重複）", index=False)
        df_paren_conflict_out.to_excel(writer, sheet_name="要確認（クライアント名括弧）", index=False)
        df_tantou_out.to_excel(writer, sheet_name="要確認（ご担当者表記）", index=False)
        df_extraction_failure_out.to_excel(writer, sheet_name="除外（氏名抽出失敗）", index=False)
        df_multi_name_out.to_excel(writer, sheet_name="除外（複数名）", index=False)

    summary = {
        "app_version": config.app_version,
        "input_filename": input_filename,
        "rows_input": int(len(df_raw)),
        "header_mapping": mapping,
        "counts": {
            "送付対象": int(len(df_main)),
            "要確認（氏名重複）": int(len(df_name_conflict_out)),
            "要確認（クライアント名括弧）": int(len(df_paren_conflict_out)),
            "要確認（ご担当者表記）": int(len(df_tantou_out)),
            "除外（氏名抽出失敗）": int(len(df_extraction_failure_out)),
            "除外（複数名）": int(len(df_multi_name_out)),
        },
        "generated_at": datetime.now(JST).isoformat(timespec="seconds"),
    }

    return out_buf.getvalue(), summary
