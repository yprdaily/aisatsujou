# extract_greetings.py
from __future__ import annotations

import hashlib
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from io import BytesIO
from pathlib import Path
from typing import Optional, Iterable

import pandas as pd


JST = timezone(timedelta(hours=9))

COL_CLIENT = "クライアント名"
COL_CONTACT = "ご担当者名"
COL_POST = "郵便番号"
COL_ADDR1 = "住所１"
COL_ADDR2 = "住所２"
COL_ADDR3 = "住所３"
COL_TEL = "電話番号"
COL_TANTO = "担当者名称"
COL_BUKA = "部課名"
COL_BUMON = "部門"

REQUIRED_COLS = [
    COL_CLIENT,
    COL_CONTACT,
    COL_POST,
    COL_ADDR1,
    COL_ADDR2,
    COL_ADDR3,
    COL_TEL,
    COL_TANTO,
    COL_BUKA,
    COL_BUMON,
]

KANJI_REPLACE_MAP = {
    "髙": "高",
    "邊": "辺",
    "﨑": "崎",
}

TITLE_KEYWORDS = [
    "代表取締役",
    "取締役",
    "執行役員",
    "監査役",
    "会長",
    "社長",
    "専務",
    "常務",
    "本部長",
    "部長",
    "次長",
    "課長",
    "室長",
    "所長",
    "支店長",
    "マネージャー",
    "担当者",
    "様",
]

ORG_ENDINGS = [
    "本部",
    "統括本部",
    "事業本部",
    "支店",
    "営業所",
    "事業所",
    "センター",
    "本社",
    "部",
    "課",
    "室",
    "所",
]

BUILDING_KEYWORDS = [
    "ビル",
    "ビルディング",
    "マンション",
    "アパート",
    "ハイツ",
    "コーポ",
    "レジデンス",
    "タワー",
    "プラザ",
    "会館",
    "会舘",
    "センター",
    "オフィス",
    "工場",
    "寮",
    "荘",
    "コート",
    "ハウス",
    "ホーム",
    "館",
    "庁舎",
    "棟",
]

HYPHEN_CHARS = "－‐-‒–—−ー-"
HYPHEN_RE = r"[{}]".format(re.escape(HYPHEN_CHARS))

MULTI_DELIMS_RE = re.compile(r"[、,，・/／&＆＋+]|(?:他|ほか|等)|(?:及び)|(?:と\s)|(?:and\b)", re.IGNORECASE)
PLACEHOLDER_RE = re.compile(r"(ご担当者様|ご担当者)", re.IGNORECASE)
PAREN_RE = re.compile(r"[()（）]")

FULL_KATA_MAP = {
    "ガ": "ｶﾞ",
    "ギ": "ｷﾞ",
    "グ": "ｸﾞ",
    "ゲ": "ｹﾞ",
    "ゴ": "ｺﾞ",
    "ザ": "ｻﾞ",
    "ジ": "ｼﾞ",
    "ズ": "ｽﾞ",
    "ゼ": "ｾﾞ",
    "ゾ": "ｿﾞ",
    "ダ": "ﾀﾞ",
    "ヂ": "ﾁﾞ",
    "ヅ": "ﾂﾞ",
    "デ": "ﾃﾞ",
    "ド": "ﾄﾞ",
    "バ": "ﾊﾞ",
    "ビ": "ﾋﾞ",
    "ブ": "ﾌﾞ",
    "ベ": "ﾍﾞ",
    "ボ": "ﾎﾞ",
    "パ": "ﾊﾟ",
    "ピ": "ﾋﾟ",
    "プ": "ﾌﾟ",
    "ペ": "ﾍﾟ",
    "ポ": "ﾎﾟ",
    "ヴ": "ｳﾞ",
    "ワ": "ﾜ",
    "ヰ": "ｲ",
    "ヱ": "ｴ",
    "ヲ": "ｦ",
    "ア": "ｱ",
    "イ": "ｲ",
    "ウ": "ｳ",
    "エ": "ｴ",
    "オ": "ｵ",
    "カ": "ｶ",
    "キ": "ｷ",
    "ク": "ｸ",
    "ケ": "ｹ",
    "コ": "ｺ",
    "サ": "ｻ",
    "シ": "ｼ",
    "ス": "ｽ",
    "セ": "ｾ",
    "ソ": "ｿ",
    "タ": "ﾀ",
    "チ": "ﾁ",
    "ツ": "ﾂ",
    "テ": "ﾃ",
    "ト": "ﾄ",
    "ナ": "ﾅ",
    "ニ": "ﾆ",
    "ヌ": "ﾇ",
    "ネ": "ﾈ",
    "ノ": "ﾉ",
    "ハ": "ﾊ",
    "ヒ": "ﾋ",
    "フ": "ﾌ",
    "ヘ": "ﾍ",
    "ホ": "ﾎ",
    "マ": "ﾏ",
    "ミ": "ﾐ",
    "ム": "ﾑ",
    "メ": "ﾒ",
    "モ": "ﾓ",
    "ヤ": "ﾔ",
    "ユ": "ﾕ",
    "ヨ": "ﾖ",
    "ラ": "ﾗ",
    "リ": "ﾘ",
    "ル": "ﾙ",
    "レ": "ﾚ",
    "ロ": "ﾛ",
    "ヮ": "ﾜ",
    "ン": "ﾝ",
    "ァ": "ｧ",
    "ィ": "ｨ",
    "ゥ": "ｩ",
    "ェ": "ｪ",
    "ォ": "ｫ",
    "ッ": "ｯ",
    "ャ": "ｬ",
    "ュ": "ｭ",
    "ョ": "ｮ",
    "、": "､",
    "。": "｡",
    "ー": "ｰ",
    "「": "｢",
    "」": "｣",
    "・": "･",
}

TITLE_TOKEN_PATTERN = re.compile(
    r"^(?:"
    r"代表取締役|取締役|執行役員|監査役|会長|社長|専務|常務|本部長|部長|次長|課長|室長|所長|支店長|"
    r"課長代理|部長代理|次長代理|主任|係長|代理|補佐|リーダー|マネージャー|担当|担当者|"
    r".*?代理|.*?補佐"
    r")$"
)


@dataclass(frozen=True)
class ProcessConfig:
    addr_split_mode: str = "auto"
    surname_whitelist: frozenset[str] = frozenset()
    app_version: str = "v0"


def _is_na(v) -> bool:
    return v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v)


def safe_str(v) -> str:
    if _is_na(v):
        return ""
    s = str(v)
    if s.lower() == "nan" or s.lower() == "none":
        return ""
    return s


def normalize_colname(name: str) -> str:
    t = unicodedata.normalize("NFKC", safe_str(name))
    t = t.replace(" ", "").replace("　", "")
    t = t.replace("_", "").replace("-", "")
    return t


def canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    raw_cols = list(df.columns)
    norm_to_raw = {normalize_colname(c): c for c in raw_cols}

    def pick(*cands: str) -> Optional[str]:
        for c in cands:
            if c in norm_to_raw:
                return norm_to_raw[c]
        return None

    mapping = {}

    c = pick("クライアント名")
    if c:
        mapping[c] = COL_CLIENT
    c = pick("ご担当者名")
    if c:
        mapping[c] = COL_CONTACT
    c = pick("郵便番号")
    if c:
        mapping[c] = COL_POST
    c = pick("住所1", "住所１")
    if c:
        mapping[c] = COL_ADDR1
    c = pick("住所2", "住所２")
    if c:
        mapping[c] = COL_ADDR2
    c = pick("住所3", "住所３")
    if c:
        mapping[c] = COL_ADDR3
    c = pick("電話番号", "TEL", "Tel", "tel")
    if c:
        mapping[c] = COL_TEL
    c = pick("担当者名称")
    if c:
        mapping[c] = COL_TANTO
    c = pick("部課名")
    if c:
        mapping[c] = COL_BUKA
    c = pick("部門")
    if c:
        mapping[c] = COL_BUMON

    if mapping:
        df = df.rename(columns=mapping)
    return df


def normalize_spaces_fullwidth(s: str) -> str:
    t = safe_str(s).strip()
    t = re.sub(r"[ \t]+", "　", t)
    t = re.sub(r"　{2,}", "　", t)
    return t


def apply_kanji_conversion(s: str) -> str:
    t = safe_str(s)
    for old, new in KANJI_REPLACE_MAP.items():
        t = t.replace(old, new)
    return t


def kanji_conversion_memo(before: str) -> str:
    b = safe_str(before)
    memos = []
    for old, new in KANJI_REPLACE_MAP.items():
        if old in b:
            memos.append(f"{old}→{new}")
    return " / ".join(memos)


def normalize_full_katakana_to_half(s: str) -> str:
    t = unicodedata.normalize("NFKC", safe_str(s))
    if not t:
        return ""
    pattern = "|".join(map(re.escape, FULL_KATA_MAP.keys()))
    if pattern:
        t = re.sub(f"({pattern})", lambda m: FULL_KATA_MAP.get(m.group(0), m.group(0)), t)
    return t


def is_multi_name_cell(text: str) -> bool:
    t = safe_str(text)
    if not t:
        return False
    return bool(MULTI_DELIMS_RE.search(t))


def contains_parentheses_any_width(text: str) -> bool:
    t = safe_str(text)
    if not t:
        return False
    return bool(PAREN_RE.search(t))


def is_tantou_placeholder(text: str) -> bool:
    t = safe_str(text)
    if not t:
        return False
    return bool(PLACEHOLDER_RE.search(t))


def surname_is_orglike(surname: str, whitelist: set[str]) -> bool:
    s = safe_str(surname)
    if not s:
        return False
    if s in whitelist:
        return False
    if len(s) < 3:
        return False
    for ending in sorted(ORG_ENDINGS, key=len, reverse=True):
        if s.endswith(ending):
            return True
    return False


def extract_last_fullname(text: str, whitelist: set[str]) -> Optional[tuple[str, str, str, str]]:
    raw = safe_str(text).strip()
    if not raw:
        return None

    norm = normalize_spaces_fullwidth(raw)

    m = re.search(r"(.+?)　([^　]+?)　([^　]+?)$", norm)
    if not m:
        return None

    prefix = m.group(1).strip("　")
    last_family = m.group(2).strip("　")
    last_given = m.group(3).strip("　")

    if not last_family or not last_given:
        return None

    if len(prefix) > 30:
        return None

    if len(last_family) > 5:
        return None

    if last_family not in whitelist:
        for kw in TITLE_KEYWORDS:
            if kw and kw in last_family:
                return None

    if surname_is_orglike(last_family, whitelist):
        return None

    fullname = f"{last_family}　{last_given}"
    return (fullname, last_family, last_given, prefix)


def split_prefix_to_dept_title(prefix: str) -> tuple[str, str]:
    p = normalize_spaces_fullwidth(prefix)
    if not p:
        return ("", "")
    tokens = [t for t in p.split("　") if t.strip()]
    if not tokens:
        return ("", "")

    titles_rev: list[str] = []
    i = len(tokens) - 1
    while i >= 0:
        tok = tokens[i]
        if TITLE_TOKEN_PATTERN.match(tok):
            titles_rev.append(tok)
            i -= 1
            continue
        if i >= 1 and tokens[i - 1] + tokens[i] in {"課長代理", "部長代理", "次長代理"}:
            titles_rev.append(tokens[i - 1] + tokens[i])
            i -= 2
            continue
        break

    dept_tokens = tokens[: i + 1]
    title_tokens = list(reversed(titles_rev))
    dept = "　".join(dept_tokens).strip()
    title = "　".join(title_tokens).strip()
    return (dept, title)


def join_address(a1, a2, a3) -> str:
    parts = [safe_str(x).strip() for x in (a1, a2, a3)]
    parts = [p for p in parts if p]
    s = " ".join(parts)
    s = re.sub(r"[\s\r\n\t　]+", " ", s).strip()
    return s


def _strip_spaces_with_map(s: str) -> tuple[str, list[int]]:
    idx_map = []
    out_chars = []
    for i, ch in enumerate(s):
        if ch in {" ", "　", "\t", "\r", "\n"}:
            continue
        idx_map.append(i)
        out_chars.append(ch)
    return ("".join(out_chars), idx_map)


def _split_at_nospace_index(original: str, idx_map: list[int], nospace_pos: int) -> tuple[str, str]:
    if nospace_pos <= 0:
        return (original.strip(), "")
    if nospace_pos >= len(idx_map):
        return (original.strip(), "")
    cut = idx_map[nospace_pos]
    left = original[:cut].strip()
    right = original[cut:].strip()
    return (left, right)


def split_address(full_addr: str, mode: str) -> tuple[str, str]:
    s = safe_str(full_addr).strip()
    if not s:
        return ("", "")

    mode = (mode or "auto").strip().lower()
    if mode not in {"auto", "building_first", "street_first"}:
        mode = "auto"

    s_norm = re.sub(r"[\s\r\n\t　]+", " ", s).strip()
    nospace, idx_map = _strip_spaces_with_map(s_norm)

    kw_pattern = "|".join(map(re.escape, sorted(BUILDING_KEYWORDS, key=len, reverse=True)))
    kw_re = re.compile(kw_pattern)

    street_re = re.compile(
        rf"(.+?)(\d{{1,4}}(?:丁目)?{HYPHEN_RE}\d{{1,4}}(?:{HYPHEN_RE}\d{{1,4}})?|\d{{1,4}}丁目\d{{1,4}}(?:{HYPHEN_RE}\d{{1,4}})?|\d{{1,4}}番地?\d{{1,4}}(?:号)?)(.+)"
    )

    def try_building() -> Optional[tuple[str, str]]:
        m = kw_re.search(nospace)
        if not m:
            return None
        left, right = _split_at_nospace_index(s_norm, idx_map, m.start())
        if left and right:
            return (left, right)
        return None

    def try_street() -> Optional[tuple[str, str]]:
        m = street_re.match(nospace)
        if not m:
            return None
        base = (m.group(1) + m.group(2)).strip()
        tail = m.group(3).strip()
        if not tail:
            return None
        pos = len(base)
        left, right = _split_at_nospace_index(s_norm, idx_map, pos)
        if left and right:
            return (left, right)
        return None

    tries: list[callable] = []
    if mode == "building_first":
        tries = [try_building, try_street]
    elif mode == "street_first":
        tries = [try_street, try_building]
    else:
        tries = [try_building, try_street]

    for fn in tries:
        res = fn()
        if res:
            a1, a2 = res
            return (a1, a2)

    return (s_norm, "")


def sanitize_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out.where(~out.isna(), "")
    for c in out.columns:
        if out[c].dtype == object:
            out[c] = out[c].map(lambda v: "" if _is_na(v) else ("" if str(v).strip().lower() in {"nan", "none"} else str(v)))
    return out


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def build_summary_df(summary: dict[str, object]) -> pd.DataFrame:
    items = []
    for k, v in summary.items():
        items.append({"項目": k, "値": "" if v is None else str(v)})
    return pd.DataFrame(items)


def process_excel_bytes(
    input_bytes: bytes,
    input_filename: str,
    config: ProcessConfig,
) -> tuple[bytes, dict[str, object]]:
    if not input_bytes:
        raise ValueError("入力ファイルが空です。")

    in_hash = sha256_bytes(input_bytes)
    with BytesIO(input_bytes) as bio:
        df = pd.read_excel(bio, sheet_name=0, engine="openpyxl", dtype=object)

    df = canonicalize_columns(df)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"必要な列が見つかりません: {missing}")

    surname_whitelist = set(config.surname_whitelist)

    df["クライアント名（原文）"] = df[COL_CLIENT].map(safe_str)
    df["ご担当者名（原文）"] = df[COL_CONTACT].map(safe_str)

    df["旧字メモ（氏名）"] = df[COL_CONTACT].map(kanji_conversion_memo)
    df["旧字メモ（住所）"] = df.apply(
        lambda r: " / ".join(
            [m for m in [kanji_conversion_memo(r.get(COL_ADDR1, "")), kanji_conversion_memo(r.get(COL_ADDR2, "")), kanji_conversion_memo(r.get(COL_ADDR3, ""))] if m]
        ),
        axis=1,
    ).map(safe_str)

    df[COL_CONTACT] = df[COL_CONTACT].map(apply_kanji_conversion)
    df[COL_ADDR1] = df[COL_ADDR1].map(apply_kanji_conversion)
    df[COL_ADDR2] = df[COL_ADDR2].map(apply_kanji_conversion)
    df[COL_ADDR3] = df[COL_ADDR3].map(apply_kanji_conversion)
    df[COL_CLIENT] = df[COL_CLIENT].map(apply_kanji_conversion)

    df["クライアント名（半角カナ統一）"] = df[COL_CLIENT].map(normalize_full_katakana_to_half)
    df["_client_has_paren"] = df[COL_CLIENT].map(contains_parentheses_any_width)

    extracted = df[COL_CONTACT].map(lambda s: extract_last_fullname(s, surname_whitelist))
    df["_extracted_ok"] = extracted.map(lambda x: bool(x))
    df["送付氏名（姓　名）"] = extracted.map(lambda x: x[0] if x else "")
    df["姓"] = extracted.map(lambda x: x[1] if x else "")
    df["名"] = extracted.map(lambda x: x[2] if x else "")
    df["_prefix_raw"] = extracted.map(lambda x: x[3] if x else "")

    dept_title = df["_prefix_raw"].map(split_prefix_to_dept_title)
    df["部署名（推定）"] = dept_title.map(lambda t: t[0])
    df["肩書"] = dept_title.map(lambda t: t[1])

    df["_is_multi"] = df[COL_CONTACT].map(is_multi_name_cell)
    df["_is_placeholder"] = df[COL_CONTACT].map(is_tantou_placeholder)

    df["住所（全文）"] = df.apply(lambda r: join_address(r.get(COL_ADDR1, ""), r.get(COL_ADDR2, ""), r.get(COL_ADDR3, "")), axis=1)
    df[["住所１", "住所２"]] = df["住所（全文）"].map(lambda s: split_address(s, config.addr_split_mode)).apply(pd.Series)

    df["_surname_orglike"] = df["姓"].map(lambda s: "要" if surname_is_orglike(s, surname_whitelist) else "")

    def build_attention(row) -> tuple[str, str]:
        reasons = []
        if safe_str(row.get("部署名（推定）")):
            reasons.append("部署名あり")
        if safe_str(row.get("肩書")):
            reasons.append("肩書あり")
        if safe_str(row.get("_surname_orglike")):
            reasons.append("姓が組織語尾")
        if len(safe_str(row.get("名"))) == 1:
            reasons.append("名が1文字")
        if reasons:
            return ("要", " / ".join(reasons))
        return ("", "")

    att = df.apply(build_attention, axis=1, result_type="expand")
    att.columns = ["注意フラグ", "注意理由"]
    df = pd.concat([df, att], axis=1)

    ok_mask = df["_extracted_ok"] & (~df["_is_multi"]) & (~df["_is_placeholder"])
    client_nuniques = (
        df.loc[ok_mask].groupby("送付氏名（姓　名）")["クライアント名（半角カナ統一）"].nunique(dropna=False).reset_index(name="client_distinct")
    )
    conflict_names = set(client_nuniques.loc[client_nuniques["client_distinct"] > 1, "送付氏名（姓　名）"])
    df["_name_conflict"] = df["送付氏名（姓　名）"].isin(conflict_names)

    reason = []
    for _, r in df.iterrows():
        if r["_is_multi"]:
            reason.append("連名/複数名")
        elif not r["_extracted_ok"]:
            reason.append("氏名抽出失敗")
        elif r["_is_placeholder"]:
            reason.append("ご担当者表記")
        elif r["_client_has_paren"]:
            reason.append("クライアント名に括弧あり")
        elif r["_name_conflict"]:
            reason.append("氏名同一でクライアント相違")
        else:
            reason.append("")
    df["理由"] = reason

    mask_multi = df["_is_multi"]
    mask_fail = (~df["_extracted_ok"]) & (~mask_multi)
    mask_placeholder = df["_is_placeholder"] & (~mask_multi) & (~mask_fail)
    mask_paren = df["_client_has_paren"] & ok_mask
    mask_name_conf = df["_name_conflict"] & ok_mask & (~mask_paren)

    df_ex_multi = df.loc[mask_multi].copy()
    df_ex_fail = df.loc[mask_fail].copy()
    df_chk_placeholder = df.loc[mask_placeholder].copy()
    df_chk_paren = df.loc[mask_paren].copy()
    df_chk_name = df.loc[mask_name_conf].copy()

    df_main = df.loc[~(mask_multi | mask_fail | mask_placeholder | mask_paren | mask_name_conf)].copy()

    export_cols_main = [
        "注意フラグ",
        "注意理由",
        "クライアント名（半角カナ統一）",
        "部署名（推定）",
        "肩書",
        "送付氏名（姓　名）",
        "姓",
        "名",
        COL_POST,
        "住所１",
        "住所２",
        "住所（全文）",
        COL_TEL,
        "旧字メモ（氏名）",
        "旧字メモ（住所）",
        "クライアント名（原文）",
        "ご担当者名（原文）",
    ]

    export_cols_check = ["理由"] + export_cols_main
    export_cols_ex = [
        "理由",
        "注意フラグ",
        "注意理由",
        "クライアント名（半角カナ統一）",
        COL_CLIENT,
        COL_CONTACT,
        COL_POST,
        "住所（全文）",
        "住所１",
        "住所２",
        COL_TEL,
        "クライアント名（原文）",
        "ご担当者名（原文）",
    ]

    df_main = sanitize_df(df_main.reindex(columns=export_cols_main))
    df_chk_name = sanitize_df(df_chk_name.reindex(columns=export_cols_check))
    df_chk_paren = sanitize_df(df_chk_paren.reindex(columns=export_cols_check))
    df_chk_placeholder = sanitize_df(df_chk_placeholder.reindex(columns=export_cols_check))
    df_ex_fail = sanitize_df(df_ex_fail.reindex(columns=export_cols_ex))
    df_ex_multi = sanitize_df(df_ex_multi.reindex(columns=export_cols_ex))

    summary = {
        "処理日時（JST）": datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"),
        "アプリVersion": config.app_version,
        "入力ファイル名": input_filename,
        "入力ファイルSHA256": in_hash,
        "入力行数": len(df),
        "送付対象 件数": len(df_main),
        "要確認（氏名重複） 件数": len(df_chk_name),
        "要確認（クライアント名括弧） 件数": len(df_chk_paren),
        "要確認（ご担当者表記） 件数": len(df_chk_placeholder),
        "除外（氏名抽出失敗） 件数": len(df_ex_fail),
        "除外（複数名） 件数": len(df_ex_multi),
        "注意フラグ 件数（送付対象内）": int((df_main["注意フラグ"] == "要").sum()) if "注意フラグ" in df_main.columns else 0,
        "住所分割モード": config.addr_split_mode,
        "例外苗字（環境変数）": ",".join(sorted(surname_whitelist)) if surname_whitelist else "",
    }

    out_bio = BytesIO()
    with pd.ExcelWriter(out_bio, engine="openpyxl") as writer:
        build_summary_df(summary).to_excel(writer, sheet_name="処理サマリ", index=False)
        df_main.to_excel(writer, sheet_name="送付対象", index=False)
        df_chk_name.to_excel(writer, sheet_name="要確認（氏名重複）", index=False)
        df_chk_paren.to_excel(writer, sheet_name="要確認（クライアント名括弧）", index=False)
        df_chk_placeholder.to_excel(writer, sheet_name="要確認（ご担当者表記）", index=False)
        df_ex_fail.to_excel(writer, sheet_name="除外（氏名抽出失敗）", index=False)
        df_ex_multi.to_excel(writer, sheet_name="除外（複数名）", index=False)

    return out_bio.getvalue(), summary


def load_config_from_env() -> ProcessConfig:
    addr_split_mode = safe_str(os.getenv("ADDR_SPLIT_MODE", "auto")).strip() or "auto"
    app_version = safe_str(os.getenv("APP_VERSION", "v0")).strip() or "v0"
    wl_raw = safe_str(os.getenv("SURNAME_WHITELIST", "")).strip()
    wl = frozenset({x.strip() for x in wl_raw.split(",") if x.strip()})
    return ProcessConfig(addr_split_mode=addr_split_mode, surname_whitelist=wl, app_version=app_version)


def process_excel_file(input_path: str | Path, output_path: str | Path | None = None) -> Path:
    p = Path(input_path)
    if not p.exists():
        raise FileNotFoundError(str(p))
    data = p.read_bytes()
    cfg = load_config_from_env()
    out_bytes, _ = process_excel_bytes(data, p.name, cfg)
    outp = Path(output_path) if output_path else (p.parent / "挨拶状_送付対象.xlsx")
    outp.write_bytes(out_bytes)
    return outp
