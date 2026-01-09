import os
import time
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path

import streamlit as st

import extract_greetings

JST = timezone(timedelta(hours=9))


def _env_required(name: str) -> str:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        raise RuntimeError(f"環境変数 {name} が未設定です。")
    return str(v)


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        n = int(str(v).strip())
        return n if n > 0 else default
    except Exception:
        return default


st.set_page_config(page_title="挨拶状抽出システム", layout="centered")

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

try:
    APP_PASSWORD = _env_required("APP_PASSWORD")
except Exception as e:
    st.error(str(e))
    st.stop()

cfg = extract_greetings.load_config_from_env()
max_upload_mb = _env_int("MAX_UPLOAD_MB", 20)
max_upload_bytes = max_upload_mb * 1024 * 1024

st.sidebar.header("設定")
st.sidebar.write(
    {
        "APP_VERSION": cfg.app_version,
        "ADDR_SPLIT_MODE": cfg.addr_split_mode,
        "SURNAME_WHITELIST": ",".join(sorted(cfg.surname_whitelist)) if cfg.surname_whitelist else "",
        "MAX_UPLOAD_MB": max_upload_mb,
    }
)

st.title("挨拶状 送付対象者抽出")
st.markdown("---")


def check_password():
    if st.session_state.password_input == APP_PASSWORD:
        st.session_state.authenticated = True
        del st.session_state.password_input
    else:
        st.error("パスワードが違います")


if not st.session_state.authenticated:
    st.subheader("ログイン")
    st.text_input("パスワードを入力してください", type="password", key="password_input", on_change=check_password)
    st.stop()

st.info("経理提供のExcelファイルをアップロードしてください。処理は自動で行われます。")

uploaded_file = st.file_uploader("Excelファイル (input.xlsx) をアップロード", type=["xlsx"])

if uploaded_file is None:
    st.stop()

if uploaded_file.size and uploaded_file.size > max_upload_bytes:
    st.error(f"ファイルサイズが上限を超えています（上限: {max_upload_mb} MB）。")
    st.stop()

st.write({"ファイル名": uploaded_file.name, "サイズ(bytes)": int(uploaded_file.size or 0)})

if st.button("処理実行", type="primary", use_container_width=True):
    with st.status("処理を実行中...", expanded=True) as status:
        started = time.time()
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                tmp_path = Path(tmp_dir)
                input_path = tmp_path / "input.xlsx"
                input_path.write_bytes(uploaded_file.getbuffer())

                status.update(label="データ抽出・変換中...", state="running")

                out_bytes, summary = extract_greetings.process_excel_bytes(
                    input_bytes=input_path.read_bytes(),
                    input_filename=uploaded_file.name,
                    config=extract_greetings.ProcessConfig(
                        addr_split_mode=cfg.addr_split_mode,
                        surname_whitelist=cfg.surname_whitelist,
                        app_version=cfg.app_version,
                    ),
                )

            elapsed = time.time() - started
            status.update(label=f"処理完了（{elapsed:.2f}秒）", state="complete")

            st.success("処理が完了しました。以下のボタンからダウンロードしてください。")
            st.subheader("処理サマリ")
            st.write(summary)

            ts = datetime.now(JST).strftime("%Y%m%d")
            st.download_button(
                label="📥 処理結果をダウンロード",
                data=out_bytes,
                file_name=f"挨拶状_送付対象_{ts}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        except Exception as e:
            status.update(label="エラーが発生しました", state="error")
            st.error(f"エラーが発生しました: {e}")
            st.exception(e)
