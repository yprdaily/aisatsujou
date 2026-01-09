import os
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

import extract_greetings


st.set_page_config(page_title="挨拶状 送付対象者抽出", layout="centered", initial_sidebar_state="collapsed")


APP_VERSION = (os.getenv("APP_VERSION") or "").strip()
MAX_UPLOAD_MB = int((os.getenv("MAX_UPLOAD_MB") or "20").strip() or "20")
EXPECTED_PASSWORD = (os.getenv("APP_PASSWORD") or "").rstrip("\r\n")


def _hide_streamlit_ui():
    st.markdown(
        """
        <style>
        #MainMenu {visibility:hidden;}
        footer {visibility:hidden;}
        header {visibility:hidden;}

        section[data-testid="stSidebar"] {display:none !important;}
        div[data-testid="collapsedControl"] {display:none !important;}

        .block-container {max-width: 980px; padding-top: 2.0rem; padding-bottom: 3.0rem;}
        </style>
        """,
        unsafe_allow_html=True,
    )


def _require_password():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not EXPECTED_PASSWORD:
        st.error("APP_PASSWORD が未設定です（Cloud Run の Secret / 環境変数を確認してください）。")
        st.stop()

    if st.session_state.authenticated:
        return

    st.title("挨拶状 送付対象者抽出")
    if APP_VERSION:
        st.caption(f"Version: {APP_VERSION}")

    st.subheader("ログイン")
    entered = st.text_input("パスワードを入力してください", type="password", key="password_input")

    if st.button("ログイン"):
        if (entered or "").rstrip("\r\n") == EXPECTED_PASSWORD:
            st.session_state.authenticated = True
            st.session_state.password_input = ""
            st.rerun()
        else:
            st.error("パスワードが違います")

    st.stop()


def _main():
    _hide_streamlit_ui()
    _require_password()

    st.title("挨拶状 送付対象者抽出")
    if APP_VERSION:
        st.caption(f"Version: {APP_VERSION}")

    st.info("経理提供のExcelファイルをアップロードしてください。処理結果はExcelでダウンロードできます。")

    uploaded = st.file_uploader("Excelファイル（.xlsx）をアップロード", type=["xlsx"])

    if uploaded is None:
        return

    try:
        size = uploaded.size
    except Exception:
        size = None

    if size is not None:
        max_bytes = MAX_UPLOAD_MB * 1024 * 1024
        if size > max_bytes:
            st.error(f"ファイルサイズが大きすぎます（上限 {MAX_UPLOAD_MB} MB）。")
            return

    if st.button("処理実行"):
        with st.status("処理を実行中...", expanded=True) as status:
            try:
                input_bytes = uploaded.getvalue()
                input_name = getattr(uploaded, "name", "input.xlsx") or "input.xlsx"

                status.update(label="設定読み込み...", state="running")
                cfg = extract_greetings.load_config_from_env()

                status.update(label="データ抽出・変換中...", state="running")
                out_bytes, summary = extract_greetings.process_excel_bytes(
                    input_bytes=input_bytes,
                    input_filename=input_name,
                    config=cfg,
                )

                status.update(label="処理完了！", state="complete")

                st.success("処理が完了しました。以下のボタンからダウンロードしてください。")
                st.download_button(
                    label="📥 処理結果をダウンロード",
                    data=out_bytes,
                    file_name=f"挨拶状_送付対象_{pd.Timestamp.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

            except Exception as e:
                st.error(f"エラーが発生しました: {e}")
                st.exception(e)


if __name__ == "__main__":
    _main()
