import streamlit as st
import pandas as pd
import os
import tempfile
from pathlib import Path
import extract_greetings

st.set_page_config(page_title="挨拶状 送付対象者抽出", layout="wide", initial_sidebar_state="collapsed")

APP_VERSION = (os.getenv("APP_VERSION") or "").strip()
MAX_UPLOAD_MB = int((os.getenv("MAX_UPLOAD_MB") or "20").strip() or "20")
EXPECTED_PASSWORD = (os.getenv("APP_PASSWORD") or "").strip()

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

def _hide_chrome():
    st.markdown(
        """
        <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        </style>
        """,
        unsafe_allow_html=True,
    )

def _login_view():
    _hide_chrome()
    st.title("挨拶状 送付対象者抽出")
    if APP_VERSION:
        st.caption(f"Version: {APP_VERSION}")

    if not EXPECTED_PASSWORD:
        st.error("APP_PASSWORD が未設定です（Cloud Run の Secret / 環境変数を確認してください）。")
        st.stop()

    def check_password():
        entered = (st.session_state.get("password_input") or "")
        if entered == EXPECTED_PASSWORD:
            st.session_state.authenticated = True
            st.session_state.password_input = ""
        else:
            st.session_state.authenticated = False
            st.error("パスワードが違います")

    st.subheader("ログイン")
    st.text_input("パスワードを入力してください", type="password", key="password_input", on_change=check_password)
    st.stop()

def _main_view():
    _hide_chrome()
    st.title("挨拶状 送付対象者抽出")
    if APP_VERSION:
        st.caption(f"Version: {APP_VERSION}")

    st.info("経理提供のExcelファイルをアップロードしてください。処理結果はExcelでダウンロードできます。")
    uploaded_file = st.file_uploader("Excelファイル (xlsx) をアップロード", type=["xlsx"])

    if uploaded_file is None:
        return

    max_bytes = MAX_UPLOAD_MB * 1024 * 1024
    try:
        size = uploaded_file.size
    except Exception:
        size = None

    if size is not None and size > max_bytes:
        st.error(f"ファイルサイズが大きすぎます（上限 {MAX_UPLOAD_MB} MB）。")
        return

    if st.button("処理実行"):
        with st.status("処理を実行中...", expanded=True) as status:
            original_base_dir = getattr(extract_greetings, "BASE_DIR", None)
            original_input = getattr(extract_greetings, "INPUT_XLSX", None)
            original_output = getattr(extract_greetings, "OUTPUT_XLSX", None)
            original_log = getattr(extract_greetings, "LOG_FILE", None)

            try:
                with tempfile.TemporaryDirectory() as tmp_dir:
                    tmp_path = Path(tmp_dir)

                    input_path = tmp_path / "input.xlsx"
                    with open(input_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())

                    output_path = tmp_path / "挨拶状_送付対象.xlsx"
                    log_path = tmp_path / "process.log"

                    if hasattr(extract_greetings, "BASE_DIR"):
                        extract_greetings.BASE_DIR = tmp_path
                    if hasattr(extract_greetings, "INPUT_XLSX"):
                        extract_greetings.INPUT_XLSX = input_path
                    if hasattr(extract_greetings, "OUTPUT_XLSX"):
                        extract_greetings.OUTPUT_XLSX = output_path
                    if hasattr(extract_greetings, "LOG_FILE"):
                        extract_greetings.LOG_FILE = log_path

                    status.update(label="データ抽出・変換中...", state="running")
                    out = extract_greetings.main()

                    status.update(label="処理完了。ダウンロード準備中...", state="running")
                    out_path = Path(out)

                    if not out_path.exists():
                        raise FileNotFoundError(f"出力ファイルが見つかりません: {out_path}")

                    with open(out_path, "rb") as f:
                        data = f.read()

                    status.update(label="処理完了！", state="complete")
                    st.success("処理が完了しました。以下からダウンロードしてください。")

                    st.download_button(
                        label="📥 処理結果をダウンロード",
                        data=data,
                        file_name=f"挨拶状_送付対象_{pd.Timestamp.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )

            except Exception as e:
                st.error(f"エラーが発生しました: {e}")
                st.exception(e)
            finally:
                if original_base_dir is not None:
                    extract_greetings.BASE_DIR = original_base_dir
                if original_input is not None:
                    extract_greetings.INPUT_XLSX = original_input
                if original_output is not None:
                    extract_greetings.OUTPUT_XLSX = original_output
                if original_log is not None:
                    extract_greetings.LOG_FILE = original_log

if not st.session_state.authenticated:
    _login_view()
else:
    _main_view()
