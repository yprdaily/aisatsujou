import streamlit as st
import pandas as pd
import os

import extract_greetings


def _consteq(a: str, b: str) -> bool:
    if a is None:
        a = ""
    if b is None:
        b = ""
    a = str(a)
    b = str(b)
    if len(a) != len(b):
        return False
    r = 0
    for x, y in zip(a.encode("utf-8"), b.encode("utf-8")):
        r |= x ^ y
    return r == 0


def _require_password() -> None:
    app_pw = str(os.getenv("APP_PASSWORD", "") or "")
    if app_pw == "":
        return

    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if st.session_state.authenticated:
        return

    st.title("ログイン")
    with st.form("login_form", clear_on_submit=False):
        entered = st.text_input("パスワードを入力してください", type="password")
        ok = st.form_submit_button("ログイン")

    if ok:
        if _consteq(entered, app_pw):
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("パスワードが違います")

    st.stop()


def _max_upload_bytes() -> int:
    mb = str(os.getenv("MAX_UPLOAD_MB", "20") or "20").strip()
    try:
        v = int(mb)
    except Exception:
        v = 20
    if v <= 0:
        v = 20
    return v * 1024 * 1024


def _main() -> None:
    st.set_page_config(page_title="挨拶状抽出システム", layout="centered")

    st.markdown(
        """
<style>
.block-container { max-width: 980px; padding-top: 1.5rem; padding-bottom: 2rem; }
footer { visibility: hidden; }
</style>
""",
        unsafe_allow_html=True,
    )

    _require_password()

    cfg = extract_greetings.load_config_from_env()

    st.title("挨拶状 送付対象者抽出")
    st.markdown("---")

    if cfg.app_version:
        st.caption(f"Version: {cfg.app_version}")

    st.info("経理提供のExcelファイルをアップロードしてください。")

    uploaded_file = st.file_uploader("Excelファイル（.xlsx）をアップロード", type=["xlsx"])
    if uploaded_file is None:
        return

    max_bytes = _max_upload_bytes()
    b = uploaded_file.getvalue()
    if len(b) > max_bytes:
        st.error(f"ファイルサイズが大きすぎます（上限 {max_bytes // (1024*1024)} MB）")
        return

    if st.button("処理実行"):
        st.session_state.pop("result_bytes", None)
        st.session_state.pop("result_summary", None)
        st.session_state.pop("result_filename", None)

        try:
            with st.spinner("処理を実行中..."):
                out_bytes, summary = extract_greetings.process_excel_bytes(
                    input_bytes=b,
                    input_filename=uploaded_file.name,
                    config=cfg,
                )

            out_name = summary.get("output_filename", "result.xlsx")

            st.session_state.result_bytes = out_bytes
            st.session_state.result_summary = summary
            st.session_state.result_filename = out_name

        except Exception as e:
            st.error(f"エラーが発生しました: {e}")
            st.exception(e)

    if st.session_state.get("result_bytes") is not None:
        st.success("処理が完了しました。以下のボタンからダウンロードしてください。")
        st.download_button(
            label="📥 処理結果をダウンロード",
            data=st.session_state.result_bytes,
            file_name=st.session_state.get("result_filename") or "result.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        with st.expander("処理サマリ", expanded=False):
            st.json(st.session_state.get("result_summary") or {})


if __name__ == "__main__":
    _main()