
import os, asyncio, io
from datetime import datetime
import pandas as pd
from pydantic import BaseModel
from dotenv import load_dotenv
import streamlit as st

from utils import extract_emails_from_text, crawl_site_for_emails
from connectors import fetch_hira_facilities, fetch_localdata_business, find_homepage_via_bing

load_dotenv()

st.set_page_config(page_title="지역·업종 스크래퍼", layout="wide")

st.title("지역과 업종으로 회사 정보를 수집하여 엑셀로 저장")

st.markdown("""
이 도구는 선택한 지역과 업종을 기준으로
공공 데이터 우선 원천에서 사업장 목록을 가져오고,
가능한 경우 공식 홈페이지를 탐색하여 대표 이메일을 추출합니다.
주소는 입력 그대로 저장하며, 도로명주소 정규화는 추후 모듈에서 확장할 수 있습니다.
""")

with st.sidebar:
    st.header("설정")
    sido = st.selectbox("시도 선택", ["서울특별시","부산광역시","대구광역시","인천광역시","광주광역시","대전광역시","울산광역시","세종특별자치시","경기도","강원특별자치도","충청북도","충청남도","전라북도","전라남도","경상북도","경상남도","제주특별자치도"])
    sggu = st.text_input("시군구 입력", "", placeholder="예: 강남구, 수원시 영통구 등")
    industry = st.radio("업종 선택", ["의료기관", "일반 인허가 업종(음식점, 미용 등)"])
    only_role = st.checkbox("역할 계정만 선호", value=True, help="info, contact, sales 등 역할 기반 이메일을 우선 정렬")
    limit = st.slider("최대 항목 수", min_value=50, max_value=2000, value=200, step=50)
    run = st.button("수집 시작")

def role_priority(emails):
    if not emails:
        return []
    roles = ("info@", "contact@", "sales@", "admin@", "support@", "help@")
    return sorted(set(emails), key=lambda e: (not e.lower().startswith(roles), e))

async def pipeline():
    rows = []
    # 1단계: 원천 데이터
    if industry == "의료기관":
        rows = await fetch_hira_facilities(sido=sido, sggu=sggu or None)
    else:
        rows = await fetch_localdata_business(sido=sido, sggu=sggu or None, industry_keyword=None)
    if not rows:
        # 샘플 폴백
        st.info("API 키가 없거나 원천에서 데이터를 받지 못했습니다. 샘플 데이터로 데모를 진행합니다.")
        rows = pd.read_csv("sample_seed.csv").to_dict(orient="records")

    df = pd.DataFrame(rows)
    # 열 정리
    if "회사명" not in df.columns and "yadmNm" in df.columns:
        df = df.rename(columns={"yadmNm": "회사명"})
    if "주소" not in df.columns and "addr" in df.columns:
        df = df.rename(columns={"addr": "주소"})

    # 비고 컬럼 보강 (없으면 빈 문자열로 생성)
    if "비고" not in df.columns:
        df["비고"] = ""

    # 기본 컬럼 보강
    for col in ["회사명","주소","홈페이지","이메일"]:
        if col not in df.columns:
            df[col] = ""

    # 2단계: 홈페이지 탐색
    homepages = []
    for i, row in df.head(limit).iterrows():
        name = str(row.get("회사명","")).strip()
        addr = str(row.get("주소","")).strip()
        hp = str(row.get("홈페이지","") or "").strip()
        if not hp:
            q = f"{name} 공식 홈페이지 {sido} {sggu}".strip()
            hp = await find_homepage_via_bing(q)
        homepages.append(hp or "")
    df.loc[:len(homepages)-1, "홈페이지"] = homepages

    # 3단계: 이메일 추출
    emails_col = []
    for i, row in df.head(limit).iterrows():
        hp = row.get("홈페이지","")
        emails = []
        if hp:
            try:
                emails = await crawl_site_for_emails(hp, max_pages=5)
            except Exception:
                emails = []
        # 텍스트에서 자주 등장하는 변형도 탐지 (안전 접근)
        text_blob = str(row.get("비고", "") or "")
        if not emails and text_blob:
            emails = extract_emails_from_text(text_blob)

        if only_role:
            emails = [e for e in emails if any(e.lower().startswith(x) for x in ["info@","contact@","sales@","admin@","support@"])]
        emails_col.append(", ".join(dict.fromkeys(emails)))  # 중복 제거 유지
    df.loc[:len(emails_col)-1, "이메일"] = emails_col

    # 4단계: 결과 정리
    out = df[["회사명","주소","이메일","홈페이지"]].copy()
    out.insert(0, "지역", f"{sido} {sggu}".strip())
    out.insert(1, "업종", industry)
    out["수집일시"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return out

if run:
    with st.spinner("수집 중..."):
        out_df = asyncio.run(pipeline())
    st.success(f"완료. {len(out_df)} 행.")
    st.dataframe(out_df.head(50))
    buf = io.BytesIO()
    out_df.to_excel(buf, index=False)
    st.download_button("엑셀 내려받기", data=buf.getvalue(), file_name="scraped_companies.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

st.markdown("""
주의와 안내
- 의료기관은 건강보험심사평가원 OPEN API를 이용하며, 이용 안내와 코드 표는 해당 문서를 참고해야 합니다.
- 일반 업종은 지방행정 인허가 데이터 개방을 검토하여 각 카탈로그별 엔드포인트를 선택해 구현해야 합니다.
- 이메일은 공개된 대표 연락처만을 목표로 하며, 개인 식별 이메일은 수집 대상에서 제외하는 것이 바람직합니다.
""")
