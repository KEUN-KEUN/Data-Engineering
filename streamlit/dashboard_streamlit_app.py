# import streamlit as st
# from kafka import KafkaConsumer
# import json
# import pandas as pd
#
# st.set_page_config(page_title="Kafka 대시보드", layout="wide")
# st.title("🔥 Kafka 실시간 인기 상품 분석")
#
# def consume_messages(max_messages=50):
#     consumer = KafkaConsumer(
#         "popular-products-by-age",
#         bootstrap_servers="localhost:9094",
#         value_deserializer=lambda m: json.loads(m.decode("utf-8")),
#         auto_offset_reset="earliest",
#         consumer_timeout_ms=2000,  # 🔥 이거 꼭 넣어야 함: 2초 기다리고 종료
#         enable_auto_commit=False
#     )
#     messages = []
#     try:
#         for i, msg in enumerate(consumer):
#             messages.append(msg.value)
#             print("📦 Kafka Message:", msg.value)
#             if i >= max_messages - 1:
#                 break
#     except Exception as e:
#         print("❌ Kafka consume 에러:", e)
#     finally:
#         consumer.close()
#     return messages
#
# # 버튼으로 수동 트리거
# if st.button("🔄 Kafka 데이터 불러오기"):
#     with st.spinner("Kafka에서 데이터를 수신 중..."):
#         data = consume_messages()
#         if not data:
#             st.warning("⚠️ Kafka에서 데이터를 받지 못했습니다.")
#         else:
#             df = pd.DataFrame(data)
#             st.subheader("📋 수신된 Kafka 메시지")
#             st.dataframe(df)
#
#             st.subheader("📊 상품별 클릭 수 합계 (product_id 기준)")
#             click_sum = df.groupby("product_id")["click_count"].sum().reset_index()
#             click_sum = click_sum.sort_values(by="click_count", ascending=False)
#             st.bar_chart(click_sum.set_index("product_id"))
# else:
#     st.info("👆 버튼을 눌러 Kafka 데이터를 불러오세요.")

import streamlit as st
import requests
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
import json

st.set_page_config(page_title="시스템 로그 & Kafka 대시보드", layout="wide")
st.title("🔥 시스템 로그 분석 & Kafka 실시간 인기 상품 분석")

option = st.selectbox(
    "데이터 소스 선택",
    ("시스템 로그 분석 결과", "Kafka 실시간 인기 상품")
)

def fetch_analysis():
    url = "http://localhost:33333/system-log/analysis"
    response = requests.post(url)
    if response.status_code == 201:
        return response.json()
    else:
        st.error(f"API 호출 실패: {response.status_code}")
        return None

def consume_messages(max_messages=50):
    consumer = KafkaConsumer(
        "popular-products-by-age",
        bootstrap_servers="localhost:9094",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        consumer_timeout_ms=2000,
        enable_auto_commit=False
    )
    messages = []
    try:
        for i, msg in enumerate(consumer):
            messages.append(msg.value)
            if i >= max_messages - 1:
                break
    except Exception as e:
        st.error(f"Kafka consume 에러: {e}")
    finally:
        consumer.close()
    return messages

if option == "시스템 로그 분석 결과":
    st.header("시스템 로그 분석 결과")

    if st.button("분석 데이터 가져오기"):
        data = fetch_analysis()
        if data and data.get("success"):
            content = data["data"]

            # 3. 사용자별 평균 duration
            user_avg_df = pd.DataFrame(content["user_avg"])
            st.subheader("3. 사용자별 평균 duration")
            plt.figure(figsize=(10, 5))
            sns.barplot(x="user_id", y="duration_ms", data=user_avg_df)
            plt.title("User-wise Average Duration (ms)")
            plt.ylabel("Avg Duration (ms)")
            st.pyplot(plt)
            plt.clf()

            # 시간대별 요청 수 (데이터 없으므로 생략 또는 안전 처리)
            if "hourly_requests" in content:
                hourly_requests_df = pd.DataFrame(content["hourly_requests"])
                st.subheader("4. 시간대별 요청 수")
                plt.figure(figsize=(10, 5))
                sns.barplot(data=hourly_requests_df, x='hour', y='count', palette='coolwarm')
                plt.title("Request Count per Hour")
                plt.xlabel("Hour of Day")
                plt.ylabel("Number of Requests")
                st.pyplot(plt)
                plt.clf()
            else:
                st.info("시간대별 요청 수 데이터가 없습니다.")

            # 사용자 vs 액션 수 Heatmap (데이터 없으므로 생략 또는 안전 처리)
            if "action_counts" in content:
                pivot_table = pd.pivot_table(pd.DataFrame(content["action_counts"]), index='user_id', columns='action',
                                             aggfunc='size', fill_value=0)
                st.subheader("5. Heatmap: 사용자 vs 액션 수")
                plt.figure(figsize=(8, 6))
                sns.heatmap(pivot_table, annot=True, cmap='YlGnBu')
                plt.title("User vs Action Frequency")
                st.pyplot(plt)
                plt.clf()

            # 이상 행동 출력
            anomalies_df = pd.DataFrame(content["anomalies"])
            if not anomalies_df.empty:
                st.subheader("🚨 이상 행동 기록")
                st.dataframe(anomalies_df)
            else:
                st.info("이상 행동이 발견되지 않았습니다.")

        else:
            st.warning("분석 데이터를 불러오지 못했습니다.")
    else:
        st.info("분석 데이터를 불러오려면 버튼을 눌러주세요.")

elif option == "Kafka 실시간 인기 상품":
    st.header("Kafka 실시간 인기 상품 분석")

    if st.button("🔄 Kafka 데이터 불러오기"):
        with st.spinner("Kafka에서 데이터를 수신 중..."):
            data = consume_messages()
            if not data:
                st.warning("⚠️ Kafka에서 데이터를 받지 못했습니다.")
            else:
                df = pd.DataFrame(data)
                st.subheader("📋 수신된 Kafka 메시지")
                st.dataframe(df)

                st.subheader("📊 상품별 클릭 수 합계 (product_id 기준)")
                click_sum = df.groupby("product_id")["click_count"].sum().reset_index()
                click_sum = click_sum.sort_values(by="click_count", ascending=False)
                st.bar_chart(click_sum.set_index("product_id"))
    else:
        st.info("👆 버튼을 눌러 Kafka 데이터를 불러오세요.")
