import streamlit as st
import pandas as pd
import os
from azure.storage.blob import BlobServiceClient
import pyarrow.parquet as pq
import altair as alt
import datetime

st.set_page_config(page_title="🚖 Ride Analytics", layout="wide")
st.title("🚖 Real-Time Ride-Hailing Analytics")

# Sidebar: Navigation
section = st.sidebar.radio("📂 Analysis Sections", ["🔴 Anomalies", "💸 Prices", "📍 Zones", "❌ Cancellations"])

# Azure Blob Configuration
ACCOUNT_NAME = "iesstsabbadbaa"
ACCOUNT_KEY = "ZT6z+TYSxF0Xdm0vOCRbIpWoBss2BxOU0EcP2UDceddHX7Kyi8gyJvjyWG5THNp2HOprCHmblb2f+AStp8mAGw=="
CONTAINER_NAME = "streamed-data-group1"

def load_parquet_from_blob(blob_prefix, local_dir):
    blob_service_client = BlobServiceClient(f"https://{ACCOUNT_NAME}.blob.core.windows.net", credential=ACCOUNT_KEY)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    os.makedirs(local_dir, exist_ok=True)
    for blob in container_client.list_blobs(name_starts_with=blob_prefix):
        if blob.name.endswith(".parquet"):
            local_path = os.path.join(local_dir, os.path.basename(blob.name))
            if not os.path.exists(local_path):
                with open(local_path, "wb") as f:
                    f.write(container_client.get_blob_client(blob.name).download_blob().readall())
    return [os.path.join(local_dir, f) for f in os.listdir(local_dir) if f.endswith(".parquet")]

# Load datasets
ride_files = load_parquet_from_blob("rides-output", "./rides_dl")
request_files = load_parquet_from_blob("requests-output", "./requests_dl")
df_rides = pd.concat([pd.read_parquet(f) for f in ride_files], ignore_index=True) if ride_files else pd.DataFrame()
df_requests = pd.concat([pd.read_parquet(f) for f in request_files], ignore_index=True) if request_files else pd.DataFrame()

df_rides["timestamp"] = pd.to_datetime(df_rides["request_time"], unit="s")
df_requests["timestamp"] = pd.to_datetime(df_requests["timestamp"], unit="s", errors="coerce")

# Sidebar filters
st.sidebar.header("📊 Filters")
zones = st.sidebar.multiselect("Pickup zone", df_rides["pickup_location"].unique(), default=df_rides["pickup_location"].unique())
vehicle_types = st.sidebar.multiselect("Vehicle type", df_rides["vehicle_type"].unique(), default=df_rides["vehicle_type"].unique())
start_hour, end_hour = st.sidebar.slider("⏱️ Hour range", min_value=datetime.time(0, 0), max_value=datetime.time(23, 59),
                                         value=(datetime.time(0, 0), datetime.time(23, 59)), step=datetime.timedelta(minutes=15))

df_rides = df_rides[df_rides["pickup_location"].isin(zones)]
df_rides = df_rides[df_rides["vehicle_type"].isin(vehicle_types)]
df_rides = df_rides[df_rides["timestamp"].dt.time.between(start_hour, end_hour)]

# KPIs visible on all pages
st.subheader("📊 Global KPIs")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Rides", len(df_rides))
col2.metric("Completed", len(df_rides[df_rides["ride_status"] == "completed"]))
col3.metric("Cancelled", len(df_rides[df_rides["ride_status"] == "cancelled"]))
col4.metric("Avg Duration (s)", round(df_rides["ride_duration"].mean(), 2))
df_rides["response_time"] = df_rides["pickup_time"] - df_rides["request_time"]
st.metric("Avg Driver Response Time (s)", round(df_rides["response_time"].mean(), 2))

# 🔴 Anomalies
if section == "🔴 Anomalies":
    st.subheader("🔴 Price Anomalies")
    q1, q3 = df_rides["price"].quantile([0.25, 0.75])
    iqr = q3 - q1
    threshold = q3 + 1.5 * iqr
    anomalies = df_rides[df_rides["price"] > threshold]
    st.write(f"🚨 {len(anomalies)} rides with price above {threshold:.2f}")
    st.dataframe(anomalies[["ride_id", "price", "pickup_location", "dropoff_location"]])

    st.subheader("🕒 Duration Anomalies")
    q1_dur, q3_dur = df_rides["ride_duration"].quantile([0.25, 0.75])
    iqr_dur = q3_dur - q1_dur
    threshold_dur = q3_dur + 1.5 * iqr_dur
    duration_anomalies = df_rides[df_rides["ride_duration"] > threshold_dur]
    st.write(f"⏱️ {len(duration_anomalies)} rides with abnormal duration (>{threshold_dur:.2f} seconds)")
    st.dataframe(duration_anomalies[["ride_id", "ride_duration", "pickup_location", "dropoff_location"]])

    st.subheader("⚠️ Short Rides with High Prices")
    df_suspicious = df_rides[(df_rides["ride_duration"] < 60) & (df_rides["price"] > 20)]
    st.write(f"⚠️ {len(df_suspicious)} short & expensive rides (possible fraud)")
    st.dataframe(df_suspicious[["ride_id", "ride_duration", "price", "pickup_location", "dropoff_location"]])

    st.subheader("🗺️ Zones with Most Price Anomalies")
    anomalies["is_anomaly"] = True
    zone_anomalies = anomalies.groupby("pickup_location").size().reset_index(name="count")
    st.altair_chart(
        alt.Chart(zone_anomalies).mark_bar().encode(
            x="pickup_location",
            y="count",
            color=alt.value("#d62728")
        ),
        use_container_width=True
    )

    st.subheader("📈 Deviation from Expected Price")
    df_rides["expected_price"] = df_rides["distance"] * 1.5 + 3
    df_rides["price_diff"] = df_rides["price"] - df_rides["expected_price"]
    df_outliers = df_rides[df_rides["price_diff"].abs() > 50]
    st.write(f"🔎 {len(df_outliers)} rides with > €50 deviation from expected price")
    st.dataframe(df_outliers[["ride_id", "price", "expected_price", "price_diff"]])

# 💸 Prices
elif section == "💸 Prices":
    st.subheader("💰 Price Distribution")
    max_price = min(300, df_rides["price"].max())
    price_hist = alt.Chart(df_rides[df_rides["price"] <= max_price]).mark_bar().encode(
        alt.X("price", bin=alt.Bin(maxbins=40), title="Ride Price"),
        alt.Y("count()", title="Ride Count")
    ).properties(height=300)
    st.altair_chart(price_hist, use_container_width=True)

    st.subheader("💸 Hourly Price Stats (Min, Mean, Max)")
    df_valid_price = df_rides[df_rides["price"] > 0]
    price_hourly_stats = df_valid_price.groupby(pd.Grouper(key="timestamp", freq="1H")).agg(
        avg_price=("price", "mean"),
        min_price=("price", "min"),
        max_price=("price", "max")
    ).reset_index()

    show_avg = st.checkbox("Show average price", value=True)
    show_min = st.checkbox("Show minimum price", value=False)
    show_max = st.checkbox("Show maximum price", value=False)

    base = alt.Chart(price_hourly_stats).encode(x="timestamp:T")
    layers = []
    if show_avg:
        layers.append(base.mark_line(color="steelblue").encode(y="avg_price:Q"))
    if show_min:
        layers.append(base.mark_line(color="green").encode(y="min_price:Q"))
    if show_max:
        layers.append(base.mark_line(color="red").encode(y="max_price:Q"))

    if layers:
        st.altair_chart(alt.layer(*layers).properties(height=300), use_container_width=True)
    else:
        st.info("Please select at least one metric to display the chart.")

    if not df_requests.empty and "payment_type" in df_requests.columns:
        st.subheader("💳 Payment Methods (from Requests)")
        payment = df_requests["payment_type"].dropna().value_counts().reset_index()
        payment.columns = ["Method", "Count"]
        st.altair_chart(alt.Chart(payment).mark_bar().encode(x="Method", y="Count", color="Method"), use_container_width=True)

# 📍 Zones
elif section == "📍 Zones":
    st.subheader("🔥 Heatmap: Rides by Hour and Zone")
    df_rides["hour_num"] = df_rides["timestamp"].dt.hour
    heatmap = df_rides.groupby(["pickup_location", "hour_num"]).size().reset_index(name="rides")
    st.altair_chart(alt.Chart(heatmap).mark_rect().encode(
        x="hour_num:O", y="pickup_location:O", color="rides:Q"
    ), use_container_width=True)

    st.subheader("🟡 Cancellation Rate by Zone")
    cancel_rate = df_rides[df_rides["ride_status"] == "cancelled"].groupby("pickup_location").size().div(df_rides.groupby("pickup_location").size()).fillna(0).reset_index(name="cancel_rate")
    st.altair_chart(alt.Chart(cancel_rate).mark_bar().encode(x="pickup_location", y="cancel_rate", color="pickup_location"), use_container_width=True)

    st.subheader("📊 Ride Comparison by Zone")
    completed = df_rides[df_rides["ride_status"] == "completed"].groupby("pickup_location").size().reset_index(name="Completed")
    cancelled = df_rides[df_rides["ride_status"] == "cancelled"].groupby("pickup_location").size().reset_index(name="Cancelled")
    merged = completed.merge(cancelled, on="pickup_location", how="outer").fillna(0)
    melted = merged.melt(id_vars="pickup_location", var_name="Status", value_name="Count")
    chart = alt.Chart(melted).mark_bar().encode(
        x="pickup_location:N",
        y="Count:Q",
        color=alt.Color("Status:N", scale=alt.Scale(domain=["Completed", "Cancelled"], range=["#00913F", "#c0392b"])),
        tooltip=["pickup_location", "Status", "Count"]
    ).properties(height=400)
    st.altair_chart(chart, use_container_width=True)

    st.subheader("📍 Most Active Zones")
    top_pickups = df_rides["pickup_location"].value_counts().reset_index()
    top_pickups.columns = ["location", "count"]
    top_dropoffs = df_rides["dropoff_location"].value_counts().reset_index()
    top_dropoffs.columns = ["location", "count"]
    st.write("🔼 Top Pickup Zones")
    st.altair_chart(alt.Chart(top_pickups).mark_bar().encode(x="location", y="count"), use_container_width=True)
    st.write("🔽 Top Dropoff Zones")
    st.altair_chart(alt.Chart(top_dropoffs).mark_bar().encode(x="location", y="count"), use_container_width=True)

# ❌ Cancellations
elif section == "❌ Cancellations":
    st.subheader("❌ Cancellations per Hour")
    cancel_by_hour = df_rides[df_rides["ride_status"] == "cancelled"].copy()
    cancel_by_hour["hour"] = cancel_by_hour["timestamp"].dt.hour
    cancel_hist = cancel_by_hour.groupby("hour").size().reset_index(name="cancelled")
    st.altair_chart(alt.Chart(cancel_hist).mark_bar().encode(x="hour:O", y="cancelled:Q"), use_container_width=True)

    st.subheader("📉 Hourly Cancellation Rate")
    df_rides["hour"] = df_rides["timestamp"].dt.hour
    total_per_hour = df_rides.groupby("hour").size().reset_index(name="total")
    cancel_per_hour = df_rides[df_rides["ride_status"] == "cancelled"].groupby("hour").size().reset_index(name="cancelled")
    cancel_hourly = pd.merge(total_per_hour, cancel_per_hour, on="hour", how="left").fillna(0)
    cancel_hourly["cancel_rate"] = cancel_hourly["cancelled"] / cancel_hourly["total"]
    chart = alt.Chart(cancel_hourly).mark_line(point=True).encode(
        x="hour:O",
        y="cancel_rate:Q",
        tooltip=["hour", "cancel_rate"]
    ).properties(height=300)
    st.altair_chart(chart, use_container_width=True)
