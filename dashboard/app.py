import time
from io import StringIO
from typing import Dict

import boto3
import pandas as pd
import streamlit as st
import plotly.express as px


AWS_REGION = st.secrets.get("AWS_DEFAULT_REGION", "us-east-2")
DATABASE_NAME = st.secrets.get(
    "ATHENA_DATABASE_NAME",
    "calendly_marketing_insights",
)
ATHENA_OUTPUT_LOCATION = st.secrets.get(
    "ATHENA_OUTPUT_LOCATION",
    "s3://calendly-marketing-insights-guy-raw/athena-results/",
)


QUERIES: Dict[str, str] = {
    "combined_dashboard_kpis": f"""
        SELECT
            total_spend_usd,
            total_bookings,
            average_cost_per_booking_usd,
            channel_count,
            spend_date_count,
            booking_date_count,
            gold_processed_at
        FROM {DATABASE_NAME}.combined_dashboard_kpis
    """,
    "cpb_by_channel": f"""
        SELECT
            channel,
            total_spend_usd,
            booked_call_count,
            cost_per_booking_usd
        FROM {DATABASE_NAME}.cpb_by_channel
        ORDER BY cost_per_booking_usd ASC NULLS LAST
    """,
    "daily_cpb_by_channel": f"""
        SELECT
            metric_date,
            channel,
            total_spend_usd,
            booked_call_count,
            cost_per_booking_usd
        FROM {DATABASE_NAME}.daily_cpb_by_channel
        ORDER BY metric_date, channel
    """,
    "daily_calls_by_source": f"""
        SELECT
            booking_date,
            channel,
            booked_call_count
        FROM {DATABASE_NAME}.daily_calls_by_source
        ORDER BY booking_date, channel
    """,
    "booking_trends": f"""
        SELECT
            booking_date,
            booked_call_count
        FROM {DATABASE_NAME}.booking_trends
        ORDER BY booking_date
    """,
    "channel_attribution": f"""
        SELECT
            channel,
            utm_campaign,
            booked_call_count
        FROM {DATABASE_NAME}.channel_attribution
        ORDER BY booked_call_count DESC, channel, utm_campaign
    """,
    "booking_volume_by_time_slot": f"""
        SELECT
            meeting_day_of_week,
            meeting_hour,
            booked_call_count
        FROM {DATABASE_NAME}.booking_volume_by_time_slot
        ORDER BY booked_call_count DESC, meeting_day_of_week, meeting_hour
    """,
    "employee_meeting_load": f"""
        SELECT
            employee_id,
            employee_name,
            employee_email,
            booked_call_count
        FROM {DATABASE_NAME}.employee_meeting_load
        ORDER BY booked_call_count DESC, employee_name
    """,
}


FRIENDLY_COLUMN_NAMES = {
    "channel": "Channel",
    "total_spend_usd": "Total Spend",
    "booked_call_count": "Booked Calls",
    "cost_per_booking_usd": "Cost per Booking",
    "metric_date": "Metric Date",
    "booking_date": "Booking Date",
    "utm_campaign": "UTM Campaign",
    "meeting_day_of_week": "Meeting Day",
    "meeting_hour": "Meeting Hour",
    "employee_id": "Employee ID",
    "employee_name": "Employee Name",
    "employee_email": "Employee Email",
    "gold_processed_at": "Gold Processed At",
    "total_bookings": "Total Bookings",
    "average_cost_per_booking_usd": "Average CPB",
    "channel_count": "Channel Count",
    "spend_date_count": "Spend Date Count",
    "booking_date_count": "Booking Date Count",
}


FRIENDLY_CHANNEL_NAMES = {
    "facebook_paid_ads": "Facebook Paid Ads",
    "youtube_paid_ads": "YouTube Paid Ads",
    "tiktok_paid_ads": "TikTok Paid Ads",
}


def make_chart_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Return a copy of a DataFrame with dashboard-friendly column names
    and friendly channel values for chart display.
    """
    display_df = dataframe.copy()

    if "channel" in display_df.columns:
        display_df["channel"] = display_df["channel"].replace(FRIENDLY_CHANNEL_NAMES)

    return display_df.rename(columns=FRIENDLY_COLUMN_NAMES)


def make_display_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Return a copy of a DataFrame with dashboard-friendly column names.
    """
    return dataframe.rename(columns=FRIENDLY_COLUMN_NAMES)


def get_boto3_client(service_name: str):
    """
    Create a boto3 client.

    Locally, this can use AWS_PROFILE / ~/.aws credentials.
    On Streamlit Community Cloud, this uses secrets from the app settings.
    """
    aws_access_key_id = st.secrets.get("AWS_ACCESS_KEY_ID", None)
    aws_secret_access_key = st.secrets.get("AWS_SECRET_ACCESS_KEY", None)
    aws_region = st.secrets.get("AWS_DEFAULT_REGION", AWS_REGION)

    if aws_access_key_id and aws_secret_access_key:
        return boto3.client(
            service_name,
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    return boto3.client(service_name, region_name=AWS_REGION)


def get_athena_client():
    return get_boto3_client("athena")


def get_s3_client():
    return get_boto3_client("s3")


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """
    Convert s3://bucket/key into bucket and key.
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {s3_uri}")

    path = s3_uri.replace("s3://", "", 1)
    bucket, key = path.split("/", 1)

    return bucket, key


def wait_for_query(query_execution_id: str, poll_interval_seconds: float = 1.0) -> None:
    """
    Wait for an Athena query to complete or fail.
    """
    athena = get_athena_client()

    while True:
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = response["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            return

        if status in {"FAILED", "CANCELLED"}:
            reason = response["QueryExecution"]["Status"].get(
                "StateChangeReason",
                "No failure reason provided.",
            )
            raise RuntimeError(
                f"Athena query {query_execution_id} ended with status "
                f"{status}: {reason}"
            )

        time.sleep(poll_interval_seconds)


@st.cache_data(ttl=300, show_spinner=False)
def run_athena_query(sql: str) -> pd.DataFrame:
    """
    Run an Athena query and return the result as a pandas DataFrame.

    Results are cached for 5 minutes to avoid rerunning every query on every
    Streamlit interaction.
    """
    athena = get_athena_client()
    s3 = get_s3_client()

    start_response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE_NAME},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
    )

    query_execution_id = start_response["QueryExecutionId"]

    wait_for_query(query_execution_id)

    query_response = athena.get_query_execution(QueryExecutionId=query_execution_id)
    output_location = query_response["QueryExecution"]["ResultConfiguration"][
        "OutputLocation"
    ]

    bucket, key = parse_s3_uri(output_location)

    result_object = s3.get_object(Bucket=bucket, Key=key)
    result_csv = result_object["Body"].read().decode("utf-8")

    return pd.read_csv(StringIO(result_csv))


def format_currency(value) -> str:
    if pd.isna(value):
        return "N/A"

    return f"${float(value):,.2f}"


def format_integer(value) -> str:
    if pd.isna(value):
        return "0"

    return f"{int(value):,}"


def load_dashboard_data() -> Dict[str, pd.DataFrame]:
    return {
        name: run_athena_query(query)
        for name, query in QUERIES.items()
    }


def main() -> None:
    st.set_page_config(
        page_title="Calendly Marketing Insights",
        page_icon="📈",
        layout="wide",
    )

    st.title("Calendly Marketing Insights Dashboard")
    st.caption(
        "Gold-layer metrics from AWS Glue, Delta Lake, Glue Data Catalog, and Athena."
    )

    if st.button("Refresh dashboard data"):
        st.cache_data.clear()
        st.rerun()

    with st.spinner("Loading Gold metrics from Athena..."):
        data = load_dashboard_data()

    kpis_df = data["combined_dashboard_kpis"]

    if kpis_df.empty:
        st.error("No dashboard KPI data found.")
        return

    kpis = kpis_df.iloc[0]

    st.subheader("Executive KPI Summary")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Total Spend",
        format_currency(kpis["total_spend_usd"]),
    )
    col2.metric(
        "Total Bookings",
        format_integer(kpis["total_bookings"]),
    )
    col3.metric(
        "Average CPB",
        format_currency(kpis["average_cost_per_booking_usd"]),
    )
    col4.metric(
        "Channels",
        format_integer(kpis["channel_count"]),
    )

    st.caption(
        f"Spend days: {format_integer(kpis['spend_date_count'])} | "
        f"Booking days: {format_integer(kpis['booking_date_count'])} | "
        f"Gold processed at: {kpis.get('gold_processed_at', 'N/A')}"
    )

    st.divider()

    st.subheader("Cost per Booking by Channel")

    cpb_by_channel_df = data["cpb_by_channel"].copy()

    st.dataframe(
        make_display_dataframe(cpb_by_channel_df),
        use_container_width=True,
        hide_index=True,
    )

    cpb_chart_df = cpb_by_channel_df.dropna(subset=["cost_per_booking_usd"])

    cpb_chart_display_df = make_chart_dataframe(cpb_chart_df)

    if not cpb_chart_df.empty:
        cpb_fig = px.bar(
            cpb_chart_display_df,
            x="Channel",
            y="Cost per Booking",
            text="Cost per Booking",
        )

        cpb_fig.update_layout(
            xaxis_title="Channel",
            yaxis_title="Cost per Booking",
            margin=dict(l=120, r=40, t=30, b=120),
            height=500,
        )

        cpb_fig.update_traces(
            texttemplate="$%{text:,.2f}",
            textposition="outside",
        )

        st.plotly_chart(
            cpb_fig,
            use_container_width=True,
        )

    st.divider()

    st.subheader("Daily Calls Booked by Source")

    daily_calls_df = data["daily_calls_by_source"].copy()
    daily_calls_chart_df = daily_calls_df.copy()
    daily_calls_chart_df["channel"] = daily_calls_chart_df["channel"].replace(
        FRIENDLY_CHANNEL_NAMES
    )

    if not daily_calls_df.empty:
        daily_calls_pivot_df = daily_calls_chart_df.pivot_table(
            index="booking_date",
            columns="channel",
            values="booked_call_count",
            aggfunc="sum",
            fill_value=0,
        )

        daily_calls_pivot_df.index.name = "Booking Date"

        st.line_chart(daily_calls_pivot_df)

    st.dataframe(
        make_display_dataframe(daily_calls_df),
        use_container_width=True,
        hide_index=True,
    )

    st.divider()

    st.subheader("Booking Trend Over Time")

    booking_trends_df = data["booking_trends"].copy()

    if not booking_trends_df.empty:
        trend_chart_df = booking_trends_df.set_index("booking_date")
        st.line_chart(trend_chart_df["booked_call_count"])

    st.dataframe(
        make_display_dataframe(booking_trends_df),
        use_container_width=True,
        hide_index=True,
    )

    st.divider()

    st.subheader("Daily Cost per Booking by Channel")

    daily_cpb_df = data["daily_cpb_by_channel"].copy()

    if not daily_cpb_df.empty:
        daily_cpb_chart_df = daily_cpb_df.dropna(
            subset=["cost_per_booking_usd"]
        ).copy()

        if not daily_cpb_chart_df.empty:
            daily_cpb_chart_df["channel"] = daily_cpb_chart_df["channel"].replace(
                FRIENDLY_CHANNEL_NAMES
            )

            daily_cpb_pivot_df = daily_cpb_chart_df.pivot_table(
                index="metric_date",
                columns="channel",
                values="cost_per_booking_usd",
                aggfunc="mean",
            )

            daily_cpb_pivot_df.index.name = "Metric Date"

            st.line_chart(daily_cpb_pivot_df)

    st.dataframe(
        make_display_dataframe(daily_cpb_df),
        use_container_width=True,
        hide_index=True,
    )

    st.divider()

    st.subheader("Channel Attribution")

    channel_attribution_df = data["channel_attribution"].copy()

    st.dataframe(
        make_display_dataframe(channel_attribution_df),
        use_container_width=True,
        hide_index=True,
    )

    st.divider()

    st.subheader("Booking Volume by Time Slot")

    time_slot_df = data["booking_volume_by_time_slot"].copy()

    st.dataframe(
        make_display_dataframe(time_slot_df),
        use_container_width=True,
        hide_index=True,
    )

    st.divider()

    st.subheader("Meeting Load per Employee")

    employee_load_df = data["employee_meeting_load"].copy()

    st.dataframe(
        make_display_dataframe(employee_load_df),
        use_container_width=True,
        hide_index=True,
    )

    if not employee_load_df.empty:
        employee_chart_df = make_chart_dataframe(
            employee_load_df[["employee_name", "booked_call_count"]]
        )
        st.bar_chart(
            employee_chart_df,
            x="Employee Name",
            y="Booked Calls",
        )


if __name__ == "__main__":
    main()