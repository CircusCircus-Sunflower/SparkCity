# Import required libraries
import streamlit as st
import pandas as pd
import datetime

# Page Title
st.title("Spark City Dashboard (MVP FRAMEWORK)")

# Sidebar Navigation
menu = st.sidebar.selectbox(
    "Select Page",
    ["Overview", "Anomaly Detection", "Predictive Analytics"]
)

# Load Cleaned Dataset
try:
    # Correct path to the cleaned data file
    df = pd.read_csv("data/processed/traffic_sample_cleaned.csv")
    
    # Debugging output
    st.write("Dataset Columns:", df.columns)
    st.write("First Few Rows:")
    st.write(df.head())
    
    # Handle the Timestamp column
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])  # Parse timestamp column
    else:
        # Generate mock timestamps if no valid timestamp column exists
        df["timestamp"] = [
            datetime.datetime.now() + datetime.timedelta(hours=i) 
            for i in range(len(df))
        ]
        st.warning("Timestamp column missing in dataset. Mock timestamps added.")

    st.success("Dataset loaded successfully!")
except FileNotFoundError:
    st.error("Dataset file not found: data/processed/traffic_sample_cleaned.csv")
    df = pd.DataFrame()  # Fallback in case no data is found

# Pages
if menu == "Overview":
    # Overview Page
    st.header("Overview")
    st.write("City-Wide Statistics:")

    if not df.empty:
        st.write(df.describe())  # Summary statistics
        
        # Line Chart to Visualize data over time (use `count` or `speed`)
        st.line_chart(df.set_index("timestamp")[["count", "speed"]])  # Adjust columns
    else:
        st.warning("Data unavailable for Overview.")

elif menu == "Anomaly Detection":
    # Anomaly Detection Page
    st.header("Anomaly Detection")
    st.write("This page will show detected anomalies.")
    
    # Select column for anomaly detection
    anomaly_column = st.sidebar.selectbox("Select Column for Anomaly Detection:", ["count", "speed"])
    
    # Slider to set threshold
    threshold = st.sidebar.slider(
        f"Set {anomaly_column} Threshold", min_value=int(df[anomaly_column].min()), max_value=int(df[anomaly_column].max()), value=int(df[anomaly_column].mean())
    )
    st.write(f"Anomaly Threshold ({anomaly_column}): {threshold}")

    if not df.empty:
        # Filter anomalies based on the selected column and threshold
        anomalies = df[df[anomaly_column] > threshold]
        st.write("Detected Anomalies:", anomalies)

        # Bar Chart for the selected column
        st.bar_chart(df.set_index("timestamp")[anomaly_column])
    else:
        st.warning("Data unavailable for Anomaly Detection.")

elif menu == "Predictive Analytics":
    # Predictive Analytics Page
    st.header("Predictive Analytics")
    st.write("Forecasted Values:")

    if not df.empty:
        # Placeholder forecast logic (example using `speed`)
        forecasted_speed = df["speed"].mean() + 10  # Dummy forecast logic
        st.write(f"Predicted Average Speed for Tomorrow: {forecasted_speed:.2f}")
    else:
        st.write("Prediction data is not available yet!")
        st.warning("Awaiting future integration of forecasting models.")
