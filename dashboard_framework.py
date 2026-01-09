# Istalled st & pd
import streamlit as st
import pandas as pd

# Page Title
st. title("Spark City Dashboard (MVP FRAMEWORK)")

# Sidebar Navigation
menu = st.sidebar.selectbox(
    "Select Page",
    ["Overview", "Anomaly Detection", "Prediction Analytics"]
)    

# Pages
if menu == "Overview":
    st.header("Overview")
    st.write("This page will display city-wide stats and general information.")

elif menu == "Anomaly Detection":
    st.header("Anomaly Detection")
    st.write("This page will show detected anomalies.")
    
    # PM2.5 Threshold Slider in Sidebar
    threshold = st.sidebar.slider("Set PM2.5 Threshold", min_value=50, max_value=300, value=150)
    st.write(f"Anomaly Threshold: {threshold}")

    # Mock Data for Anomalies
    mock_data = pd.DataFrame({
        "Hour": ["9 AM", "10 AM", "11 AM", "12 PM", "1 PM"],
        "PM2.5": [151, 290, 180, 85, 210],
    })

    # Filter Anomalies Based on Threshold
    anomalies = mock_data[mock_data["PM2.5"] > threshold]
    st.write("Detected Anomalies", anomalies)
    
    # Bar Chart for PM2.5 Levels
    st.bar_chart(mock_data.set_index("Hour"))

elif menu == "Predictive Analytics":
    st.header("Predictive Analytics")
    st.write("This page will show forecasted trends.")
    # Placeholder for predictions
    st.write("Predicted PM2.5 for tomorrow: 180 (Moderate Air Quality)")
    