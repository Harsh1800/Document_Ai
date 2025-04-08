import streamlit as st
import os
import requests
from google.cloud import storage


# 🔹 Set Google Cloud authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Harshal Pagar\Desktop\Data-Extrator Trio\Doctrio_Project\document-ai-454812-868bb86365c6.json"

# 🔹 Cloud function URL
CLOUD_FUNCTION_URL = "https://us-central1-document-ai-454812.cloudfunctions.net/Document_Extractor"

# 🔹 Sidebar Logo Path (Replace with actual path)
sidebar_logo_path = r"........"
company_logo_path = r"........"  # Add a sidebar logo

# -------- CUSTOM CSS --------
st.markdown(
    """
    <style>
        body {
            font-family: 'Arial', sans-serif;
        }
        .main-title {
            color: #2a9d8f;
            text-align: center;
            font-size: 36px;
            font-weight: bold;
        }
        .sidebar-title {
            color: #264653;
            font-size: 24px; 
            font-weight: bold;
            margin-bottom: 10px;
        }
        .box {
            border: 3px solid #2a9d8f;
            border-radius: 12px;
            padding: 20px;
            text-align: center;
            margin-bottom: 15px;
            background: linear-gradient(135deg, #f8f9fa, #e0f2f1);
            font-size: 18px;
            font-weight: bold;
            box-shadow: 0px 4px 8px rgba(0, 0, 0, 0.2);
        }
        .center-logo {
            display: flex;
            justify-content: center;
            margin-bottom: 20px;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# -------- SIGN-IN PAGE --------
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.image(company_logo_path, width=120)
    st.title("🔐 Welcome to Financial Insights")
    st.subheader("Sign in to continue")

    username = st.text_input("Username", placeholder="Enter username")
    password = st.text_input("Password", placeholder="Enter password", type="password")

    if st.button("Sign In", use_container_width=True):
        if username == "Atgeir" and password == "docai":
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("❌ Invalid credentials. Try again.")
    st.stop()

def download_gcs_file(gcs_uri):
    if not gcs_uri.startswith("gs://"):
        return None
    bucket_name, blob_name = gcs_uri.replace("gs://", "").split("/", 1)
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.download_as_bytes()


# -------- MAIN DASHBOARD --------
st.sidebar.image(sidebar_logo_path, width=120)  # Add sidebar logo
st.sidebar.markdown("<div class='sidebar-title'>📂 Data Extractor</div>", unsafe_allow_html=True)
st.sidebar.markdown("---")

page = st.sidebar.radio("Navigation", ["Dashboard", "Logs"])

# Sidebar section for financial data downloads
st.sidebar.markdown("<div class='sidebar-title'>📥 Financial Data</div>", unsafe_allow_html=True)
json_download_btn = st.sidebar.container()
csv_download_btn = st.sidebar.container()
st.sidebar.markdown("---")

if st.sidebar.button("🚪 Logout", use_container_width=True):
    st.session_state.authenticated = False
    st.rerun()

# -------- DASHBOARD PAGE --------
if page == "Dashboard":
    st.markdown("<div class='main-title'>📊 Financial Data Dashboard</div>", unsafe_allow_html=True)

    uploaded_file = st.file_uploader("Drop your PDF file here", type=["pdf"])

    if uploaded_file:
        st.success("✅ File uploaded successfully!")
        gcs_pdf_uri = f"gs://data_extractors_input/{uploaded_file.name}"

        with st.spinner("Processing your document..."):
            response = requests.post(CLOUD_FUNCTION_URL, json={"pdf_gcs_uri": gcs_pdf_uri}, verify=True)

        if response.status_code == 200:
            result = response.json()
            calculated_metrics = result.get("Calculated Metrics", {})
            extracted_data = result.get("extracted_data", {})
            json_gcs_uri = result.get("json_gcs_uri", "")
            csv_gcs_uri = result.get("csv_gcs_uri", "")

            st.success("✅ Processing complete!")

            # Display financial overview
            st.subheader("📊 Financial Overview")
            for key, value in calculated_metrics.items():
                if value != "N/A":
                    st.markdown(f"<div class='box'>{key}: {value}</div>", unsafe_allow_html=True)

            # Show extracted data
            st.subheader("📄 Extracted Data")
            st.json(extracted_data)

            # Enable JSON & CSV Download - Keep buttons visible
            pdf_basename = uploaded_file.name.rsplit('.', 1)[0]

            if json_gcs_uri:
                json_data = download_gcs_file(json_gcs_uri)
                if json_data:
                    with json_download_btn:
                        st.download_button("📥 Download JSON File", json_data, f"{pdf_basename}.json", "application/json")

            if csv_gcs_uri:
                csv_data = download_gcs_file(csv_gcs_uri)
                if csv_data:
                    with csv_download_btn:
                        st.download_button("📥 Download CSV File", csv_data, f"{pdf_basename}.csv", "text/csv")

        else:
            st.error(f"❌ Error: {response.status_code}")
            st.json(response.json())
