import streamlit as st
from streamlit_image_zoom import image_zoom
from PIL import Image
import pandas as pd
import os
import requests
from io import BytesIO
from io import StringIO
from datetime import datetime
import re
import boto3
from dotenv import load_dotenv

load_dotenv()

# -------------- AWS + S3 Config ----------------
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)
BUCKET_NAME = os.getenv("S3_BUCKET")

def upload_csv_to_s3(local_path, s3_key):
    try:
        s3.upload_file(local_path, BUCKET_NAME, s3_key)
        print(f"✅ Uploaded {s3_key} to S3")
    except Exception as e:
        print(f"⚠ Failed to upload CSV to S3: {e}")

def download_csv_from_s3(s3_key):
    """Download CSV from S3 and return as a pandas DataFrame."""
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        return df
    except s3.exceptions.NoSuchKey:
        return pd.DataFrame(columns=["Original_Image", "Generated_Image", "Plausibility", "Date"])
    except Exception as e:
        print(f"⚠ Failed to download CSV from S3: {e}")
        return pd.DataFrame(columns=["Original_Image", "Generated_Image", "Plausibility", "Date"])

# ---------------- Streamlit Config ----------------
st.set_page_config(page_title="Image Annotation Tool")

# ---------------- Session State Setup ----------------
if "selected_task" not in st.session_state:
    from interface import app_selector
    app_selector()
    st.stop()
task = st.session_state.selected_task
CSV_FILE = f"project/{task}_annotations.csv"
S3_CSV_KEY = f"annotations/project/{task}_annotations.csv"

if "current_index" not in st.session_state:
    st.session_state.current_index = 0
if "selections" not in st.session_state:
    st.session_state.selections = {}
if "completed_sets" not in st.session_state:
    st.session_state.completed_sets = set()

# ---------------- Load URLs ----------------
from interface import get_image_sets
image_sets = get_image_sets()
if not image_sets:
    st.warning("No images available.")
    st.stop()
# Validate and reset current_index if out of bounds
if st.session_state.current_index >= len(image_sets):
    st.session_state.current_index = max(0, len(image_sets) - 1)

# ---------------- Image Fetching ----------------
@st.cache_data(show_spinner=False)
def load_image(url):
    try:
        response = requests.get(url, timeout=10)
        img = Image.open(BytesIO(response.content)).convert("RGB")
        return img
    except Exception:
        return None

# ---------------- CSV Saving ----------------
def save_selection_to_csv(original_url, generated_url, label):
    def extract_key(url):
        match = re.search(r"\.com/(.+?)(?:\?|$)", url)
        return match.group(1) if match else url

    original = extract_key(original_url)
    generated = extract_key(generated_url)

    data = {
        "Original_Image": original,
        "Generated_Image": generated,
        "Plausibility": label,
        "Date": datetime.now().strftime("%d-%m-%Y")
    }

    # Ensure project directory exists
    os.makedirs(os.path.dirname(CSV_FILE), exist_ok=True)

    # Load existing CSV from S3
    df = download_csv_from_s3(S3_CSV_KEY)

    # Append new data, avoiding duplicates
    new_row = pd.DataFrame([data])
    df = pd.concat([df, new_row], ignore_index=True).drop_duplicates(
        subset=["Original_Image", "Generated_Image"], keep="last"
    )

    # Save locally and upload to S3
    df.to_csv(CSV_FILE, index=False)
    upload_csv_to_s3(CSV_FILE, S3_CSV_KEY)

# ---------------- Counter for Fully Annotated Originals ----------------
def get_total_fully_annotated():
    df = download_csv_from_s3(S3_CSV_KEY)
    if df.empty:
        return 0
    try:
        counts = df.groupby("Original_Image")["Generated_Image"].count()
        fully_done = counts[counts == 5]  # Assumes 5 generated images per set
        return len(fully_done)
    except Exception:
        return 0

# ---------------- Navigation ----------------
def show_navigation():
    col1, col2, col3 = st.columns([1, 4, 1])
    with col1:
        if st.session_state.current_index == 0:
            if st.button("Back to Task Selector"):
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                st.rerun()
        else:
            if st.button("Previous"):
                st.session_state.current_index -= 1
                if st.session_state.current_index < 0:
                    st.session_state.current_index = 0
                st.rerun()

    with col3:
        if st.button("Next"):
            index = st.session_state.current_index
            current_set = image_sets[index]
            def extract_key(url):
                match = re.search(r"\.com/(.+?)(?:\?|$)", url)
                return match.group(1) if match else url
            original_key = extract_key(current_set["original"])
            df = download_csv_from_s3(S3_CSV_KEY)
            annotations = df[df["Original_Image"] == original_key]
            generated_keys = [extract_key(url) for url in current_set["generated"]]
            if len(annotations) == len(generated_keys):  # All 5 annotated
                st.session_state.completed_sets.add(index)
                if st.session_state.current_index < len(image_sets) - 1:
                    st.session_state.current_index += 1
                st.rerun()
            else:
                st.warning("⚠ Please annotate all 5 generated images before proceeding.")

# ---------------- Main View ----------------
def show_main_view():
    index = st.session_state.current_index
    current_set = image_sets[index]

    # Load existing annotations for this set from S3
    def extract_key(url):
        match = re.search(r"\.com/(.+?)(?:\?|$)", url)
        return match.group(1) if match else url
    original_key = extract_key(current_set["original"])
    df = download_csv_from_s3(S3_CSV_KEY)
    annotations = df[df["Original_Image"] == original_key]

    st.markdown("### Original Image")
    col1, col2, col3 = st.columns([1, 4, 1])
    with col2:
        original_img = load_image(current_set["original"])
        if original_img:
            ow, oh = original_img.size
            oh_scaled = int(oh * 400 / ow)
            image_zoom(original_img, size=(400, oh_scaled), zoom_factor=2.5)
        else:
            st.error("Could not load original image.")

    st.markdown("---")
    st.markdown("### Generated Images")
    for i, gen_url in enumerate(current_set["generated"]):
        with st.container():
            cols = st.columns([2, 3])
            with cols[0]:
                gen_img = load_image(gen_url)
                if gen_img:
                    gw, gh = gen_img.size
                    gh_scaled = int(gh * 300 / gw)
                    image_zoom(gen_img, size=(300, gh_scaled), zoom_factor=2.5)
                else:
                    st.warning(f"Could not load generated image {i+1}")
            with cols[1]:
                key = (index, i)
                gen_key = extract_key(gen_url)
                
                # Check if this generated image is already annotated
                existing_annotation = annotations[annotations["Generated_Image"] == gen_key]
                if not existing_annotation.empty:
                    selected = existing_annotation.iloc[0]["Plausibility"]
                    st.session_state.selections[key] = selected
                else:
                    selected = st.session_state.selections.get(key)

                margin_top = 60
                st.write(f"<div style='height: {margin_top}px;'></div>", unsafe_allow_html=True)

                c1, c2 = st.columns([1, 1])
                with c1:
                    label = "✔ Plausible" if selected == "Plausible" else "Plausible"
                    if st.button(label, key=f"p_{index}_{i}"):
                        st.session_state.selections[key] = "Plausible"
                        save_selection_to_csv(current_set["original"], gen_url, "Plausible")
                        st.rerun()
                with c2:
                    label = "✔ Implausible" if selected == "Implausible" else "Implausible"
                    if st.button(label, key=f"ip_{index}_{i}"):
                        st.session_state.selections[key] = "Implausible"
                        save_selection_to_csv(current_set["original"], gen_url, "Implausible")
                        st.rerun()

    show_navigation()

# ---------------- Run ----------------
st.title("Image Annotation Tool")
st.success(f"Annotated Images: {get_total_fully_annotated()}")

show_main_view()