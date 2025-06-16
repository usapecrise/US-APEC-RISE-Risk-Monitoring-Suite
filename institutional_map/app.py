import streamlit as st
import pandas as pd
import networkx as nx
from pyvis.network import Network
import streamlit.components.v1 as components
import os

st.set_page_config(page_title="Institutional Mapping Tool", layout="wide")
st.title("US APEC-RISE Institutional Mapping Tool")

# Upload or load data
uploaded_file = st.file_uploader("Upload your Institutional Mapping CSV", type="csv")

if uploaded_file:
    df = pd.read_csv(uploaded_file)
elif os.path.exists("sample_institutional_mapping.csv"):
    df = pd.read_csv("sample_institutional_mapping.csv")
    st.success("Loaded sample dataset.")
else:
    st.warning("Please upload a CSV file to begin.")
    st.stop()

# Filter
economies = ["All"] + sorted(df["Economy"].unique())
selected_economy = st.selectbox("Filter by Economy", options=economies)

if selected_economy != "All":
    df = df[df["Economy"] == selected_economy]

# Optional time filter (if time column exists)
if "Time Period" in df.columns:
    time_periods = ["All"] + sorted(df["Time Period"].dropna().unique())
    selected_time = st.selectbox("Filter by Time Period", options=time_periods)
    if selected_time != "All":
        df = df[df["Time Period"] == selected_time]

# Role-based color map
role_colors = {
    "Coordinator": "#1f77b4",
    "Regulator": "#ff7f0e",
    "Private Sector Partner": "#2ca02c",
    "Funding Authority": "#d62728",
    "Technical Lead": "#9467bd"
}

# Build graph
G = nx.Graph()
for _, row in df.iterrows():
    color = role_colors.get(row["Role"], "#7f7f7f")
    G.add_node(
        row["Institution"],
        title=f"Role: {row['Role']}<br>Economy: {row['Economy']}<br>Theme(s): {row['Reform Theme(s)']}",
        size=row["Influence Score"] * 10,
        color=color
    )
    if pd.notna(row["Linkages"]):
        for target in row["Linkages"].split(','):
            G.add_edge(row["Institution"], target.strip())

# Create Pyvis network graph
net = Network(height="600px", width="100%", bgcolor="#f8f9fa", font_color="black")
net.from_nx(G)
net.repulsion(node_distance=200, spring_length=200)
net.show_buttons(filter_=['physics'])  # Add button panel for download/interaction
net.save_graph("graph.html")

# Display the graph
with open("graph.html", "r", encoding="utf-8") as f:
    html = f.read()
components.html(html, height=650, scrolling=True)

# Show raw data
with st.expander("View Data Table"):
    st.dataframe(df)
