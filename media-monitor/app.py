import streamlit as st
import os
import importlib.util

# Helper to load a module by filepath under a unique name
def load_app_module(name, filepath):
    spec = importlib.util.spec_from_file_location(name, filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# Build absolute paths to each app.py
base = os.path.dirname(__file__)
media_path    = os.path.join(base, "media-monitor",    "app.py")
scenario_path = os.path.join(base, "scenario-simulator","app.py")
map_path      = os.path.join(base, "institutional-map","app.py")

# Load each as a distinct module
media_app    = load_app_module("media_app",    media_path)
scenario_app = load_app_module("scenario_app", scenario_path)
map_app      = load_app_module("map_app",      map_path)

# Streamlit page config
st.set_page_config(page_title="APEC-RISE Monitoring Suite", layout="wide")
st.title("🧠 APEC-RISE Monitoring Suite")
st.markdown("""
Welcome to the **U.S. APEC-RISE Monitoring Suite**.  
Use the tabs below to switch between your three integrated tools.
""")

# Tabs
tab1, tab2, tab3 = st.tabs([
    "📡 Media Monitor",
    "🧭 Scenario Simulator",
    "🏛 Institutional Map"
])

with tab1:
    st.subheader("📡 Media Monitor")
    media_app.main()

with tab2:
    st.subheader("🧭 Scenario Simulator")
    scenario_app.main()

with tab3:
    st.subheader("🏛 Institutional Mapping Tool")
    map_app.main()
