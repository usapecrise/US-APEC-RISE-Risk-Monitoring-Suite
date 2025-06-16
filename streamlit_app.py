# streamlit_app.py (at repo root)
import streamlit as st
import os, importlib.util

def load_app_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

base = os.path.dirname(__file__)
media_path    = os.path.join(base, "media-monitor",    "app.py")
scenario_path = os.path.join(base, "scenario-simulator","app.py")
map_path      = os.path.join(base, "institutional-map","app.py")

media_app    = load_app_module("media_app",    media_path)
scenario_app = load_app_module("scenario_app", scenario_path)
map_app      = load_app_module("map_app",      map_path)

st.set_page_config(page_title="APEC-RISE Monitoring Suite", layout="wide")
st.title("🧠 APEC-RISE Monitoring Suite")
st.markdown("Use the tabs below to switch between your integrated tools.")

tab1, tab2, tab3 = st.tabs([
    "📡 Media Monitor",
    "🧭 Scenario Simulator",
    "🏛 Institutional Map"
])

with tab1:
    media_app.main()
with tab2:
    scenario_app.main()
with tab3:
    map_app.main()

