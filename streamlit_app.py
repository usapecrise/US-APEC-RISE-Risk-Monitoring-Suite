import streamlit as st
import os
import importlib.util

# Helper to load a module by filepath
def load_app(name, filepath):
    spec = importlib.util.spec_from_file_location(name, filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# Map slugs to (tab title, path to app.py)
BASE = os.path.dirname(__file__)
APPS = {
    "media":   ("📡 Media Monitor",       os.path.join(BASE, "media-monitor",    "app.py")),
    "scenario":("🧭 Scenario Simulator", os.path.join(BASE, "scenario-simulator","app.py")),
    "map":     ("🏛 Institutional Map",   os.path.join(BASE, "institutional-map","app.py")),
}

# Read query param
params = st.experimental_get_query_params()
selected = params.get("app", [None])[0]

# Page config
st.set_page_config(page_title="APEC-RISE Monitoring Suite", layout="wide")

if selected not in APPS:
    # --- Landing page ---
    st.title("🧠 APEC-RISE Monitoring Suite")
    st.markdown("""
    Welcome! Select one of the tools below to open it in this browser tab.
    """)
    for slug, (title, _) in APPS.items():
        st.markdown(f"- [{title}](?app={slug})")
    st.markdown("---")
    st.markdown("*Apps will open here. Use your browser’s back button or the ‘Back’ link inside each tool to return.*")

else:
    # --- Sub-app page ---
    title, path = APPS[selected]
    st.header(title)
    # Back link
    st.markdown("[← Back to suite](?app=)")
    # Dynamically load & run
    try:
        app = load_app(selected, path)
        app.main()
    except FileNotFoundError as e:
        st.error(f"⚠️ Couldn’t load {title}: {e}")
    except Exception as e:
        st.error(f"❌ Error running {title}: {e}")
