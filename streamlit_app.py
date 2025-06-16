import streamlit as st
import os
import importlib.util

# ── Page config must be the very first Streamlit command ──
st.set_page_config(page_title="APEC-RISE Monitoring Suite", layout="wide")

def load_app(name, filepath):
    """Helper to dynamically load a sub-app module."""
    spec = importlib.util.spec_from_file_location(name, filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# Map slugs to (title, path)
BASE = os.path.dirname(__file__)
APPS = {
    "media":    ("📡 Media Monitor",        os.path.join(BASE, "media-monitor",    "app.py")),
    "scenario": ("🧭 Scenario Simulator",  os.path.join(BASE, "scenario-simulator","app.py")),
    "map":      ("🏛 Institutional Map",    os.path.join(BASE, "institutional-map","app.py")),
}

# ── Use the new st.query_params API ──
params = st.query_params
selected = params.get("app", [None])[0]

if selected not in APPS:
    # Landing page
    st.title("🧠 APEC-RISE Monitoring Suite")
    st.markdown("Welcome! Select a tool below to open it in this tab:")
    for slug, (title, _) in APPS.items():
        st.markdown(f"- [{title}](?app={slug})")
    st.markdown("---")
    st.markdown("*Click a link above to launch that tool.*")
else:
    # Sub-app view
    title, path = APPS[selected]
    st.header(title)
    st.markdown("[← Back to suite](?app=)")
    try:
        app = load_app(selected, path)
        app.main()
    except FileNotFoundError as e:
        st.error(f"⚠️ Couldn't load {title}: {e}")
    except Exception as e:
        st.error(f"❌ Error running {title}: {e}")
