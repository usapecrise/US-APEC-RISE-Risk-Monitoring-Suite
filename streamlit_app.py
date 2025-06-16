import streamlit as st
import os
import importlib.util

# ── Page config must be first ──
st.set_page_config(page_title="APEC-RISE Monitoring Suite", layout="wide")

# Helper to dynamically load a sub-app module
 def load_app_module(name, filepath):
    spec = importlib.util.spec_from_file_location(name, filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# Map slugs to (title, description, path)
BASE = os.path.dirname(__file__)
APPS = {
    "media": (
        "📡 Media Monitor",
        "Tracks leadership changes, media sentiment, and alignment with U.S. objectives across APEC economies.",
        os.path.join(BASE, "media-monitor", "app.py")
    ),
    "scenario": (
        "🧭 Scenario Simulator",
        "Explore shifts from baseline to optimistic/pessimistic scenarios based on risk triggers.",
        os.path.join(BASE, "scenario-simulator", "app.py")
    ),
    "map": (
        "🏛 Institutional Map",
        "Visualize networks of influence and institutional coordination across stakeholders.",
        os.path.join(BASE, "institutional-map", "app.py")
    )
}

# Read query params
params = st.query_params
selected = params.get("app", [None])[0]

if selected not in APPS:
    # --- Landing page ---
    st.title("🧠 APEC-RISE Monitoring Suite")
    st.write(
        "Welcome! Choose one of the tools below to launch it in this tab."
    )

    cols = st.columns(len(APPS))
    for (slug, (title, desc, _)), col in zip(APPS.items(), cols):
        with col:
            st.subheader(title)
            st.write(desc)
            # Launch button sets query param to load the app
            if st.button("Open", key=slug):
                st.experimental_set_query_params(app=slug)

else:
    # --- Sub-app view ---
    title, desc, path = APPS[selected]
    st.header(title)
    # Back button resets query params
    if st.button("← Back to Suite"):
        st.experimental_set_query_params()
    try:
        app = load_app_module(selected, path)
        app.main()
    except FileNotFoundError as e:
        st.error(f"⚠️ Couldn't load {title}: {e}")
    except Exception as e:
        st.error(f"❌ Error running {title}: {e}")
