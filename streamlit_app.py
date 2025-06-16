import streamlit as st
import os
import importlib.util

# ── Page config must be first ──
st.set_page_config(page_title="APEC-RISE Risk Monitoring Suite", layout="wide")

def load_app_module(name, filepath):
    """Helper to dynamically load a sub-app module."""
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
    ),
}

# Read query params
params = st.query_params
selected = params.get("app", [None])[0]

if selected not in APPS:
    # --- Landing page ---
    st.title("🚨 APEC-RISE Risk Monitoring Suite")
    st.write("Select a tool below to launch it in this tab:")

    # Display each app as a markdown link in its own column
    cols = st.columns(len(APPS))
    for (slug, (title, desc, _)), col in zip(APPS.items(), cols):
        with col:
            st.markdown(f"**[{title}](?app={slug})**")
            st.caption(desc)

else:
    # --- Sub-app view ---
    title, desc, path = APPS[selected]
    st.header(title)
    st.write(desc)
    st.markdown("---")
    # Back link
    st.markdown("[← Back to Suite](?app=)")

    # Load and run the selected sub-app
    try:
        app = load_app_module(selected, path)
        app.main()
    except FileNotFoundError as e:
        st.error(f"⚠️ Couldn't load {title}: {e}")
    except Exception as e:
        st.error(f"❌ Error running {title}: {e}")
