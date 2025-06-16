import streamlit as st
import os
import importlib.util

def load_and_run(name, filepath):
    """Dynamically load a sub-app and run its main(), isolating errors."""
    spec = importlib.util.spec_from_file_location(name, filepath)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except FileNotFoundError as e:
        st.error(f"⚠️ Couldn’t load {name}: {e}")
        return
    except Exception as e:
        st.error(f"❌ Error in {name}: {e}")
        return

    if hasattr(module, "main"):
        try:
            module.main()
        except Exception as e:
            st.error(f"❌ Runtime error in {name}: {e}")
    else:
        st.error(f"⚠️ {name} has no main() function")

# Paths to each sub-app
BASE = os.path.dirname(__file__)
APPS = {
    "📡 Media Monitor":     os.path.join(BASE, "media-monitor",    "app.py"),
    "🧭 Scenario Simulator": os.path.join(BASE, "scenario-simulator","app.py"),
    "🏛 Institutional Map":   os.path.join(BASE, "institutional-map", "app.py"),
}

# Page setup
st.set_page_config(page_title="US APEC-RISE Risk Monitoring Suite", layout="wide")
st.title("🧠 US APEC-RISE Risk Monitoring Suite")
st.markdown("Use the tabs below to switch between your inte
