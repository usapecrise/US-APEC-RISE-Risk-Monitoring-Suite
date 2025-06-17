import streamlit as st

# ── Page configuration ──
st.set_page_config(page_title="US APEC-RISE Risk Monitoring Suite", layout="wide")

# ── Header ──
st.markdown(
    """
    <div style="text-align: center; padding: 20px 0;">
        <h1 style="margin:0;">US APEC-RISE Risk Monitoring Suite</h1>
        <p style="font-size:18px; color: #555; margin-top:10px;">
            Integrated tools for monitoring risks to critical assumptions, simulate scenario shifts, and inform adaptive strategies across APEC economies.
        </p>
    </div>
    """,
    unsafe_allow_html=True
)

# ── Tool cards ──
links = {
    "📡 Media Monitor": {
        "desc": "Track leadership changes, media sentiment, and alignment with U.S. objectives.",
        "url": "https://us-apec-rise-media-monitor.streamlit.app/"
    },
    "🧭 Scenario Watch": {
        "desc": "Simulate shifts from baseline to optimistic/pessimistic scenarios based on risk triggers.",
        "url": "https://us-apec-rise-risk-monitoring-suite-scenario-simulator.streamlit.app/"
    },
    "🏛 Institutional Map": {
        "desc": "Visualize networks of influence and institutional coordination.",
        "url": "https://us-apec-rise-risk-monitoring-suite-institutional-map.streamlit.app/"
    }
}

# Display cards in responsive layout
cols = st.columns(len(links))
for (title, info), col in zip(links.items(), cols):
    with col:
        st.markdown(
            f"""
            <div style=
                "padding:16px; border:1px solid #eee; border-radius:8px;
                 box-shadow:0 2px 4px rgba(0,0,0,0.1); background-color:#fff; margin-bottom:20px; text-align:center;"
            >
                <h3 style="margin-bottom:8px;">{title}</h3>
                <p style="color:#666; font-size:14px; margin-bottom:12px;">{info['desc']}</p>
                <a href="{info['url']}" target="_blank" style="text-decoration:none;">
                    <button style=
                        "padding:10px 24px; background-color:#005A9C; color:#fff;
                         border:none; border-radius:4px; font-size:14px; cursor:pointer;"
                    >Launch</button>
                </a>
            </div>
            """,
            unsafe_allow_html=True
        )

# ── Footer ──
st.markdown(
    """
    <hr>
    <p style="text-align:center; font-size:12px; color:#888; margin-top:10px;">
        © 2025 APEC-RISE Risk Monitoring Suite
    </p>
    """,
    unsafe_allow_html=True
)
