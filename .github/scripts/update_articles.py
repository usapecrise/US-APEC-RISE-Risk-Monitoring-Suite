import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

# === QA Dashboard ===
try:
    # Ensure we have clean dates
    df["published"] = pd.to_datetime(df["published"], errors="coerce")

    # 1. Articles over time
    by_date = df.groupby(df["published"].dt.date).size().reset_index(name="articles")
    fig1 = px.line(by_date, x="published", y="articles",
                   title="Articles Over Time")

    # 2. Sentiment by economy
    sent_econ = df.groupby(["economy", "sentiment"]).size().reset_index(name="count")
    fig2 = px.bar(sent_econ, x="economy", y="count", color="sentiment",
                  title="Sentiment by Economy", barmode="stack")

    # 3. Source type breakdown
    src = df.groupby("source_type").size().reset_index(name="count").sort_values("count", ascending=False)
    fig3 = px.bar(src, x="source_type", y="count", title="Articles by Source Type")

    # Combine into one dashboard
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=("Articles Over Time", "Sentiment by Economy", "Articles by Source Type", "")
    )

    for trace in fig1.data:
        fig.add_trace(trace, row=1, col=1)
    for trace in fig2.data:
        fig.add_trace(trace, row=1, col=2)
    for trace in fig3.data:
        fig.add_trace(trace, row=2, col=1)

    fig.update_layout(title_text="📰 Media Monitor QA Dashboard", showlegend=True)

    # Save as standalone HTML
    fig.write_html("data/media_dashboard.html")

    print("✅ QA dashboard saved → data/media_dashboard.html")
except Exception as e:
    print(f"⚠️ Could not generate dashboard: {e}")
