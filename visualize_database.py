"""Generate interactive visualisations that summarise database activity."""

from pathlib import Path
import sys

import pandas as pd
import plotly.express as px
from plotly.offline import plot

from query_database import TwitterDBQuery, get_db_path


def load_posts(query: TwitterDBQuery) -> pd.DataFrame:
    """Fetch all posts from the database and return them as a DataFrame."""

    posts = query.get_all_posts()
    if not posts:
        raise ValueError("No posts were found in the database.")

    df = pd.DataFrame(posts)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df = df.dropna(subset=["created_at"]).sort_values("created_at").reset_index(drop=True)
    df["post_type"] = df["retweet_of_user_id"].apply(
        lambda value: "Retweet" if pd.notna(value) else "Original Tweet"
    )
    return df


def build_daily_series(df: pd.DataFrame) -> pd.DataFrame:
    """Return daily totals for the entire dataset."""

    daily_counts = (
        df.set_index("created_at")
        .resample("D")
        .size()
        .reset_index(name="post_count")
    )
    return daily_counts


def build_user_daily_series(df: pd.DataFrame) -> pd.DataFrame:
    """Return daily totals per author."""

    user_daily = (
        df.set_index("created_at")
        .groupby("author_username")
        .resample("D")
        .size()
        .reset_index(name="post_count")
    )
    return user_daily


def create_figures(df: pd.DataFrame) -> dict[str, object]:
    """Create the required Plotly figures and return them in a dictionary."""

    figures = {}

    # Who posts when: scatter timeline per author
    figures["who_posts_when"] = px.strip(
        df,
        x="created_at",
        y="author_username",
        color="post_type",
        hover_data={
            "tweet_id": True,
            "author_username": True,
            "created_at": "|%Y-%m-%d %H:%M",
            "retweet_of_username": True,
            "full_text": True,
        },
        title="Who Posts When",
    )
    figures["who_posts_when"].update_layout(yaxis_title="Author", xaxis_title="Timestamp")

    # Global time series
    daily_counts = build_daily_series(df)
    figures["overall_timeseries"] = px.line(
        daily_counts,
        x="created_at",
        y="post_count",
        title="Daily Post Volume (All Users)",
        markers=True,
    )
    figures["overall_timeseries"].update_layout(xaxis_title="Date", yaxis_title="Posts per Day")

    # Per-user time series
    user_daily = build_user_daily_series(df)
    figures["per_user_timeseries"] = px.line(
        user_daily,
        x="created_at",
        y="post_count",
        color="author_username",
        title="Daily Post Volume per User",
    )
    figures["per_user_timeseries"].update_layout(
        xaxis_title="Date",
        yaxis_title="Posts per Day",
        legend_title="Author",
        hovermode="x unified",
    )

    return figures


def save_dashboard(figures: dict[str, object], output_path: Path) -> Path:
    """Combine the figures into a single HTML dashboard."""

    output_path.parent.mkdir(parents=True, exist_ok=True)

    html_parts = [
        "<html><head><meta charset='utf-8'><title>Database Overview</title></head><body>",
        "<h1>Database Activity Overview</h1>",
    ]

    for figure in figures.values():
        html_parts.append(f"<section><h2>{figure.layout.title.text}</h2>")
        html_parts.append(plot(figure, include_plotlyjs='cdn', output_type='div'))
        html_parts.append("</section>")

    html_parts.append("</body></html>")

    output_path.write_text("\n".join(html_parts), encoding="utf-8")
    return output_path


def main() -> int:
    print("--- Database Overview Visualiser ---")
    try:
        db_path = get_db_path()
    except EOFError:
        print("Input cancelled. Exiting.")
        return 1

    try:
        query = TwitterDBQuery(db_path)
        df = load_posts(query)
    except Exception as exc:  # noqa: BLE001 - provide friendly message
        print(f"Failed to load data: {exc}")
        return 1

    figures = create_figures(df)

    output_file = Path("visualizations") / "database_overview.html"
    output_path = save_dashboard(figures, output_file)

    print("Created the following visualisations:")
    for key, figure in figures.items():
        print(f"  - {key}: {figure.layout.title.text}")

    print(f"\nDashboard saved to: {output_path.resolve()}")
    print("Open the file in a web browser to explore the interactive charts.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
