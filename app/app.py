"""
CS 457 Final Project
app.py
Dashboard for steam viewer
"""

import pandas as pd
import plotly.express as px
import streamlit as st
from db import GameService

st.set_page_config(
    page_title="Steam Explorer",
    page_icon="🎮",
    layout="wide",
)

st.title("🎮 Steam Explorer")
st.caption("Browse and analyze 27,000+ Steam games powered by PostgreSQL.")

@st.cache_resource
def get_service() -> GameService:
    return GameService()

svc = get_service()

st.sidebar.header("Filters")


name_query = st.sidebar.text_input("Search by title", placeholder="e.g. Counter-Strike")


@st.cache_data
def load_genres():
    return get_service().get_all_genres()

all_genres = load_genres()
selected_genres = st.sidebar.multiselect("Genre(s)", all_genres)


@st.cache_data
def load_year_range():
    return get_service().get_release_year_range()

year_min_db, year_max_db = load_year_range()
year_range = st.sidebar.slider(
    "Release Year",
    min_value=year_min_db,
    max_value=year_max_db,
    value=(year_min_db, year_max_db),
)


@st.cache_data
def load_price_range():
    return get_service().get_price_range()

price_min_db, price_max_db = load_price_range()
price_range = st.sidebar.slider(
    "Price (USD)",
    min_value=float(price_min_db),
    max_value=float(price_max_db),
    value=(float(price_min_db), float(price_max_db)),
    step=0.99,
)


tab_browse, tab_charts = st.tabs(["🔍 Browse Games", "📊 Analytics"])


with tab_browse:
    filter_kwargs = dict(
        genres=selected_genres or None,
        year_min=year_range[0],
        year_max=year_range[1],
        price_min=price_range[0],
        price_max=price_range[1],
        name_query=name_query or None,
    )

    total = svc.count_games(**filter_kwargs)
    st.subheader(f"{total:,} games match your filters")

    PAGE_SIZE = 50
    page = st.number_input("Page", min_value=1, max_value=max(1, -(-total // PAGE_SIZE)), value=1)
    offset = (page - 1) * PAGE_SIZE

    rows = svc.search_games(**filter_kwargs, limit=PAGE_SIZE, offset=offset)

    if rows:
        df = pd.DataFrame(rows)

        
        df["price"] = df["price"].apply(
            lambda p: "Free" if p == 0 else (f"${p:.2f}" if pd.notna(p) else "N/A")
        )
        df["review_score"] = df["review_score"].apply(
            lambda s: f"{s}%" if pd.notna(s) else "N/A"
        )
        df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce").dt.strftime("%Y-%m-%d")

        display_cols = {
            "name": "Title",
            "developer": "Developer",
            "publisher": "Publisher",
            "release_date": "Released",
            "price": "Price",
            "review_score": "Review Score",
            "genres": "Genres",
            "owners": "Owners",
        }
        st.dataframe(
            df[list(display_cols.keys())].rename(columns=display_cols),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No games match your current filters.")

with tab_charts:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Game Releases Per Year")
        year_data = pd.DataFrame(svc.games_per_year())
        if not year_data.empty:
            fig = px.bar(year_data, x="year", y="count", labels={"year": "Year", "count": "Games Released"})
            fig.update_layout(margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)

 
    with col2:
        st.subheader("Top Genres by Game Count")
        genre_data = pd.DataFrame(svc.top_genres())
        if not genre_data.empty:
            fig = px.bar(
                genre_data.sort_values("count"),
                x="count", y="genre", orientation="h",
                labels={"count": "# Games", "genre": "Genre"},
            )
            fig.update_layout(margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)

    
    with col3:
        st.subheader("Avg Price by Genre (paid games, 10+ titles)")
        price_data = pd.DataFrame(svc.avg_price_by_genre())
        if not price_data.empty:
            fig = px.bar(
                price_data.sort_values("avg_price"),
                x="avg_price", y="genre", orientation="h",
                labels={"avg_price": "Avg Price (USD)", "genre": "Genre"},
            )
            fig.update_layout(margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)

    
    with col4:
        st.subheader("Platform Support")
        plat = svc.platform_breakdown()
        if plat:
            plat_df = pd.DataFrame(
                {"Platform": ["Windows", "Mac", "Linux"],
                 "Games": [plat["windows"], plat["mac"], plat["linux"]]}
            )
            fig = px.pie(plat_df, names="Platform", values="Games")
            fig.update_layout(margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)

    
    st.subheader("Top Rated Games (min 500 reviews)")
    top_rated = pd.DataFrame(svc.top_rated_games())
    if not top_rated.empty:
        top_rated["score"] = top_rated["score"].apply(lambda s: f"{s}%")
        st.dataframe(
            top_rated.rename(columns={
                "name": "Title",
                "positive_ratings": "Positive",
                "negative_ratings": "Negative",
                "score": "Score",
            }),
            use_container_width=True,
            hide_index=True,
        )