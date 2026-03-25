"""Hospital Price Transparency Dashboard — Total Knee Arthroplasty (HCPCS 27447).

Read-only viewer for the pipeline's combined.csv export.  No pipeline imports;
data is loaded from a committed snapshot in app/data/ or from the pipeline
output in data/processed/.
"""

from __future__ import annotations

import json
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Hospital Price Transparency: Knee Replacement",
    page_icon=":hospital:",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Data paths — prefer committed snapshot, fall back to pipeline output
# ---------------------------------------------------------------------------
_APP_DIR = Path(__file__).resolve().parent
_DATA_CANDIDATES = [
    _APP_DIR / "data" / "combined.csv",
    _APP_DIR.parent / "data" / "processed" / "combined.csv",
]
_META_CANDIDATES = [
    (_APP_DIR / "data" / "qa_summary.json", _APP_DIR / "data" / "export_metadata.json"),
    (
        _APP_DIR.parent / "data" / "processed" / "qa_summary.json",
        _APP_DIR.parent / "data" / "processed" / "export_metadata.json",
    ),
]


@st.cache_data
def load_data() -> pl.DataFrame:
    for p in _DATA_CANDIDATES:
        if p.is_file():
            df = pl.read_csv(p, infer_schema_length=5000)
            # Normalize charge_methodology to lowercase (mixed case in source)
            if "charge_methodology" in df.columns:
                df = df.with_columns(
                    pl.col("charge_methodology").str.to_lowercase().alias("charge_methodology")
                )
            return df
    return pl.DataFrame()


@st.cache_data
def load_metadata() -> tuple[dict, dict]:
    for qa_path, meta_path in _META_CANDIDATES:
        if qa_path.is_file() and meta_path.is_file():
            qa = json.loads(qa_path.read_text())
            meta = json.loads(meta_path.read_text())
            return qa, meta
    return {}, {}


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
df_all = load_data()
qa_summary, export_meta = load_metadata()

if df_all.is_empty():
    st.error("No data found. Run the pipeline first: `hpt run-all`")
    st.stop()

# ---------------------------------------------------------------------------
# Sidebar — filters
# ---------------------------------------------------------------------------
st.sidebar.title("Filters")

hospitals = sorted(df_all["hospital_name"].unique().to_list())
sel_hospitals = st.sidebar.multiselect("Hospital", hospitals, default=hospitals)

states = sorted(df_all["state"].unique().to_list())
sel_states = st.sidebar.multiselect("State", states, default=states)

payer_search = st.sidebar.text_input("Payer name (search)", "")

methodologies = sorted(
    [v for v in df_all["charge_methodology"].drop_nulls().unique().to_list() if v]
)
sel_methods = st.sidebar.multiselect("Charge methodology", methodologies, default=methodologies)

match_statuses = sorted(df_all["cms_match_status"].unique().to_list())
sel_match = st.sidebar.multiselect("CMS match status", match_statuses, default=match_statuses)

# DQ flags — extract unique tokens
dq_tokens: list[str] = []
for val in df_all["dq_flags"].drop_nulls().unique().to_list():
    if val:
        for tok in val.split("|"):
            tok = tok.strip()
            if tok and tok not in dq_tokens:
                dq_tokens.append(tok)
dq_tokens = sorted(dq_tokens)
dq_options = ["(none)"] + dq_tokens
sel_dq = st.sidebar.multiselect("DQ flags", dq_options, default=dq_options)

exclude_outliers = st.sidebar.checkbox("Exclude ratio outliers (> 10x)", value=True)

st.sidebar.markdown("---")
if export_meta:
    st.sidebar.caption(
        f"**Pipeline** v{export_meta.get('pipeline_version', '?')}  \n"
        f"**Generated** {export_meta.get('generated_at', '?')[:10]}  \n"
        f"**Rows** {export_meta.get('row_count', '?'):,}"
    )

# ---------------------------------------------------------------------------
# Apply filters
# ---------------------------------------------------------------------------
df = df_all.filter(
    pl.col("hospital_name").is_in(sel_hospitals)
    & pl.col("state").is_in(sel_states)
    & pl.col("cms_match_status").is_in(sel_match)
)

# Charge methodology — handle nulls
if sel_methods:
    df = df.filter(
        pl.col("charge_methodology").is_in(sel_methods) | pl.col("charge_methodology").is_null()
    )

# Payer search
if payer_search:
    df = df.filter(pl.col("payer_name").str.to_lowercase().str.contains(payer_search.lower()))

# DQ flags
if sel_dq:
    has_none = "(none)" in sel_dq
    token_list = [t for t in sel_dq if t != "(none)"]
    if has_none and token_list:
        df = df.filter(
            pl.col("dq_flags").is_null()
            | (pl.col("dq_flags") == "")
            | pl.col("dq_flags").str.contains("|".join(token_list))
        )
    elif has_none:
        df = df.filter(pl.col("dq_flags").is_null() | (pl.col("dq_flags") == ""))
    elif token_list:
        df = df.filter(pl.col("dq_flags").str.contains("|".join(token_list)))

# Ratio outlier filter — applied to a separate view for charts
df_ratio = df.filter(pl.col("commercial_to_medicare_ratio").is_not_null())
if exclude_outliers:
    df_ratio = df_ratio.filter(pl.col("commercial_to_medicare_ratio") <= 10.0)

# ---------------------------------------------------------------------------
# Title
# ---------------------------------------------------------------------------
st.title("Hospital Price Transparency: Total Knee Replacement")
st.caption(
    "Negotiated rates for HCPCS 27447 / DRG 469-470 across 15 hospitals, "
    "joined to CMS Medicare knee-replacement benchmarks."
)

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_overview, tab_ratio, tab_payer, tab_explorer = st.tabs(
    ["Overview", "Ratio Analysis", "Payer Comparison", "Data Explorer"]
)

# ========================== TAB 1: OVERVIEW ================================
with tab_overview:
    n_rows = len(df)
    n_hospitals = df["hospital_name"].n_unique()
    n_with_neg = df.filter(pl.col("negotiated_amount").is_not_null()).height
    ratios = df_ratio["commercial_to_medicare_ratio"].to_list()
    median_ratio = sorted(ratios)[len(ratios) // 2] if ratios else None
    match_rate = (
        df.filter(pl.col("cms_match_status") == "matched_ccn_roster").height / n_rows * 100
        if n_rows
        else 0
    )

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Rows", f"{n_rows:,}")
    c2.metric("Hospitals", n_hospitals)
    c3.metric("Median Ratio", f"{median_ratio:.2f}x" if median_ratio else "N/A")
    c4.metric("CMS Match", f"{match_rate:.0f}%")
    c5.metric("With Neg. Amt", f"{n_with_neg:,}")

    col_left, col_right = st.columns(2)

    with col_left:
        hosp_counts = (
            df.group_by("hospital_name")
            .agg(pl.len().alias("rows"))
            .sort("rows", descending=True)
            .to_pandas()
        )
        fig_hosp = px.bar(
            hosp_counts,
            y="hospital_name",
            x="rows",
            orientation="h",
            title="Rows by Hospital",
            labels={"hospital_name": "", "rows": "Row count"},
        )
        fig_hosp.update_layout(yaxis={"categoryorder": "total ascending"}, height=450)
        st.plotly_chart(fig_hosp, use_container_width=True)

    with col_right:
        match_counts = df.group_by("cms_match_status").agg(pl.len().alias("count")).to_pandas()
        fig_match = px.pie(
            match_counts,
            names="cms_match_status",
            values="count",
            title="CMS Match Status",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig_match.update_layout(height=450)
        st.plotly_chart(fig_match, use_container_width=True)

    col_dq, col_parser = st.columns(2)

    with col_dq:
        dq_counts: dict[str, int] = {}
        for val in df["dq_flags"].to_list():
            if val is None or val == "":
                dq_counts["(none)"] = dq_counts.get("(none)", 0) + 1
            else:
                for tok in val.split("|"):
                    tok = tok.strip()
                    if tok:
                        dq_counts[tok] = dq_counts.get(tok, 0) + 1
        if dq_counts:
            dq_df = (
                pl.DataFrame({"flag": list(dq_counts.keys()), "count": list(dq_counts.values())})
                .sort("count", descending=True)
                .to_pandas()
            )
            fig_dq = px.bar(
                dq_df, x="count", y="flag", orientation="h", title="DQ Flags Distribution"
            )
            fig_dq.update_layout(yaxis={"categoryorder": "total ascending"}, height=350)
            st.plotly_chart(fig_dq, use_container_width=True)

    with col_parser:
        parser_counts = (
            df.group_by("parser_strategy")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .to_pandas()
        )
        fig_parser = px.bar(
            parser_counts,
            x="count",
            y="parser_strategy",
            orientation="h",
            title="Parser Strategy Distribution",
        )
        fig_parser.update_layout(yaxis={"categoryorder": "total ascending"}, height=350)
        st.plotly_chart(fig_parser, use_container_width=True)

# ========================== TAB 2: RATIO ANALYSIS ==========================
with tab_ratio:
    if df_ratio.is_empty():
        st.info("No rows with computable commercial-to-Medicare ratio after filters.")
    else:
        st.subheader("Commercial-to-Medicare Ratio by Hospital")
        ratio_pd = df_ratio.select(
            "hospital_name",
            "commercial_to_medicare_ratio",
            "negotiated_amount",
            "cms_avg_mdcr_pymt_amt",
            "payer_name",
            "charge_methodology",
        ).to_pandas()

        fig_box = px.box(
            ratio_pd,
            x="commercial_to_medicare_ratio",
            y="hospital_name",
            orientation="h",
            title="Ratio Distribution by Hospital",
            labels={
                "commercial_to_medicare_ratio": "Commercial / Medicare Ratio",
                "hospital_name": "",
            },
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig_box.update_layout(
            yaxis={"categoryorder": "total ascending"},
            height=500,
        )
        st.plotly_chart(fig_box, use_container_width=True)

        col_hist, col_scatter = st.columns(2)

        with col_hist:
            fig_hist = px.histogram(
                ratio_pd,
                x="commercial_to_medicare_ratio",
                nbins=50,
                title="Ratio Distribution (Histogram)",
                labels={"commercial_to_medicare_ratio": "Commercial / Medicare Ratio"},
            )
            fig_hist.update_layout(height=400)
            st.plotly_chart(fig_hist, use_container_width=True)

        with col_scatter:
            scatter_df = ratio_pd.dropna(subset=["negotiated_amount", "cms_avg_mdcr_pymt_amt"])
            if not scatter_df.empty:
                fig_scatter = px.scatter(
                    scatter_df,
                    x="cms_avg_mdcr_pymt_amt",
                    y="negotiated_amount",
                    color="hospital_name",
                    title="Negotiated vs Medicare Payment",
                    labels={
                        "cms_avg_mdcr_pymt_amt": "CMS Medicare Avg Payment ($)",
                        "negotiated_amount": "Negotiated Amount ($)",
                    },
                    hover_data=["payer_name", "charge_methodology"],
                )
                max_val = max(
                    scatter_df["negotiated_amount"].max(),
                    scatter_df["cms_avg_mdcr_pymt_amt"].max(),
                )
                fig_scatter.add_trace(
                    go.Scatter(
                        x=[0, max_val],
                        y=[0, max_val],
                        mode="lines",
                        line={"dash": "dash", "color": "gray"},
                        name="1:1 line",
                        showlegend=True,
                    )
                )
                fig_scatter.update_layout(height=400)
                st.plotly_chart(fig_scatter, use_container_width=True)

        st.subheader("Per-Hospital Ratio Summary")
        summary = (
            df_ratio.group_by("hospital_name")
            .agg(
                pl.col("commercial_to_medicare_ratio").median().alias("median_ratio"),
                pl.col("commercial_to_medicare_ratio").quantile(0.25).alias("q1"),
                pl.col("commercial_to_medicare_ratio").quantile(0.75).alias("q3"),
                pl.col("commercial_to_medicare_ratio").min().alias("min"),
                pl.col("commercial_to_medicare_ratio").max().alias("max"),
                pl.len().alias("rows"),
            )
            .sort("median_ratio", descending=True)
        )
        fmt = {
            "median_ratio": "{:.2f}x",
            "q1": "{:.2f}x",
            "q3": "{:.2f}x",
            "min": "{:.2f}x",
            "max": "{:.2f}x",
        }
        st.dataframe(
            summary.to_pandas().style.format(fmt),
            use_container_width=True,
            hide_index=True,
        )

# ========================== TAB 3: PAYER COMPARISON ========================
with tab_payer:
    df_payer = df.filter(pl.col("negotiated_amount").is_not_null())
    if df_payer.is_empty():
        st.info("No rows with negotiated amounts after filters.")
    else:
        payer_stats = (
            df_payer.group_by("payer_name")
            .agg(
                pl.col("negotiated_amount").median().alias("median_amount"),
                pl.col("negotiated_amount").mean().alias("mean_amount"),
                pl.len().alias("rows"),
            )
            .sort("rows", descending=True)
        )

        # Also compute median ratio per payer (where available)
        if not df_ratio.is_empty():
            payer_ratio = df_ratio.group_by("payer_name").agg(
                pl.col("commercial_to_medicare_ratio").median().alias("median_ratio")
            )
            payer_stats = payer_stats.join(payer_ratio, on="payer_name", how="left")

        st.subheader("Top 15 Payers by Row Count")
        top15 = payer_stats.head(15).to_pandas()
        fig_payer = px.bar(
            top15,
            x="median_amount",
            y="payer_name",
            orientation="h",
            title="Median Negotiated Amount — Top 15 Payers",
            labels={"median_amount": "Median Negotiated Amount ($)", "payer_name": ""},
        )
        fig_payer.update_layout(
            yaxis={"categoryorder": "total ascending"},
            height=500,
        )
        st.plotly_chart(fig_payer, use_container_width=True)

        st.subheader("Payer Statistics")
        payer_pd = payer_stats.to_pandas()
        fmt_payer: dict[str, str] = {
            "median_amount": "${:,.0f}",
            "mean_amount": "${:,.0f}",
        }
        if "median_ratio" in payer_pd.columns:
            fmt_payer["median_ratio"] = "{:.2f}x"
        st.dataframe(
            payer_pd.style.format(fmt_payer),
            use_container_width=True,
            hide_index=True,
            height=500,
        )

# ========================== TAB 4: DATA EXPLORER ===========================
with tab_explorer:
    KEY_COLS = [
        "hospital_name",
        "state",
        "payer_name",
        "plan_name",
        "negotiated_amount",
        "charge_methodology",
        "cms_avg_mdcr_pymt_amt",
        "commercial_to_medicare_ratio",
        "cms_match_status",
        "dq_flags",
        "procedure_code",
        "parser_strategy",
    ]

    show_all = st.checkbox("Show all 54 columns", value=False)
    display_cols = df.columns if show_all else [c for c in KEY_COLS if c in df.columns]

    st.caption(f"Showing **{len(df):,}** of {len(df_all):,} rows")
    st.dataframe(
        df.select(display_cols).to_pandas(),
        use_container_width=True,
        height=600,
        hide_index=True,
    )

    csv_bytes = df.select(display_cols).write_csv().encode("utf-8")
    st.download_button(
        label=f"Download filtered CSV ({len(df):,} rows)",
        data=csv_bytes,
        file_name="hpt_filtered_export.csv",
        mime="text/csv",
    )
