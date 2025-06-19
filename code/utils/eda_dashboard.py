from typing import Dict

import ipywidgets as widgets
import numpy as np
import plotly.graph_objects as go
import polars as pl
from IPython.display import display
from plotly.subplots import make_subplots


class InteractiveDataFrameInspector:
    """
    Interactive dashboard for comprehensive dataframe inspection in Jupyter notebooks.
    """

    def __init__(self, df: pl.DataFrame, name: str = "DataFrame"):
        self.df = df
        self.name = name
        self.n_rows, self.n_cols = df.shape

        # Categorize columns
        self.numeric_cols = [
            col
            for col in df.columns
            if df[col].dtype
            in [pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8]
        ]
        self.categorical_cols = [
            col for col in df.columns if df[col].dtype in [pl.Utf8, pl.Categorical]
        ]
        self.date_cols = [
            col for col in df.columns if df[col].dtype in [pl.Date, pl.Datetime]
        ]
        self.other_cols = [
            col
            for col in df.columns
            if col not in self.numeric_cols + self.categorical_cols + self.date_cols
        ]

        # Calculate quality metrics
        self.quality_metrics = self._calculate_quality_metrics()

        # Create widgets
        self._create_widgets()

    def _calculate_quality_metrics(self) -> Dict:
        """Calculate data quality metrics."""
        missing_summary = self.df.null_count()
        total_missing = missing_summary.sum_horizontal().item(0)
        missing_pct = (total_missing / (self.n_rows * self.n_cols)) * 100

        duplicate_count = self.n_rows - self.df.unique().shape[0]
        duplicate_pct = (duplicate_count / self.n_rows) * 100

        # Missing values by column
        missing_cols = []
        for col in self.df.columns:
            missing_count = self.df[col].null_count()
            if missing_count > 0:
                missing_pct_col = (missing_count / self.n_rows) * 100
                missing_cols.append((col, missing_count, missing_pct_col))

        return {
            "total_missing": total_missing,
            "missing_percentage": missing_pct,
            "duplicate_count": duplicate_count,
            "duplicate_percentage": duplicate_pct,
            "missing_by_column": missing_cols,
        }

    def _create_widgets(self):
        """Create interactive widgets."""
        # Dropdown for plot type
        self.plot_type_widget = widgets.Dropdown(
            options=[
                ("Overview Dashboard", "overview"),
                ("Missing Values Analysis", "missing"),
                ("Numeric Distribution", "numeric_dist"),
                ("Categorical Analysis", "categorical"),
                ("Correlation Matrix", "correlation"),
                ("Data Quality Summary", "quality"),
            ],
            value="overview",
            description="Plot Type:",
            style={"description_width": "initial"},
        )

        # Dropdown for variable selection (dynamic based on plot type)
        self.variable_widget = widgets.Dropdown(
            options=self.df.columns,
            value=self.df.columns[0] if self.df.columns else None,
            description="Variable:",
            style={"description_width": "initial"},
        )

        # Checkbox for sample data (for large datasets)
        self.sample_widget = widgets.Checkbox(
            value=True,
            description="Sample data (faster for large datasets)",
            style={"description_width": "initial"},
        )

        # Sample size slider
        max_sample = max(100, min(10000, self.n_rows))  # Ensure max >= min
        self.sample_size_widget = widgets.IntSlider(
            value=min(max_sample, self.n_rows),
            min=min(100, self.n_rows),  # Adjust min for small datasets
            max=max_sample,
            step=min(500, max(1, self.n_rows // 10)),  # Adjust step for small datasets
            description="Sample Size:",
            style={"description_width": "initial"},
        )

        # Update button
        self.update_button = widgets.Button(
            description="Update Plot", button_style="primary"
        )

        # Bind events
        self.plot_type_widget.observe(self._on_plot_type_change, names="value")
        self.update_button.on_click(self._update_plot)

    def _on_plot_type_change(self, change):
        """Update variable options based on plot type."""
        plot_type = change["new"]

        if plot_type == "numeric_dist":
            self.variable_widget.options = self.numeric_cols
        elif plot_type == "categorical":
            self.variable_widget.options = self.categorical_cols
        elif plot_type in ["correlation"]:
            self.variable_widget.options = ["All"] + self.numeric_cols
        else:
            self.variable_widget.options = ["All"] + list(self.df.columns)

        if self.variable_widget.options:
            self.variable_widget.value = self.variable_widget.options[0]

    def _get_sample_data(self) -> pl.DataFrame:
        """Get sample of data if requested."""
        if self.sample_widget.value and self.n_rows > self.sample_size_widget.value:
            return self.df.sample(self.sample_size_widget.value)
        return self.df

    def _create_overview_dashboard(self) -> go.Figure:
        """Create overview dashboard with better spacing and layout."""
        # Create a cleaner layout with 16:10 aspect ratio
        fig = make_subplots(
            rows=3,
            cols=2,
            subplot_titles=[
                "Data Type Distribution",
                "",
                "Missing Values by Column (Top 10)",
                "",
                "Sample Data Preview",
                "",
            ],
            specs=[
                [{"type": "pie"}, {"type": "indicator"}],
                [{"type": "bar", "colspan": 2}, None],
                [{"type": "table", "colspan": 2}, None],
            ],
            vertical_spacing=0.15,  # Increase vertical spacing
            horizontal_spacing=0.1,  # Increase horizontal spacing
        )

        # 1. Data type distribution (pie chart)
        type_counts = [
            len(self.numeric_cols),
            len(self.categorical_cols),
            len(self.date_cols),
            len(self.other_cols),
        ]
        type_labels = ["Numeric", "Categorical", "Date", "Other"]
        type_counts = [count for count in type_counts if count > 0]
        type_labels = [
            label for count, label in zip(type_counts, type_labels) if count > 0
        ]

        fig.add_trace(
            go.Pie(
                labels=type_labels,
                values=type_counts,
                name="Data Types",
                textinfo="label+percent",
                textposition="auto",
            ),
            row=1,
            col=1,
        )

        # 2. Quality score indicator (larger and cleaner)
        quality_score = 100 - (
            self.quality_metrics["missing_percentage"]
            + self.quality_metrics["duplicate_percentage"]
        )
        quality_score = max(0, quality_score)

        fig.add_trace(
            go.Indicator(
                mode="gauge+number",
                value=quality_score,
                title={"text": "Overall Data Quality", "font": {"size": 16}},
                number={"font": {"size": 20}},
                gauge={
                    "axis": {"range": [0, 100], "tickfont": {"size": 12}},
                    "bar": {"color": "darkblue", "thickness": 0.8},
                    "steps": [
                        {"range": [0, 50], "color": "lightgray"},
                        {"range": [50, 80], "color": "yellow"},
                        {"range": [80, 100], "color": "lightgreen"},
                    ],
                    "threshold": {
                        "line": {"color": "red", "width": 4},
                        "thickness": 0.75,
                        "value": 90,
                    },
                },
            ),
            row=1,
            col=2,
        )

        # 3. Missing values by column (horizontal bar chart, full width)
        if self.quality_metrics["missing_by_column"]:
            missing_data = sorted(
                self.quality_metrics["missing_by_column"],
                key=lambda x: x[2],
                reverse=True,
            )[:10]
            cols, counts, pcts = zip(*missing_data)

            fig.add_trace(
                go.Bar(
                    x=list(pcts),
                    y=list(cols),
                    orientation="h",
                    name="Missing %",
                    marker_color="lightcoral",
                    text=[f"{p:.1f}%" for p in pcts],
                    textposition="auto",
                ),
                row=2,
                col=1,
            )

        # 4. Sample data table (full width, limited columns for readability)
        sample_df = self._get_sample_data()
        preview_df = sample_df.head(3).to_pandas()

        # Limit to first 8 columns to avoid overcrowding
        cols_to_show = preview_df.columns[:8]
        preview_df = preview_df[cols_to_show]

        # Truncate long text values
        for col in preview_df.columns:
            if preview_df[col].dtype == "object":
                preview_df[col] = preview_df[col].astype(str).str[:20] + "..."

        fig.add_trace(
            go.Table(
                header=dict(
                    values=list(preview_df.columns), font=dict(size=11), align="left"
                ),
                cells=dict(
                    values=[preview_df[col] for col in preview_df.columns],
                    font=dict(size=10),
                    align="left",
                ),
            ),
            row=3,
            col=1,
        )

        # Update layout with 16:10 aspect ratio and better spacing
        fig.update_layout(
            width=1200,  # 16:10 aspect ratio
            height=750,  # 16:10 aspect ratio
            title_text=f"📊 {self.name} - Overview Dashboard",
            title_font_size=18,
            showlegend=False,
            margin=dict(t=80, b=50, l=50, r=50),  # Add margins
        )

        # Update x-axis for missing values chart
        fig.update_xaxes(title_text="Missing Percentage (%)", row=2, col=1)
        fig.update_yaxes(title_text="Columns", row=2, col=1)

        return fig

    def _create_missing_values_plot(self) -> go.Figure:
        """Create detailed missing values analysis."""
        df_sample = self._get_sample_data()

        # Create missing values heatmap
        missing_matrix = []
        columns = df_sample.columns[:20]  # Limit to first 20 columns

        for i, row in enumerate(df_sample.iter_rows()):
            if i >= 100:  # Limit to first 100 rows for visualization
                break
            missing_row = [1 if val is None else 0 for val in row[:20]]
            missing_matrix.append(missing_row)

        fig = go.Figure(
            data=go.Heatmap(
                z=missing_matrix,
                x=columns,
                y=list(range(len(missing_matrix))),
                colorscale="Viridis",
                showscale=True,
            )
        )

        fig.update_layout(
            title=f"Missing Values Pattern - {self.name}",
            xaxis_title="Columns",
            yaxis_title="Rows (sample)",
            width=1200,
            height=750,
        )

        return fig

    def _create_numeric_distribution(self, column: str = None) -> go.Figure:
        """Create numeric distribution plot."""
        df_sample = self._get_sample_data()

        if column and column != "All":
            cols_to_plot = [column]
        else:
            cols_to_plot = self.numeric_cols[:6]  # Limit to 6 columns

        fig = go.Figure()

        for col in cols_to_plot:
            values = df_sample[col].drop_nulls().to_pandas()
            fig.add_trace(go.Histogram(x=values, name=col, opacity=0.7, nbinsx=50))

        fig.update_layout(
            title=f"Numeric Distributions - {self.name}",
            xaxis_title="Value",
            yaxis_title="Frequency",
            barmode="overlay",
            width=1200,
            height=750,
        )

        return fig

    def _create_categorical_analysis(self, column: str = None) -> go.Figure:
        """Create categorical analysis plot."""
        if column and column != "All":
            cols_to_plot = [column]
        else:
            cols_to_plot = self.categorical_cols[:1]  # Show first categorical column

        if not cols_to_plot:
            # Create empty plot with message
            fig = go.Figure()
            fig.add_annotation(
                text="No categorical columns available",
                xref="paper",
                yref="paper",
                x=0.5,
                y=0.5,
                showarrow=False,
            )
            return fig

        col = cols_to_plot[0]
        value_counts = self.df[col].value_counts().limit(15).to_pandas()

        fig = go.Figure(
            data=[go.Bar(x=value_counts.iloc[:, 0], y=value_counts.iloc[:, 1])]
        )

        fig.update_layout(
            title=f"Top Values in {col} - {self.name}",
            xaxis_title=col,
            yaxis_title="Count",
            width=1200,
            height=750,
        )

        return fig

    def _create_correlation_matrix(self) -> go.Figure:
        """Create correlation matrix heatmap."""
        if len(self.numeric_cols) < 2:
            fig = go.Figure()
            fig.add_annotation(
                text="Need at least 2 numeric columns for correlation",
                xref="paper",
                yref="paper",
                x=0.5,
                y=0.5,
                showarrow=False,
            )
            return fig

        df_sample = self._get_sample_data()
        corr_cols = self.numeric_cols[:10]  # Limit to 10 columns
        corr_matrix = df_sample.select(corr_cols).to_pandas().corr()

        fig = go.Figure(
            data=go.Heatmap(
                z=corr_matrix.values,
                x=corr_matrix.columns,
                y=corr_matrix.columns,
                colorscale="RdBu",
                zmid=0,
                text=np.round(corr_matrix.values, 2),
                texttemplate="%{text}",
                textfont={"size": 10},
            )
        )

        fig.update_layout(
            title=f"Correlation Matrix - {self.name}", width=1200, height=750
        )

        return fig

    def _create_quality_summary(self) -> go.Figure:
        """Create data quality summary."""
        metrics = self.quality_metrics

        # Create gauge charts for different quality metrics
        fig = make_subplots(
            rows=1,
            cols=3,
            subplot_titles=["Completeness", "Uniqueness", "Overall Quality"],
            specs=[
                [{"type": "indicator"}, {"type": "indicator"}, {"type": "indicator"}]
            ],
        )

        # Completeness (100 - missing percentage)
        completeness = 100 - metrics["missing_percentage"]
        fig.add_trace(
            go.Indicator(
                mode="gauge+number",
                value=completeness,
                title={"text": "Completeness %"},
                gauge={
                    "axis": {"range": [0, 100]},
                    "bar": {"color": "green"},
                    "steps": [
                        {"range": [0, 70], "color": "lightgray"},
                        {"range": [70, 90], "color": "yellow"},
                    ],
                    "threshold": {
                        "line": {"color": "red", "width": 4},
                        "thickness": 0.75,
                        "value": 95,
                    },
                },
            ),
            row=1,
            col=1,
        )

        # Uniqueness (100 - duplicate percentage)
        uniqueness = 100 - metrics["duplicate_percentage"]
        fig.add_trace(
            go.Indicator(
                mode="gauge+number",
                value=uniqueness,
                title={"text": "Uniqueness %"},
                gauge={
                    "axis": {"range": [0, 100]},
                    "bar": {"color": "blue"},
                    "steps": [
                        {"range": [0, 70], "color": "lightgray"},
                        {"range": [70, 90], "color": "yellow"},
                    ],
                    "threshold": {
                        "line": {"color": "red", "width": 4},
                        "thickness": 0.75,
                        "value": 95,
                    },
                },
            ),
            row=1,
            col=2,
        )

        # Overall quality score
        overall_quality = (completeness + uniqueness) / 2
        fig.add_trace(
            go.Indicator(
                mode="gauge+number",
                value=overall_quality,
                title={"text": "Overall Quality %"},
                gauge={
                    "axis": {"range": [0, 100]},
                    "bar": {"color": "purple"},
                    "steps": [
                        {"range": [0, 70], "color": "lightgray"},
                        {"range": [70, 90], "color": "yellow"},
                    ],
                    "threshold": {
                        "line": {"color": "red", "width": 4},
                        "thickness": 0.75,
                        "value": 90,
                    },
                },
            ),
            row=1,
            col=3,
        )

        fig.update_layout(
            title=f"Data Quality Summary - {self.name}", width=1200, height=750
        )

        return fig

    def _update_plot(self, button):
        """Update plot based on current widget values."""
        plot_type = self.plot_type_widget.value
        variable = self.variable_widget.value

        if plot_type == "overview":
            fig = self._create_overview_dashboard()
        elif plot_type == "missing":
            fig = self._create_missing_values_plot()
        elif plot_type == "numeric_dist":
            fig = self._create_numeric_distribution(variable)
        elif plot_type == "categorical":
            fig = self._create_categorical_analysis(variable)
        elif plot_type == "correlation":
            fig = self._create_correlation_matrix()
        elif plot_type == "quality":
            fig = self._create_quality_summary()

        with self.output_widget:
            self.output_widget.clear_output(wait=True)
            fig.show()

    def display_summary(self):
        """Display text summary of the dataframe."""
        print(f"📊 {self.name.upper()} - SUMMARY")
        print("=" * 60)
        print(f"Shape: {self.n_rows:,} rows × {self.n_cols} columns")
        print(f"Memory usage: {self.df.estimated_size('mb'):.2f} MB")
        print("\nColumn Types:")
        print(f"  Numeric: {len(self.numeric_cols)}")
        print(f"  Categorical: {len(self.categorical_cols)}")
        print(f"  Date: {len(self.date_cols)}")
        print(f"  Other: {len(self.other_cols)}")

        print("\nData Quality:")
        print(
            f"  Missing values: {self.quality_metrics['total_missing']:,} ({self.quality_metrics['missing_percentage']:.2f}%)"
        )
        print(
            f"  Duplicate rows: {self.quality_metrics['duplicate_count']:,} ({self.quality_metrics['duplicate_percentage']:.2f}%)"
        )

        # Key insights
        insights = []
        if self.quality_metrics["missing_percentage"] > 10:
            insights.append(
                f"⚠️  High missing data rate ({self.quality_metrics['missing_percentage']:.1f}%)"
            )
        if self.quality_metrics["duplicate_percentage"] > 5:
            insights.append(
                f"⚠️  Significant duplicates ({self.quality_metrics['duplicate_percentage']:.1f}%)"
            )
        if len(self.numeric_cols) == 0:
            insights.append("ℹ️  No numeric columns found")
        if self.n_rows < 100:
            insights.append("⚠️  Small dataset")

        if insights:
            print("\n💡 Key Insights:")
            for insight in insights:
                print(f"  {insight}")
        else:
            print("\n✅ Data appears to be in good shape!")

        print("Summary overview:")
        with pl.Config(tbl_cols=-1, tbl_width_chars=-1):
            print(self.df.describe())

    def display_dashboard(self):
        """Display the interactive dashboard."""
        self.output_widget = widgets.Output()

        # Create horizontal control panel at the top
        controls = widgets.VBox(
            [
                widgets.HTML("<h3>🔍 Interactive DataFrame Inspector</h3>"),
                widgets.HBox(
                    [
                        self.plot_type_widget,
                        self.variable_widget,
                        self.sample_widget,
                        self.sample_size_widget,
                        self.update_button,
                    ],
                    layout=widgets.Layout(justify_content="space-between"),
                ),
            ]
        )

        # Display everything vertically
        dashboard = widgets.VBox([controls, self.output_widget])

        display(dashboard)

        # Show initial plot
        self._update_plot(None)

        return dashboard


def inspect_dataframe_interactive(
    df: pl.DataFrame, name: str = "DataFrame", figsize: tuple = (16, 10)
):
    """
    Create and display an interactive dataframe inspector.

    Args:
        df: Polars DataFrame to inspect
        name: Name for the dataframe (used in titles)
        figsize: Figure size tuple (width, height) - default 16:10 ratio

    Returns:
        InteractiveDataFrameInspector instance
    """
    inspector = InteractiveDataFrameInspector(df, name)

    # Display summary first
    inspector.display_summary()
    print("\n" + "=" * 60)

    # Display interactive dashboard
    inspector.display_dashboard()

    return inspector
