import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import alphashape
from shapely.geometry import Polygon, MultiPolygon


class Plot:
    @classmethod
    def icicle(cls, dataframe, date=None, on_click=None):
        fig = go.FigureWidget(go.Icicle(
            ids=dataframe["uuid"].astype(str),
            parents=dataframe["parent"].astype(str).fillna(""),
            labels=dataframe["name"],
            textinfo="label",
            text=dataframe['path'],
            hovertemplate="%{text}<extra></extra>",
            maxdepth=2,
            name=date,
            root_color="lightgrey"
        ))
        fig.update_layout(
            title=f"Estructura {date}",
            margin=dict(t=50, l=25, r=25, b=25),
            height=1500,
        )
        fig.data[0].on_click(on_click)
        return fig

    @classmethod
    def sunburst(cls, dataframe, date=None, on_click=None):
        fig = go.FigureWidget()
        fig.add_trace(
            go.Sunburst(
                ids=dataframe['uuid'],
                labels=dataframe['name'],
                textinfo="label",
                parents=dataframe['parent'],
                insidetextorientation='radial',
                text=dataframe['path'],
                hovertemplate="%{text}<extra></extra>",
                maxdepth=2,
                name=date,
            )
        )
        fig.update_layout(
            title=f"Estructura {date}",
            margin=dict(t=0, l=0, r=0, b=0),
            height=800,
            dragmode="zoom",
        )
        fig.data[0].on_click(on_click)
        return fig

    @classmethod
    def clusters_scatter(cls, data):
        df_clusters = data['clusters']

        sorted_dates = sorted(
            df_clusters['date'].unique(),
            key=lambda d: pd.to_datetime(d, format="%Y_%m_%d"),
        )
        date_border_colors = {sorted_dates[0]: "#50B1F7"}
        if len(sorted_dates) > 1:
            date_border_colors[sorted_dates[1]] = "#6032AE"
            title = f"Clusters de organismos del Estado ({data['first']['selected_tree']}: {data['first']['units_amount']} uds. vs {data['last']['selected_tree']}: {data['last']['units_amount']} uds.)"
        else:
            title = f"Clusters de organismos del Estado ({data['first']['selected_tree']}: {data['first']['units_amount']} uds.)"

        cluster_palette = px.colors.qualitative.Plotly
        all_jurisdictions = sorted(df_clusters['jurisdiction'].unique())
        cluster_colors = {
            label: cluster_palette[i % len(cluster_palette)]
            for i, label in enumerate(all_jurisdictions)
        }

        cluster_ids = [cid for cid in sorted(df_clusters['cluster'].unique()) if cid != -1]
        hull_palette = px.colors.qualitative.Dark24
        cluster_hull_colors = {
            cid: hull_palette[i % len(hull_palette)]
            for i, cid in enumerate(cluster_ids)
        }

        fig = go.Figure()
        for cluster_id in cluster_ids:
            df_cluster = df_clusters[df_clusters['cluster'] == cluster_id]
            points = df_cluster[['x', 'y']].values

            if len(points) >= 3:
                alpha_shape = alphashape.alphashape(points, 0.1)
                polygons = [alpha_shape] if isinstance(alpha_shape, Polygon) else list(alpha_shape.geoms)

                for poly in polygons:
                    x, y = poly.exterior.xy
                    x = list(x)
                    y = list(y)
                    fig.add_trace(go.Scatter(
                        x=x,
                        y=y,
                        mode='lines',
                        line=dict(color=cluster_hull_colors[cluster_id], width=2, dash="dot"),
                        fill='toself',
                        fillcolor=cluster_hull_colors[cluster_id],
                        opacity=0.1,
                        name=f"Cluster {cluster_id}"
                    ))

        for date in df_clusters['date'].unique():
            df_date = df_clusters[df_clusters['date'] == date]
            border_color = date_border_colors.get(date)

            for label in df_date['jurisdiction'].unique():
                df_subset = df_date[df_date['jurisdiction'] == label]

                fig.add_trace(go.Scatter(
                    x=df_subset['x'],
                    y=df_subset['y'],
                    mode='markers',
                    name=f"{date} - {label}",
                    marker=dict(
                        size=10,
                        color=cluster_colors[label],
                        line=dict(width=3, color=border_color),
                        opacity=1,
                    ),
                    customdata=df_subset["path"],
                    hovertemplate="%{customdata}<extra></extra>",
                ))

        fig.update_layout(
            height=800,
            title=title,
            legend_title_text='Fecha y Jurisdicción'
        )

        fig.show()
