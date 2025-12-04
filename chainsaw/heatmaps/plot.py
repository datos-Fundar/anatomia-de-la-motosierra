import json
from typing import Dict, List, Any, Set, Optional
from collections import Counter
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from chainsaw.heatmaps.utils import units_on_cluster
from chainsaw.heatmaps.constants import JURISDICTION_COLORS
from chainsaw.heatmaps.dimensions import Dimension
from chainsaw.heatmaps.constants import DimensionName


def _final_matrix(
    units_order: Dict[str, Dict[str, Any]],
    matrixes_by_dimension: Dict[str, List[List[float]]],
    dimensions_weights: Dict[str, float],
    strict: bool = True,
):
    total = len(units_order)
    final_matrix = [[0.0 for j in range(total)] for i in range(total)]

    for i in range(total):
        for j in range(total):
            ignore = False
            partials = []
            for dimension in dimensions_weights.keys():
                partial = matrixes_by_dimension[dimension][i][j]
                if partial == 0.0 and strict:
                    ignore = True
                    break
                elif partial == 0.0 and not strict:
                    partials.append(0.0)
                elif partial < 0:
                    partials.append(0.0)
                else:
                    partials.append(partial)
            
            if ignore and strict:
                final_matrix[i][j] = 0.0
            else:
                final_matrix[i][j] = sum(partials)
    return final_matrix


def __abbreviate(name: str, max_len: int = 45) -> str:
    return name if len(name) <= max_len else name[:max_len - 3] + "..."


def __create_figure(
    cluster_id: int,
    matrix: List[List[float]],
    labels: List[str],
    label_colors: List[str],
    filtered_jurisdictions: Set[str],
    width: int = 1100,
    height: int = 700,
):
    fig = go.Figure(data=go.Heatmap(
        z=matrix,
        x=labels,
        y=labels,
        zmin=0,
        zmax=1,
        colorscale="thermal",
    ))
    
    fig.update_layout(
        title=f"Heatmap (cluster {cluster_id} - {len(matrix)} unidades)",
        width=width,
        height=height,
        xaxis=dict(tickangle=90),
    )

    fig.update_xaxes(
        tickangle=90,
        tickmode="array",
        tickvals=list(range(len(labels))),
        ticktext=[
            f"<span style='color:{c}'>{t}</span>" 
            for t, c in zip(labels, label_colors)
        ]
    )

    fig.update_yaxes(
        tickmode="array",
        tickvals=list(range(len(labels))),
        ticktext=[
            f"<span style='color:{c}'>{t}</span>" 
            for t, c in zip(labels, label_colors)
        ]
    )

    for j in sorted(filtered_jurisdictions):
        fig.add_trace(go.Scatter(
            x=[None], y=[None],
            mode="markers",
            marker=dict(size=10, color=JURISDICTION_COLORS[j]),
            legendgroup=j,
            showlegend=True,
            name=__abbreviate(j)
        ))

    fig.update_layout(
        legend=dict(
            orientation="v",
            yanchor="bottom",       
            y=-0.4,
            xanchor="left",
            x=-0.7,
        ),
    )
    
    fig.show()


def __create_grid_figure(
    cluster_id: int,
    final_matrix: List[List[float]],
    matrixes: Dict[str, List[List[float]]],
    labels: List[str],
    label_colors: List[str],
    dimensions_weights: Dict[str, float],
    filtered_jurisdictions: Set[str],
    width: int = 1100,
    height: int = 1400,
):
    fig = make_subplots(
        rows=3,
        cols=2,
        subplot_titles=["Matriz final", ""] + [f"{name} ({dimensions_weights[name]*100}%)" for name in matrixes.keys()],
        vertical_spacing=0.2,
        horizontal_spacing=0.1,
    )

    # Fila 1, columna 1: final_matrix
    fig.add_trace(
        go.Heatmap(
            z=final_matrix,
            x=labels,
            y=labels,
            zmin=0,
            zmax=1,
            colorscale="thermal",
            colorbar=dict(title="score")
        ),
        row=1, col=1
    )
    
    # Fila 1, columna 2: leyenda (heatmap vacío)
    for j in sorted(filtered_jurisdictions):
        fig.add_trace(go.Scatter(
            x=[None], y=[None],
            mode="markers",
            marker=dict(size=10, color=JURISDICTION_COLORS[j]),
            legendgroup=j,
            showlegend=True,
            name=__abbreviate(j)
        ))

    # Agregar los 4 matrices de matrixes en filas 2 y 3
    matrices_list = list(matrixes.items())
    for idx, (name, matrix) in enumerate(matrices_list):
        row = 2 + idx // 2
        col = 1 + idx % 2
        fig.add_trace(
            go.Heatmap(
                z=matrix,
                x=labels,
                y=labels,
                zmin=0,
                zmax=1,
                colorscale="thermal",
                colorbar=dict(title="score")
            ),
            row=row, col=col
        )

    for row in range(1, 4):
        for col in range(1, 3):
            fig.update_xaxes(
                row=row, col=col,
                tickangle=90,
                tickmode="array",
                tickvals=list(range(len(labels))),
                ticktext=[f"<span style='color:{c}'>{t}</span>" for t, c in zip(labels, label_colors)]
            )

            fig.update_yaxes(
                row=row, col=col,
                tickmode="array",
                tickvals=list(range(len(labels))),
                ticktext=[f"<span style='color:{c}'>{t}</span>" for t, c in zip(labels, label_colors)],
                showticklabels=False if col == 2 else True
            )

    fig.update_layout(
        title=f"Heatmap (cluster {cluster_id} - {len(final_matrix)} unidades)",
        width=width,
        height=height,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=0.99,
            xanchor="right",
        ),
    )

    fig.show()


def partial_matrixes(
    clusters_file_name: str,
    cluster_id: int,
    tree_file: str,
    central_administration_only: bool,
    threshold: float,
    dimensions: List[str],
):
    with open(f"heatmaps/{clusters_file_name}_id_{cluster_id}.json", "r", encoding="utf-8") as f:
        llm_results = json.load(f)

    with open(f"clusters/{clusters_file_name}.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    cluster_units = units_on_cluster(data, cluster_id)
    units_order = {unit["uuid"]: {"idx": idx, "unit": unit} for idx, unit in enumerate(cluster_units)}

    params = (llm_results, tree_file, central_administration_only, threshold)
    return {dimension_name: subclass(*params).partial_matrix(units_order)
            for dimension_name in dimensions
            if (subclass:=Dimension.working_on(dimension_name))}, units_order


def _apply_dimension_weights(
    all_matrixes: Dict[int, Dict[str, Any]],
    dimensions_weights: Dict[str, float],
    max_value: Optional[float] = None,
) -> Dict[int, Dict[str, Any]]:
    modified_matrixes = all_matrixes.copy()
    real_max_value = 0

    for cluster_id, matrixes_and_units in modified_matrixes.items():
        matrixes_by_dimension = matrixes_and_units["matrixes"]
        for dimension_name, matrix in matrixes_by_dimension.items():
            weight = dimensions_weights[dimension_name]
            if dimension_name == DimensionName.DISTANCE.value:
                current_max_value = max(max(row) for row in matrix)
                real_max_value = current_max_value if current_max_value > real_max_value else real_max_value
            matrix = [[cell * weight for cell in row] for row in matrix]
            modified_matrixes[cluster_id]["matrixes"][dimension_name] = matrix
    
    _max_value = real_max_value if max_value is None else max_value
    for cluster_id, matrixes_and_units in modified_matrixes.items():
        matrixes_by_dimension = matrixes_and_units["matrixes"]
        original_matrix = matrixes_by_dimension[DimensionName.DISTANCE.value]
        new_matrix = [[cell / _max_value for cell in row] for row in original_matrix]
        matrixes_by_dimension[DimensionName.DISTANCE.value] = new_matrix
    return modified_matrixes


def heatmap(
    clusters_file_name: str,
    cluster_id: int,
    tree_file: str,
    central_administration_only: bool,
    ellipsis: int = 40,
    threshold: float = 0.8,
    dimensions_weights: Dict[str, float] = {"objetivos": 0.4, "distancia": 0.3, "destinatarios": 0.2, "ambitos": 0.1},
    width: int = 1100,
    height: int = 1400,
    full: bool = True,
    strict: bool = True,
):
    matrixes, units_order = partial_matrixes(
        clusters_file_name,
        cluster_id,
        tree_file,
        central_administration_only,
        threshold,
        list(dimensions_weights.keys()),
    )

    all_matrixes_data = {
        cluster_id: {
            "matrixes": matrixes,
            "units_order": units_order,
        }
    }

    all_matrixes_data = _apply_dimension_weights(
        all_matrixes_data,
        dimensions_weights,
    )
    matrixes = all_matrixes_data[cluster_id]["matrixes"]
    
    matrix = _final_matrix(
        units_order,
        matrixes,
        dimensions_weights,
        strict,
    )
    name_counts = Counter(each["unit"]["name"] for each in units_order.values())

    labels = []
    label_colors = []
    filtered_jurisdictions = {
        unit["jurisdiction"] 
        for unit_data in units_order.values()
        if (unit:=unit_data["unit"])
        and unit["jurisdiction"] in JURISDICTION_COLORS
    }
    
    uuid_to_name = {
        uuid: unit["name"] if name_counts[unit["name"]] == 1 else f"{unit["name"]} - {unit["jurisdiction"]}"
        for uuid, unit_data in units_order.items() if (unit:=unit_data["unit"])
    }
    for uuid, unit_data in sorted(units_order.items(), key=lambda x: x[1]["idx"]):
        unit = unit_data["unit"]
        name = __abbreviate(uuid_to_name[uuid], ellipsis)
        labels.append(name)
        label_colors.append(JURISDICTION_COLORS.get(unit["jurisdiction"]))

    if full:
        __create_grid_figure(
            cluster_id,
            matrix,
            matrixes,
            labels,
            label_colors,
            dimensions_weights,
            filtered_jurisdictions,
            width=width,
            height=height,
        )
    else:
        __create_figure(
            cluster_id,
            matrix,
            labels,
            label_colors,
            filtered_jurisdictions,
            width=width,
            height=height,
        )
