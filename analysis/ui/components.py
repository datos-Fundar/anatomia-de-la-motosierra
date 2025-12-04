import os
import requests
import warnings
from pathlib import Path
from datetime import datetime
from IPython.display import clear_output, display
from ipywidgets import (
    Button,
    Dropdown,
    SelectMultiple,
    Text,
    Textarea,
    IntText,
    FloatText,
    Output,
    VBox,
    HBox,
    Layout,
    Checkbox,
)

from chainsaw.plot import Plot
from chainsaw.model.tree import Tree
from chainsaw.model.tree_change import TreeChange
from chainsaw.db import SessionLocal
from chainsaw.clusters import Clusters
from chainsaw.borabot import download_all_official_norms
from sqlalchemy.exc import NoResultFound, MultipleResultsFound


STRUCTURE_DIR_PATH = "estructura"
FILES_PATH = os.path.join("..", "data", STRUCTURE_DIR_PATH)
warnings.filterwarnings("ignore", category=FutureWarning)


#################################################################
# Botones de descarga
#################################################################
def DownloadButtonsCreator():
    DownloadButtonsOutput = Output()

    DownloadCSVButton = Button(
        description="Descargar CSV",
        button_style="primary",
        layout=Layout(width='90%')
    )

    def __on_DownloadCSVButton_click(b):
        today = datetime.today().strftime('%Y_%m_%d')
        url = "https://mapadelestado.jefatura.gob.ar/back/api/datos.php?db=m&id=9&fi=csv"
        output_path = os.path.join(FILES_PATH, f"{today}.csv")
        os.makedirs(FILES_PATH, exist_ok=True)

        with DownloadButtonsOutput:
            response = requests.get(url)
            if response.status_code == 200:
                with open(output_path, "wb") as f:
                    f.write(response.content)
            else:
                print(f"Error al descargar: {response.status_code}")
    DownloadCSVButton.on_click(__on_DownloadCSVButton_click)

    DownloadBORAButton = Button(
        description="Descargar Boletines",
        button_style="primary",
        layout=Layout(width='90%')
    )

    def __on_DownloadBORAButton_click(b):
        with DownloadButtonsOutput:
            download_all_official_norms()
    DownloadBORAButton.on_click(__on_DownloadBORAButton_click)

    return VBox([DownloadCSVButton,
                 DownloadBORAButton,
                 DownloadButtonsOutput])


#################################################################
# Boton de Plotting
#################################################################
def PlotButton(data, method, plot_output, handle_click):
    button = Button(
        description=method.capitalize(),
        disabled=True,
        button_style="warning",
        layout=Layout(width='20%'),
    )

    def __on_PlotButton_click(b):
        date_identifier = getattr(b, "date_identifier", None)
        if date_identifier is None:
            with plot_output:
                clear_output(wait=True)
                print("No se encontraron datos cargados.")
            return

        df = data['df'].get(date_identifier)
        if df is None:
            with plot_output:
                clear_output(wait=True)
                print(f"No hay dataframe para '{date_identifier}'")
            return

        with plot_output:
            clear_output(wait=True)
            fig = getattr(Plot, method)(df, date=date_identifier, on_click=handle_click)
            display(fig)

    button.on_click(__on_PlotButton_click)
    return button


#################################################################
# Crear árbol en base al CSV elegido
#################################################################
def TreeCreator(data: dict, checkbox: Checkbox):
    def __list_csvs(path):
        return [f for f in os.listdir(path) if f.endswith(".csv")]

    CSVDropdown = Dropdown(
        options=__list_csvs(FILES_PATH),
        description="🔍 CSV:",
        layout=Layout(width='65%')
    )
    DateIdentifierText = Text(
        description='🗓️ Fecha:',
        placeholder='YYYY_MM_DD',
        layout=Layout(width='65%')
    )

    def __on_CSVDropdown_change(change):
        if change['type'] == 'change' and change['name'] == 'value':
            filename = Path(change['new']).name
            if filename.endswith(".csv"):
                fecha = filename.removesuffix(".csv")
                DateIdentifierText.value = fecha

    CSVDropdown.observe(__on_CSVDropdown_change)
    if CSVDropdown.value:
        __on_CSVDropdown_change({
            'type': 'change',
            'name': 'value',
            'new': CSVDropdown.value,
        })

    BuildTreeButton = Button(
        description="🌳 Crear Árbol",
        button_style="primary",
        layout=Layout(width='40%'),
    )
    BuildTreeOutput = Output()
    PlotOutput = Output()
    InteractiveOutput = Textarea(
        value="Hacé click en un nodo del gráfico ('icicle' o 'sunburst') para ver sus datos.",
        placeholder='',
        description='Info:',
        layout=Layout(width="100%", height="150px")
    )

    def handle_click(trace, points, state):
        if points.point_inds:
            i = points.point_inds[0]
            uuid_ = trace.ids[i]
            text_html = trace.text[i]

            path_2025 = None
            if DateIdentifierText.value.startswith("2023"):
                with SessionLocal() as session:
                    try:
                        path_2025 = session.query(TreeChange)\
                            .filter(TreeChange.uuid_2023 == uuid_)\
                            .with_entities(TreeChange.path_2025)\
                            .one()[0]
                        if path_2025:
                            path_2025 = f'\n\nPosible path 2025:\n{path_2025.replace(" -> ", "\n")}'
                        else:
                            path_2025 = "\n\n[Eliminado]"
                    except (NoResultFound, MultipleResultsFound):
                        path_2025 = ""
            else:
                path_2025 = ""
            path_txt = text_html.replace("<br>", "\n").replace("<b>", "").replace("</b>", "")
            InteractiveOutput.value = f"UUID: {uuid_}\nPATH:\n{path_txt}{path_2025}"

    IciclePlotButton = PlotButton(data, 'icicle', PlotOutput, handle_click)
    SunburstPlotButton = PlotButton(data, 'sunburst', PlotOutput, handle_click)

    def __on_BuildTreeButton_click(b):
        selected_csv_file = CSVDropdown.value
        filename = Path(selected_csv_file).name
        date_identifier = filename.removesuffix(".csv")
        path = os.path.join(FILES_PATH, selected_csv_file)

        with BuildTreeOutput:
            clear_output(wait=True)
            print(f"Creando árbol desde: {path}")
            with SessionLocal() as session:
                tree = Tree.load_or_create(path, session, checkbox.value)
                data['tree'][date_identifier] = tree
                data['df'][date_identifier] = tree.as_dataframe()
            print(f"El árbol fue guardado en data['tree']['{date_identifier}']")
            print(f"El dataframe fue guardado en data['df']['{date_identifier}']")

        for plot_button in [IciclePlotButton, SunburstPlotButton]:
            plot_button.date_identifier = date_identifier
            plot_button.disabled = False

    BuildTreeButton.on_click(__on_BuildTreeButton_click)
    TreeVBox = VBox([HBox([CSVDropdown, BuildTreeButton]),
                     HBox([DateIdentifierText, IciclePlotButton, SunburstPlotButton]),
                     HBox([BuildTreeOutput])])
    return TreeVBox, PlotOutput, InteractiveOutput


#################################################################
# Toolbar completo
#################################################################
def Toolbar(data: dict):
    CentralAdministrationCheckbox = Checkbox(
        value=True,
        description="Solo Administración Central",
        indent=False
    )

    tree_creator_1, plot_output_1, interactive_output_1 = TreeCreator(data, checkbox=CentralAdministrationCheckbox)
    tree_creator_2, plot_output_2, interactive_output_2 = TreeCreator(data, checkbox=CentralAdministrationCheckbox)
    ToolbarComplete = VBox([CentralAdministrationCheckbox,
                            HBox([DownloadButtonsCreator(), tree_creator_1, tree_creator_2])])
    InteractiveOutputs = HBox([interactive_output_1, interactive_output_2])
    Plots = HBox([plot_output_1, plot_output_2])
    return ToolbarComplete, Plots, InteractiveOutputs


#################################################################
# Selectores de árboles y creación del input para el algoritmo de clustering
#################################################################
def ClusterInputCreator(data, _id):
    TreeSelector = Dropdown(
        options=list(data["tree"].keys()) if data["tree"] else [],
        description="🌳 Árbol:",
        layout=Layout(width="99%")
    )
    UnitSelector = SelectMultiple(
        options=[],
        description='🏦 Unidades:',
        layout=Layout(width='99%', height='300px'),
        disabled=True
    )

    def on_TreeSelector_change(change):
        if change['type'] == 'change' and change['name'] == 'value':
            selected_tree_key = change['new']
            if selected_tree_key:
                tree = data['tree'][selected_tree_key]
                try:
                    del data[_id]
                except KeyError:
                    pass
                data[_id] = {'selected_tree': selected_tree_key}
                UnitSelector.options = tree.jurisdictions()
                UnitSelector.disabled = False
            else:
                del data[_id]
                UnitSelector.options = []
                UnitSelector.disabled = True
    TreeSelector.observe(on_TreeSelector_change)
    if TreeSelector.value:
        on_TreeSelector_change({
            'type': 'change',
            'name': 'value',
            'new': TreeSelector.value,
        })

    def on_UnitSelector_change(change):
        if change['type'] == 'change' and change['name'] == 'value':
            selected_units = change['new']
            if selected_units:
                data[_id]['parents'] = selected_units
            else:
                try:
                    del data[_id]['parents']
                except KeyError:
                    pass
    UnitSelector.observe(on_UnitSelector_change)
    if UnitSelector.value:
        on_UnitSelector_change({
            'type': 'change',
            'name': 'value',
            'new': UnitSelector.value,
        })

    return VBox([TreeSelector, UnitSelector])


#################################################################
# Boton de Cluster Plotting
#################################################################
def ClusterPlotCreator(data):
    FirstInput = ClusterInputCreator(data, 'first')
    LastInput = ClusterInputCreator(data, 'last')
    FirstInput.layout = Layout(flex='1 1 auto')
    LastInput.layout = Layout(flex='1 1 auto')

    PlotOutput = Output()
    
    OnlyFirstTreeCheckbox = Checkbox(
        value=True,
        description="Solo Primer Árbol",
        indent=False,
    )
    ConsiderSubstantiveUnitsCheckbox = Checkbox(
        value=True,
        description="Sustantivas",
        indent=False,
    )
    OnlyFirstTreeCheckbox.layout = Layout(flex='0.5 1 auto')
    ConsiderSubstantiveUnitsCheckbox.layout = Layout(flex='0.5 1 auto')

    ConsiderSupportUnitsCheckbox = Checkbox(
        value=False,
        description="Apoyo",
        indent=False,
    )
    ConsiderControlUnitsCheckbox = Checkbox(
        value=False,
        description="Control",
        indent=False,
    )
    ConsiderSupportUnitsCheckbox.layout = Layout(flex='0.5 1 auto')
    ConsiderControlUnitsCheckbox.layout = Layout(flex='0.5 1 auto')

    MinDFText = IntText(
        description='Mín. df:',
        placeholder=3,
    )
    MinDFText.value = 3
    MaxDFText = FloatText(
        description='Máx df:',
        placeholder=0.95,
    )
    MaxDFText.value = 0.95
    MinDFText.layout = Layout(flex='0.5 1 auto')
    MaxDFText.layout = Layout(flex='0.5 1 auto')

    RandomSeedText = IntText(
        description='Semilla:',
        placeholder=123,
    )
    RandomSeedText.value = 123
    ClusterSelectionEpsilonText = FloatText(
        description='Dist. ε máx:',
        placeholder=0.0,
    )
    ClusterSelectionEpsilonText.value = 0.0
    RandomSeedText.layout = Layout(flex='0.5 1 auto')
    ClusterSelectionEpsilonText.layout = Layout(flex='0.5 1 auto')

    MinClusterSizeText = IntText(
        description='Cluster mín:',
        placeholder=20,
    )
    MinClusterSizeText.value = 20
    MinSamplesText = IntText(
        description='Dens. mín:',
        placeholder=20,
    )
    MinSamplesText.value = 20
    MinClusterSizeText.layout = Layout(flex='0.5 1 auto')
    MinSamplesText.layout = Layout(flex='0.5 1 auto')

    ClusterGenerationButton = Button(
        description="Generar clusters",
        button_style="warning",
    )
    ClusterPlotButton = Button(
        description="Graficar clusters",
        disabled=True,
        button_style="warning",
    )
    ClusterGenerationButton.layout = Layout(flex='0.5 1 auto')
    ClusterPlotButton.layout = Layout(flex='0.5 1 auto')

    def __on_ClusterGenerationButton_click(b):
        try:
            data['first']['parents']
            data['last']['parents']
        except KeyError:
            if not OnlyFirstTreeCheckbox.value:
                with PlotOutput:
                    clear_output(wait=True)
                    print("Seleccione 2 árboles y sus respectivas unidades.")
                return
            else:
                pass

        with PlotOutput:
            clear_output(wait=True)
            print("Creando el dataframe de clusters.")
            with SessionLocal() as session:
                clusters_df = Clusters(
                    data,
                    data['first']['selected_tree'],
                    data['first']['parents'],
                    data['last']['selected_tree'] if not OnlyFirstTreeCheckbox.value else None,
                    data['last']['parents'] if not OnlyFirstTreeCheckbox.value else None,
                    session=session,
                    only_first_tree=OnlyFirstTreeCheckbox.value,
                    consider_substantive_units=ConsiderSubstantiveUnitsCheckbox.value,
                    consider_support_units=ConsiderSupportUnitsCheckbox.value,
                    consider_control_units=ConsiderControlUnitsCheckbox.value,
                    min_df=MinDFText.value,
                    max_df=MaxDFText.value,
                    min_cluster_size=MinClusterSizeText.value,
                    min_samples=MinSamplesText.value,
                    cluster_selection_epsilon=ClusterSelectionEpsilonText.value,
                    seed=RandomSeedText.value,
                )
                data['clusters'] = clusters_df
            ClusterPlotButton.disabled = False
            print("El dataframe de clusters fue guardado en data['clusters']")

    def __on_ClusterPlotButton_click(b):
        try:
            data['clusters']
        except KeyError:
            with PlotOutput:
                clear_output(wait=True)
                print("Primero genere el dataframe de clusters")
            return

        with PlotOutput:
            clear_output(wait=True)
            Plot.clusters_scatter(data)

    ClusterGenerationButton.on_click(__on_ClusterGenerationButton_click)
    ClusterPlotButton.on_click(__on_ClusterPlotButton_click)
    AllInputs = VBox([
        HBox([OnlyFirstTreeCheckbox, ConsiderSubstantiveUnitsCheckbox]),
        HBox([ConsiderSupportUnitsCheckbox, ConsiderControlUnitsCheckbox]),
        HBox([MinDFText, MaxDFText]),
        HBox([RandomSeedText, ClusterSelectionEpsilonText]),
        HBox([MinClusterSizeText, MinSamplesText]),
        HBox([ClusterGenerationButton, ClusterPlotButton]),
    ])
    TreeVBox = HBox([FirstInput, LastInput, AllInputs])
    return TreeVBox, PlotOutput
