import random
import hdbscan
import pandas as pd
from sklearn.manifold import TSNE
from sklearn.feature_extraction.text import TfidfVectorizer

from chainsaw.model.node import Unit
from chainsaw.model.official_document import Objective, Prompt


def Clusters(
    data,
    first_tree_name,
    first_parents,
    last_tree_name,
    last_parents,
    session,
    only_first_tree,
    consider_substantive_units,
    consider_support_units,
    consider_control_units,
    min_df=3,
    max_df=0.95,
    min_cluster_size=20,
    min_samples=20,
    cluster_selection_epsilon=0.0,
    seed: int = 123
):
    random.seed(seed)
    excluded_units = (
        "Presidencia de la Nación",
        "Vicepresidencia de la Nación",
        "Agrupación Seguridad e Inteligencia",
        "Agrupación Técnica",
        "Comisión Nacional de Límites Internacionales",
        "Edecanes",
    )

    def __units_from(tree, parents: list[str]):
        return list(set(
            node for node in tree.nodes
            if isinstance(node, Unit)
            and not (node.name in excluded_units)
            and (
                (consider_substantive_units and (node.unit_class == "Sustantiva")) or
                (consider_support_units and (node.unit_class == "Apoyo")) or
                (consider_control_units and (node.unit_class == "Control")))
            and (node.range in (
                'Ministerio',
                'Secretaría',
                'Subsecretaría',
                'Jefatura Gabinete de Ministros',
                'Escribanía',
                'Dirección Nacional - General', 'Dirección Primer Nivel Operativo', 'Dirección Segundo Nivel Operativo', 'Coordinación',
                ''
                ))
            and (path := tree.path_to(node.uuid))
            and any(each in path for each in parents)
        ))

    def __make_tf_idf_for(corpus):
        vectorizer = TfidfVectorizer(
            ngram_range=(1, 3),
            min_df=min_df,
            max_df=max_df,
            lowercase=True,
        )
        tfidf_matrix = vectorizer.fit_transform(corpus)
        return tfidf_matrix, vectorizer

    def __clusters_for(tfidf_matrix):
        X_embedded = TSNE(
            n_components=2,
            random_state=seed,
            metric="cosine"
        ).fit_transform(tfidf_matrix.toarray())
        cluster_labels = hdbscan.HDBSCAN(
            min_cluster_size=min_cluster_size,
            min_samples=min_samples,
            cluster_selection_epsilon=cluster_selection_epsilon,
            allow_single_cluster=False,
        ).fit_predict(X_embedded)

        return pd.DataFrame({
            'x': X_embedded[:, 0],
            'y': X_embedded[:, 1],
            'cluster': cluster_labels,
        })

    first_tree = data['tree'][first_tree_name]
    first_units = __units_from(first_tree, parents=first_parents)
    to_process = [(first_units, first_tree, first_tree_name)]
    data['first']['units_amount'] = len(first_units)

    if not only_first_tree:
        last_tree = data['tree'][last_tree_name]
        last_units = __units_from(last_tree, parents=last_parents)
        to_process.append((last_units, last_tree, last_tree_name))
        data['last']['units_amount'] = len(last_units)

    corpus = []
    names = []
    uuids = []
    paths = []
    dates = []
    jurisdictions = []

    for units, tree, tree_name in to_process:
        for unit in units:
            path = tree.path_to(unit.uuid)
            objective = session.query(Objective).filter(
                Prompt.unit_uuid == unit.uuid,
                Prompt.tree_id == tree.id,
                Prompt.id == Objective.prompt_id,
            ).first()
            if objective is None:
                continue

            corpus.append(objective.text)
            names.append(unit.name)
            uuids.append(unit.uuid)
            paths.append("<br>".join(path))
            dates.append(tree_name)
            jurisdictions.append(path[0] if len(path) == 1 else path[1])

    tfidf_matrix, _ = __make_tf_idf_for(corpus)
    clusters_df = __clusters_for(tfidf_matrix)
    clusters_df['name'] = names
    clusters_df['uuid'] = uuids
    clusters_df['path'] = paths
    clusters_df['date'] = dates
    clusters_df['jurisdiction'] = jurisdictions
    return clusters_df
