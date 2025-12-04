def units_on_cluster(clusters_json, cluster_id):
    unidades = []
    for unidad in clusters_json.get("clusters_data", []):
        if unidad.get("cluster") == cluster_id:
            uuid = unidad.get("uuid")
            unidades.append({
                "uuid": uuid,
                "name": unidad.get("name"),
                "jurisdiction": unidad.get("jurisdiction"),
                "path": unidad.get("path"),
                "objective": clusters_json.get("objectives", {}).get(uuid)
            })
    return sorted(unidades, key=lambda u: u["path"])
