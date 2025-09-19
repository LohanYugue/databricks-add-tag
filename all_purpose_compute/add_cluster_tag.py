import argparse
import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.compute import AutoScale 

# Configuração de logging com emojis
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_cluster_id_by_name(w: WorkspaceClient, cluster_name: str) -> str | None:
    """Busca o ID de um cluster pelo seu nome."""
    try:
        logging.info(f"🔍 Procurando pelo ID do cluster com nome: '{cluster_name}'")
        all_clusters = w.clusters.list()
        for cluster in all_clusters:
            if cluster.cluster_name == cluster_name:
                logging.info(f"✅ Cluster '{cluster_name}' encontrado com ID: {cluster.cluster_id}")
                return cluster.cluster_id
    except Exception as e:
        logging.error(f"❌ Erro ao listar clusters: {e}")
    return None

def add_tag_to_cluster(cluster_identifier: str, tag_key: str, tag_value: str):
    """Adiciona ou atualiza uma tag em um cluster, preservando as existentes."""
    try:
        w = WorkspaceClient()
        cluster = None

        # Tenta obter o cluster pelo ID, depois pelo nome
        try:
            cluster = w.clusters.get(cluster_id=cluster_identifier)
            logging.info(f"✅ Cluster encontrado diretamente pelo ID: {cluster.cluster_id}")
        except NotFound:
            logging.warning(f"⚠️ Não foi possível encontrar o cluster pelo ID '{cluster_identifier}'. Tentando buscar por nome...")
            cluster_id_by_name = get_cluster_id_by_name(w, cluster_identifier)
            if cluster_id_by_name:
                cluster = w.clusters.get(cluster_id=cluster_id_by_name)
            else:
                logging.error(f"❌ ERRO: Cluster com nome '{cluster_identifier}' não foi encontrado.")
                return

        if not cluster:
            logging.error(f"❌ Não foi possível obter os detalhes do cluster '{cluster_identifier}'.")
            return
            
        # Para clusters, as tags já são um dicionário simples!
        existing_tags_dict = cluster.custom_tags or {}
        if existing_tags_dict:
            logging.info(f"🏷️  Tags existentes: {existing_tags_dict}")
        else:
            logging.info(f"ℹ️  O cluster '{cluster.cluster_name}' não possui tags existentes.")

        # Adicionar/atualizar nossa tag no DICIONÁRIO
        existing_tags_dict[tag_key] = tag_value
        logging.info(f"📤 Dicionário de tags a ser aplicado: {existing_tags_dict}")

        # O 'edit' de cluster exige que passemos a configuração principal de volta.
        autoscale_config = cluster.autoscale
        num_workers = cluster.num_workers
        if not autoscale_config:
            num_workers = cluster.num_workers or 0 
        
        w.clusters.edit(
            cluster_id=cluster.cluster_id,
            cluster_name=cluster.cluster_name,
            spark_version=cluster.spark_version,
            node_type_id=cluster.node_type_id,
            driver_node_type_id=cluster.driver_node_type_id or cluster.node_type_id,
            autoscale=autoscale_config,
            num_workers=num_workers,
            aws_attributes=cluster.aws_attributes,
            # >>> A CORREÇÃO FINAL ESTÁ AQUI: Preservamos o modo de segurança/acesso <<<
            data_security_mode=cluster.data_security_mode,
            custom_tags=existing_tags_dict
        )

        logging.info(f"🎉 SUCESSO: Tag '{tag_key}: {tag_value}' adicionada/atualizada com sucesso no cluster '{cluster.cluster_name}' (ID: {cluster.cluster_id})")

    except Exception as e:
        logging.error(f"❌ FALHA ao processar o cluster '{cluster_identifier}'. Erro: {e}", exc_info=True)

def main():
    parser = argparse.ArgumentParser(description="Adiciona uma tag a uma lista de All-Purpose Clusters do Databricks.")
    parser.add_argument("--file", required=True, help="Caminho para o arquivo com nomes ou IDs de clusters.")
    parser.add_argument("--value", required=True, help="O valor para a tag 'Dominio'.")
    args = parser.parse_args()
    tag_key_to_add = "Dominio"

    try:
        with open(args.file, 'r') as f:
            clusters_list = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logging.error(f"❌ Erro: O arquivo '{args.file}' não foi encontrado.")
        return

    logging.info(f"🚀 Iniciando o processo para {len(clusters_list)} cluster(s) do arquivo '{args.file}'...")
    for cluster_identifier in clusters_list:
        add_tag_to_cluster(cluster_identifier, tag_key_to_add, args.value)
        print("-" * 70)

if __name__ == "__main__":
    main()
