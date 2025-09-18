import argparse
import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

# Configuração de logging para um output claro
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_warehouse_id_by_name(w: WorkspaceClient, warehouse_name: str) -> str | None:
    """Busca o ID de um SQL Warehouse pelo seu nome."""
    try:
        logging.info(f"Procurando pelo ID do warehouse com nome: '{warehouse_name}'")
        all_warehouses = w.warehouses.list()
        for wh in all_warehouses:
            if wh.name == warehouse_name:
                logging.info(f"Warehouse '{warehouse_name}' encontrado com ID: {wh.id}")
                return wh.id
    except Exception as e:
        logging.error(f"Erro ao listar warehouses: {e}")
    return None

def add_tag_to_warehouse(warehouse_identifier: str, tag_key: str, tag_value: str):
    """Adiciona ou atualiza uma tag em um SQL Warehouse, preservando as existentes."""
    try:
        w = WorkspaceClient()
        warehouse = None

        # Tenta obter o warehouse primeiro pelo ID, depois pelo nome
        try:
            warehouse = w.warehouses.get(id=warehouse_identifier)
            logging.info(f"Warehouse encontrado diretamente pelo ID: {warehouse.id}")
        except NotFound:
            logging.warning(f"Não foi possível encontrar o warehouse pelo ID '{warehouse_identifier}'. Tentando buscar por nome...")
            warehouse_id_by_name = get_warehouse_id_by_name(w, warehouse_identifier)
            if warehouse_id_by_name:
                warehouse = w.warehouses.get(id=warehouse_id_by_name)
            else:
                logging.error(f"ERRO: Warehouse com nome '{warehouse_identifier}' não foi encontrado.")
                return

        if not warehouse:
            logging.error(f"Não foi possível obter os detalhes do warehouse '{warehouse_identifier}'.")
            return

        # --- CORREÇÃO FINAL DA LÓGICA ---
        # 1. Criamos um dicionário vazio para armazenar as tags.
        existing_tags_dict = {}
        
        # 2. Verificamos se as tags existem. A API retorna uma LISTA de objetos.
        if warehouse.tags and warehouse.tags.custom_tags:
            tag_list_from_api = warehouse.tags.custom_tags
            logging.info(f"Tags existentes (formato API): {tag_list_from_api}")
            
            # 3. CONVERTEMOS a lista de objetos em um dicionário Python.
            for tag_pair in tag_list_from_api:
                existing_tags_dict[tag_pair.key] = tag_pair.value
            logging.info(f"Tags convertidas para dicionário: {existing_tags_dict}")
        else:
            logging.info(f"O warehouse '{warehouse.name}' não possui tags existentes.")

        # 4. Adicionamos/atualizamos nossa nova tag no DICIONÁRIO.
        existing_tags_dict[tag_key] = tag_value
        logging.info(f"Dicionário de tags a ser aplicado: {existing_tags_dict}")

        # 5. Enviamos o DICIONÁRIO completo de volta no formato que a API de edição espera.
        w.warehouses.edit(
            id=warehouse.id,
            tags={'custom_tags': existing_tags_dict}
        )

        logging.info(f"SUCESSO: Tag '{tag_key}: {tag_value}' adicionada/atualizada com sucesso no warehouse '{warehouse.name}' (ID: {warehouse.id})")

    except Exception as e:
        logging.error(f"FALHA ao processar o warehouse '{warehouse_identifier}'. Erro: {e}", exc_info=True)

def main():
    parser = argparse.ArgumentParser(description="Adiciona uma tag a uma lista de SQL Warehouses do Databricks.")
    parser.add_argument("--file", required=True, help="Caminho para o arquivo com nomes ou IDs de SQL Warehouses.")
    parser.add_argument("--value", required=True, help="O valor para a tag 'Dominio'.")
    
    args = parser.parse_args()
    tag_key_to_add = "Dominio"

    try:
        with open(args.file, 'r') as f:
            warehouses = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logging.error(f"Erro: O arquivo '{args.file}' não foi encontrado.")
        return

    logging.info(f"Iniciando o processo para {len(warehouses)} warehouse(s) do arquivo '{args.file}'...")
    
    for wh_identifier in warehouses:
        add_tag_to_warehouse(wh_identifier, tag_key_to_add, args.value)
        print("-" * 50)

if __name__ == "__main__":
    main()
