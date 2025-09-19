# databricks-add-tag 🚀

Este projeto contém scripts Python para atualizar ou adicionar uma tag personalizada em dois tipos de recursos no Databricks: **SQL Warehouses** e **All Purpose Clusters**. É útil para organizar e categorizar ambientes conforme necessidades de equipes ou projetos.

## 👀 Visão Geral

- **sql_warehouse/**: Scripts para atualizar/adicionar tags em SQL Warehouses.
- **all_purpose_compute/**: Scripts para atualizar/adicionar tags em All Purpose Clusters.

A autenticação é feita usando variáveis de ambiente para o host e token do Databricks.

## 🛠️ Requisitos

- Python 3.x
- Permissões de acesso à API do Databricks
- Variáveis de ambiente configuradas:
  ```bash
  export DATABRICKS_HOST=<URL do workspace>
  export DATABRICKS_TOKEN=<token de acesso>
  ```

## 📦 Instalação

Clone o repositório e instale as dependências (se houver):

```bash
git clone https://github.com/seu-usuario/databricks-add-tag.git
cd databricks-add-tag
# Instale dependências, se necessário
```

## ▶️ Modo de Uso

### SQL Warehouses

1. Acesse o diretório `sql_warehouse/`.
2. Prepare um arquivo `warehouses.txt` contendo os IDs dos SQL Warehouses, um por linha.
3. Execute o script informando o arquivo e o valor da tag:

   ```bash
   python add_databricks_tag.py --file warehouses.txt --value "Engenharia de Dados"
   ```

### All Purpose Clusters

1. Acesse o diretório `all_purpose_compute/`.
2. Prepare um arquivo `clusters.txt` contendo os IDs dos clusters, um por linha.
3. Execute o script informando o arquivo e o valor da tag:

   ```bash
   python add_databricks_tag.py --file clusters.txt --value "Engenharia de Dados"
   ```

## ⚠️ Observações

- Certifique-se de que o token possui permissões suficientes para editar tags nos recursos.
- Os scripts não removem tags existentes, apenas adicionam ou atualizam o valor informado.
- Para dúvidas ou problemas, consulte a documentação oficial da API Databricks.

## 📄 Licença

Este projeto está sob a licença MIT.
