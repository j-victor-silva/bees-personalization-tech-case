# BEES Personalization Tech Case

Teste técnico para a vaga de Engenheiro de Dados Pleno.

## OpenBreweryDB PySpark Medallion Pipeline

Pipeline de dados em PySpark que extrai dados da API pública Open Brewery DB e os processa no esquema medalhão:
- Bronze: dados brutos (raw) com metadados de ingestão
- Silver: dados tipados a partir do JSON
- Gold: visão agregada (quantidade por tipo e localização)

Inclui orquestração opcional via Airflow, com tarefas de validação (gates) entre as etapas: a pipeline só avança se cada teste passar.

### Estrutura do projeto
- `src/openbrewerydb_pipeline/main.py`: execução local sequencial (extract → bronze → silver → gold)
- `src/openbrewerydb_pipeline/config/config.py`: leitura de configurações via CLI/variáveis de ambiente
- `src/openbrewerydb_pipeline/etl/api_client.py`: cliente HTTP (requests) com paginação e tratamento de erros
- `src/openbrewerydb_pipeline/etl/bronze.py`: construção e escrita da camada Bronze (Parquet particionado por `ingestion_date`)
- `src/openbrewerydb_pipeline/etl/silver.py`: parsing do JSON de `payload` para colunas tipadas (schema em `utils/transform.py`)
- `src/openbrewerydb_pipeline/etl/gold.py`: agregação por `brewery_type`, `country`, `state_province`, `city`
- `src/openbrewerydb_pipeline/utils/transform.py`: schema da API (Spark StructType)
- `airflow/dags/breweries_medallion_dag.py`: DAG com fluxo e testes-gate entre as etapas

### Requisitos
- Python 3.11 (recomendado usar pyenv)
- Java 17 (JDK/JRE 17)
- Pip/venv
- (Opcional) Docker e Docker Compose

### Instalação de dependências

#### Python 3.11 com pyenv
- Manjaro/Arch:
  ```bash
  sudo pacman -S --needed pyenv
  echo 'eval "$(pyenv init -)"' >> ~/.zshrc
  source ~/.zshrc
  pyenv install 3.11.9
  pyenv local 3.11.9
  python -V  # deve ser 3.11.x
  ```
- Ubuntu/Debian:
  ```bash
  curl https://pyenv.run | bash
  echo 'export PATH="$HOME/.pyenv/bin:$PATH"' >> ~/.bashrc
  echo 'eval "$(pyenv init -)"' >> ~/.bashrc
  source ~/.bashrc
  pyenv install 3.11.9
  pyenv local 3.11.9
  python -V
  ```

#### Java 17
- Manjaro/Arch:
  ```bash
  sudo pacman -S --needed jdk17-openjdk
  sudo archlinux-java set java-17-openjdk
  java -version
  ```
- Ubuntu/Debian:
  ```bash
  sudo apt-get update
  sudo apt-get install -y openjdk-17-jre-headless
  java -version
  ```
- Opcional (JAVA_HOME):
  ```bash
  # Arch
  echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk' >> ~/.zshrc
  # Ubuntu/Debian
  echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.zshrc
  echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc
  source ~/.zshrc
  ```

#### Ambiente Python e libs
```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

### Execução local (sem Airflow)
1) Defina o IP local do Spark para evitar erros de bind/host:
```bash
export SPARK_LOCAL_IP=127.0.0.1
```
2) Ajuste placeholders no Spark do `main.py` (se ainda existirem):
- Em `src/openbrewerydb_pipeline/main.py`, substitua `<IP_DO_SEU_COMPUTADOR>` por `127.0.0.1` em `spark.driver.host` e `spark.driver.bindAddress` (ou remova essas configs se não precisar).

3) Execute o pipeline:
```bash
python -m src.openbrewerydb_pipeline.main \
  --endpoint "https://api.openbrewerydb.org/v1/breweries" \
  --per-page 50 \
  --max-pages 5 \
  --bronze-output-path "data/datalake/bronze/openbrewerydb" \
  --silver-output-path "data/datalake/silver/openbrewerydb" \
  --gold-output-path "data/datalake/gold/openbrewerydb_aggregated"
```

### Execução com Airflow (local, sem Docker)
1) Preparar ambiente:
```bash
export AIRFLOW_HOME=$(pwd)/airflow
export PYTHONPATH=$(pwd)
mkdir -p "$AIRFLOW_HOME/dags" "$AIRFLOW_HOME/logs" "$AIRFLOW_HOME/data"
pip install 'apache-airflow==2.9.3' --constraint \
  'https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt'
```
2) Inicializar e criar usuário admin:
```bash
airflow db init
airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com
```
3) Iniciar serviços:
```bash
airflow webserver -p 8080 &
airflow scheduler &
```
4) Executar DAG:
- UI: `http://localhost:8080` → `breweries_medallion_pipeline`
- CLI: `airflow dags trigger breweries_medallion_pipeline`

Variáveis úteis (opcional):
- `ENDPOINT`, `PER_PAGE`, `MAX_PAGES`, `PAGE_PARAM`, `PER_PAGE_PARAM`
- `DATA_DIR` (default: `${AIRFLOW_HOME}/data`), `BRONZE_OUTPUT_PATH`, `SILVER_OUTPUT_PATH`, `GOLD_OUTPUT_PATH`
- `DAG_CRON` (cron do DAG)

### Execução via Docker (Airflow + PySpark)
1) Subir ambiente:
```bash
docker compose up --build -d
# UI: http://localhost:8080
```
2) Permissões dos volumes (se usar bind mounts):
```bash
sudo mkdir -p data logs
sudo chown -R 50000:0 data logs
sudo chmod -R 775 data logs
docker compose down
docker compose up -d --build
```

### Esquema de dados
- Bronze:
  - Colunas: `ingestion_timestamp`, `ingestion_date`, `source_system`, `payload` (JSON cru)
  - Particionado por: `ingestion_date`
- Silver:
  - Colunas tipadas: `id`, `name`, `brewery_type`, `address_1`, `address_2`, `address_3`, `street`, `city`, `state`, `state_province`, `country`, `postal_code`, `longitude`, `latitude`, `phone`, `website_url`
  - Particionado por: `country`, `state_province`
- Gold:
  - Agregação: contagem (`num_breweries`) por `brewery_type`, `country`, `state_province`, `city`

### Variáveis de ambiente suportadas
- Extração: `ENDPOINT`, `PER_PAGE`, `MAX_PAGES`, `PAGE_PARAM`, `PER_PAGE_PARAM`, `REQUEST_TIMEOUT_SECONDS`
- Saída: `BRONZE_OUTPUT_PATH`, `SILVER_OUTPUT_PATH`, `GOLD_OUTPUT_PATH`, `DATA_DIR`
- Airflow: `DAG_CRON`, `AIRFLOW_HOME`

### Troubleshooting
- PySpark + Python 3.12/3.13: use Python 3.11 (PySpark 3.5.1 não suporta 3.12/3.13).
- JDK 21+: use Java 17 (Spark 3.5.1 não suporta 21+ de forma estável).
- Erros de bind/host do Spark: defina `SPARK_LOCAL_IP=127.0.0.1` e ajuste `spark.driver.host/bindAddress` para `127.0.0.1`.
- Airflow não grava logs/data: ajuste permissões dos volumes (`chown 50000:0`, `chmod 775`) ou use volumes nomeados.
- DAG não aparece na UI: ative “Show paused DAGs”, limpe filtros, reinicie webserver/scheduler e confirme `AIRFLOW_HOME` e `dags_folder`.

### Testes (opcional)
- Rodar testes (se configurados):
  ```bash
  pip install pytest
  pytest -q
  ``` 