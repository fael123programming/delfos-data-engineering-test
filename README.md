# Delfos Data Engineering Test

Pipeline ETL com PostgreSQL, FastAPI, SQLAlchemy, Pandas e Dagster para gerar dados sintéticos, disponibilizá-los por uma API, calcular métricas estatísticas e carregá-las em um banco de dados de destino.

## Requisitos

- [Python 3.14](https://www.python.org/downloads/)
- [Docker e Docker Compose](https://docs.docker.com/compose/install/)

## Execução

Crie um ambiente virtual usando o interpretador do Python:

```cmd
python -m venv .venv
```

Ative esse mesmo ambiente virtual, executando um dos scripts abaixo, de acordo com o **tipo do terminal** e o **sistema operacional** utilizado:

Windows 

- CMD:

```cmd
.\.venv\Scripts\activate
```

- Powershell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Linux & macOS:

```bash
source .venv/bin/activate
```

Instale as dependências do projeto com o comando abaixo:

```cmd
python -m pip install -r requirements.txt
```

Crie o arquivo de configuração contendo as variáveis de ambiente usadas:

Windows:

```cmd
copy .env.example .env
```

Linux & macOS:

```bash
cp .env.example .env
```

Nota: os valores padrão de `.env.example` correspondem ao `compose.yaml`. Altere-os apenas se modificar portas, usuários ou bancos de dados.

Inicie os bancos de dados *source_db* e *target_db* com o utilitário do `docker compose`:

```cmd
docker compose up -d
```

Crie as tabelas dos bancos de dados executando o comando abaixo:

```cmd
python -m app.create_tables
```

Popule o banco de dados *source_db* (fonte) da seguinte forma:

```cmd
python -m app.seed
```

Nota: os dados gerados abrangem o período de `2026-07-15 00:00 UTC` a `2026-07-24 23:59 UTC`.

### API

Em um terminal com o ambiente virtual ativo, rode:

```cmd
uvicorn app.api:app --reload
```

A API `FastAPI` estará disponível em `http://127.0.0.1:8000`, publicando os dados de *source_db*, de forma fácil e rápida.

### ETL Manual

Com a API em execução, abra outro terminal, ative o ambiente virtual e informe uma data disponível dentro do intervalo apresentado na nota acima, por exemplo `2026-07-17`:

```cmd
python -m app.etl 2026-07-17
```

### Dagster

Valide as definições do Dagster:

```cmd
dagster definitions validate -m app.dagster_defs
```

Com a API em execução, inicie o Dagster em outro terminal:

```cmd
dagster dev -m app.dagster_defs
```

A interface estará disponível para acesso em `http://127.0.0.1:3000`.

Para executar o ETL manualmente, basta materializar uma partição diária do asset `daily_etl_asset`.

O schedule `daily_etl_job_schedule` pode ser consultado em `Automation > Schedules`. Os dados de demonstração estão limitados ao período de `2026-07-15` a `2026-07-24`.
