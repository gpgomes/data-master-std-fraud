# Roadmap: evolução para a AWS

> **Fora do escopo desta entrega.** O que está neste repositório, e é o que se avalia, é a plataforma
> completa rodando localmente com Docker Compose. A migração para a AWS está planejada e descrita
> abaixo como passo futuro; nada aqui foi provisionado ou aplicado numa conta AWS.

## Decisão de escopo

O briefing original ([`CaseFinancialDataLakeHouse.md`](../CaseFinancialDataLakeHouse.md), congelado)
previa duas versões: uma local e outra na AWS. A entrega ficou com a versão local, por três motivos:

- **Profundidade no lugar de duplicação.** O esforço foi para o que diferencia a plataforma: o
  detector de fraude medido (offline e online), os quality gates, o E2E com cenários de falha, a
  observabilidade com SLOs e a segurança no CI. Recriar a mesma arquitetura em serviços gerenciados
  não acrescentaria nada disso.
- **Reprodutibilidade.** Quem avalia sobe tudo com `make env && make up && make setup`, sem conta de
  nuvem nem custo. Os números citados nos documentos saem de comandos que rodam nessa stack.
- **Custo.** A stack completa ligada na AWS custa cerca de US$ 0,91 a 1,11 por hora (ver
  [Custo](#custo)). Mantê-la de pé para avaliação não cabe no orçamento definido para o projeto.

O trabalho de migração está na issue [#17](https://github.com/gpgomes/data-master-std-fraud/issues/17)
(backlog). Existe um esqueleto Terraform exploratório na branch
[`feat/issue-17-terraform-skeleton`](https://github.com/gpgomes/data-master-std-fraud/tree/feat/issue-17-terraform-skeleton),
fora da `main` de propósito: bootstrap do state remoto, módulos de S3, IAM e Budgets, ambiente `dev` e
testes de guardrail (IAM sem curinga, recursos caros desligados por padrão, nenhum segredo). Ele
passou por `terraform validate`, `tflint` e `checkov` localmente, mas nunca foi aplicado.

## Mapa: componente local → serviço AWS

| Camada | Hoje (local) | Na AWS |
|---|---|---|
| Object storage (Bronze/Silver/Gold) | MinIO | Amazon S3 |
| Mensageria | Kafka + Zookeeper | Amazon MSK (provisionado, 2 × `kafka.t3.small`) |
| Processamento batch | Cluster Spark 4.2 | EMR Serverless |
| Processamento streaming | Spark Structured Streaming | EMR em EC2 (1 primário + 1 core) |
| Orquestração | Airflow 3 (Docker) | Amazon MWAA |
| Serving SQL | PostgreSQL | Amazon RDS for PostgreSQL; Athena para consultas direto no S3 |
| Catálogo | Registro versionado (`docs/data_catalog.md`) | AWS Glue Data Catalog (tabelas declaradas no Terraform) |
| Dashboards | Superset | Amazon QuickSight, ou Superset em contêiner |
| API | FastAPI (Docker) | Contêiner (ECS/Fargate) atrás de API Gateway |
| Segredos | `make env` gera no `.env` local | AWS Secrets Manager |
| Infraestrutura | Docker Compose | Terraform |

## O que já está pronto para a migração

- **Configuração por variável de ambiente.** Endpoints, buckets, tópicos e credenciais vêm do
  `src/common/config.py` (pydantic-settings); nenhum job tem endereço fixo no código.
- **Cliente S3 atual.** O Spark 4.2 usa `hadoop-aws` 3.5 com o AWS SDK v2, a mesma pilha que
  acessa o S3 de verdade. A troca do MinIO pelo S3 é de configuração, não de biblioteca.
- **Jobs independentes do ambiente.** Os jobs PySpark recebem caminhos e parâmetros; o detector de
  fraude é um núcleo puro (DataFrame → DataFrame), sem dependência do cluster local.
- **Idempotência.** Batch por overwrite dinâmico de partição e micro-batches com marcadores de
  progresso: as mesmas garantias valem num reprocessamento na nuvem (precisam ser revalidadas no S3).
- **Segurança.** Nenhum segredo versionado; scans (gitleaks, pip-audit, trivy) já bloqueiam no CI.

## O que muda no código

| Ponto | Hoje | Na AWS |
|---|---|---|
| Acesso ao storage | Endpoint do MinIO, chave estática, path-style, sem TLS | Sem endpoint, credencial por role (instance profile/IRSA), TLS; avaliar `s3://` (EMRFS) e um committer otimizado, porque o overwrite dinâmico de partição no S3 é lento e não atômico |
| Kafka | Sem autenticação, `replication_factor=1` | TLS + IAM (`aws-msk-iam-auth` no Spark e nos producers), fator de replicação 2 ou mais |
| DAGs | `SparkSubmitOperator` no cluster local | Operadores do EMR Serverless, com variante por ambiente para manter o local funcionando |
| Tópicos e buckets | Criados pelo `make setup` | Declarados no Terraform |
| Serving | Postgres no compose | RDS, com a senha no Secrets Manager |

## Ordem planejada

1. **Fundação:** state remoto, IAM, buckets S3 e alerta de orçamento (o esqueleto da branch).
2. **Portabilidade do código** (testável localmente com MinIO, sem conta AWS): os itens da tabela
   acima, atrás de configuração.
3. **Rede:** VPC, subnets privadas, endpoint de S3 (NAT só quando necessário).
4. **MSK**, depois **EMR** (Serverless para o batch, EC2 para o streaming), depois **MWAA**.
5. **Glue + Athena** sobre os Parquet do S3.
6. **Depois:** QuickSight, deploy contínuo (CD) e repetir na nuvem o E2E e o teste de carga.

Cada serviço caro entra atrás de uma flag `enable_*` desligada por padrão.

## Custo

Região `us-east-1` (cerca de 1,6× mais barata que `sa-east-1`), um único ambiente `dev` e teto de
**US$ 50 por mês**. Valores da AWS Price List, consultada em 23/09/2026:

| Recurso | Preço | Uso previsto |
|---|---|---|
| S3, IAM, Glue, Athena, Budgets | ~US$ 7/mês | Sempre ligados |
| MWAA (classe micro / small) | US$ 0,29 / 0,49 por hora | Ligar, testar, destruir |
| MSK (2 × `kafka.t3.small`) | US$ 0,09 por hora | Ligar, testar, destruir |
| EMR Serverless | US$ 0,0526 por vCPU-hora + US$ 0,0058 por GB-hora | Zero quando ocioso |
| EMR em EC2 (`m5.xlarge`, por nó) | US$ 0,24 por hora | Ligar, testar, destruir |
| NAT Gateway | US$ 0,045 por hora | Só quando necessário |
| **Stack completa ligada** | **~US$ 0,91 a 1,11 por hora** | ~30 h/mês dentro do teto |

O MSK Serverless (US$ 0,75 por hora fixo) sai mais caro que o provisionado para esse volume. Redshift
Serverless e QuickSight ficam para o fim, fora do orçamento inicial. O AWS Budgets só alerta (a
cobrança chega com horas de atraso), então a disciplina de destruir o que é efêmero faz parte do plano.

## Pontos em aberto

- Limites da classe micro do MWAA não verificados (se não servir, a small cabe em ~30 h/mês).
- Versão do Airflow suportada pelo MWAA na época da migração (o projeto usa Airflow 3.3).
- Revalidar no S3 a idempotência dos micro-batches, hoje validada só no MinIO.
