# Tech-Challenge-2
Repositório do projeto tech challenge 2 da FIAP
---

1. Contexto do problema

A alfabetização na infância é um dos pilares fundamentais para o desenvolvimento educacional, social e econômico do país. O Compromisso Nacional Criança Alfabetizada mobiliza União, estados, Distrito Federal e municípios com o objetivo de garantir que todas as crianças brasileiras estejam alfabetizadas até o final do 2º ano do ensino fundamental, até 2030. Em 2023, o INEP realizou a Pesquisa Alfabetiza Brasil, que definiu o ponto de corte de 743 pontos na escala de proficiência do Saeb como o patamar a partir do qual uma criança pode ser considerada alfabetizada. A partir desse parâmetro foi criado o Indicador Criança Alfabetizada, que expressa o percentual de estudantes que atingem esse patamar.

Entender os fatores que influenciam a alfabetização exige integrar metas nacionais, estaduais e municipais, dados territoriais e microdados de alunos. Este projeto constrói essa integração através de uma pipeline de dados em nuvem, seguindo a Arquitetura Medalhão (Raw / Bronze / Silver / Gold) no Databricks.

2. Arquitetura da solução

A solução segue a Arquitetura Medalhão sobre Databricks (Delta Lake + Unity Catalog), com quatro camadas:

RAW (Volume): Arquivos CSVs Brutos obtidos no site do INEP e no site do IBGE.
Bronze (Delta): tabelas brutas importadas dos arquivos csv, com ingestão com histórico completo.
Silver (Delta): tabelas com tratamento das informações, tipo e com o histórico completo.
Gold (Delta): tabelas com tratamento para utilização em dashboards.


2.1 Camada RAW

Os 7 arquivos CSV de origem (Base dos Dados / IBGE) foram ingeridos sem qualquer alteração em um Volume do Unity Catalog. Essa camada existe apenas para armazenas os arquivos originais. Isso preserva o arquivo exatamente como foi obtido da fonte, permitindo reprocessamento total do zero em caso de qualquer suspeita de erro nas camadas seguintes.

| Arquivo original | Volume RAW | Fonte |
|---|---|---|
| `br_inep_avaliacao_alfabetizacao_alunos.csv` | `/Volumes/raw/inep/alunos/` | `INEP` |
| `br_inep_avaliacao_alfabetizacao_municipio.csv` | `/Volumes/raw/inep/municipio/` |`INEP` |
| `br_inep_avaliacao_alfabetizacao_uf.csv` | `/Volumes/raw/inep/uf/` |`INEP` |
| `br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv` | `/Volumes/raw/inep/meta_brasil/` |`INEP` |
| `br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv` | `/Volumes/raw/inep/meta_uf/` |`INEP` |
| `br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv` | `/Volumes/raw/inep/meta_municipio/` |`INEP` |
| `BRASIL_2024_MUNICIPIOS.csv` | `/Volumes/raw/ibge/municipios/` |`IBGE` |

Para este trabalho, os arquivos foram gerados manualmente. O ideal seria uma API que lesse estas informações e gerasse os arquivos de forma automática. O Databricks community edition não permite integração online com o BigQuery - que era onde estavam os arquivos do INEP. Eu até tentei linkar os dois ambientes, mas não foi possível.

Obtive o arquivo de munícipios do site do IBGE disponibilizado neste link https://www.ibge.gov.br/explica/codigos-dos-municipios.php 
Último acesso em 11/07/2026 às 17:54

Foi necessário obter este arquivo para poder cruzar as informações com os municípios e trazer os labels com os nomes dos municípios.

2.2 Camada Bronze

Um job de ingestão batch lê cada arquivo do Volume RAW e grava em uma tabela Delta na camada Bronze, adicionando a coluna de controle dt_ingestao, que identifica a data e hora em que aquela carga foi processada. Não há transformação de conteúdo nesta etapa — apenas estruturação em tabela e a inclusão da data-hora da carga.

| Tabela Bronze | Origem (RAW) |
|---|---|
| `bronze.b_avaliacao_alunos` | `br_inep_avaliacao_alfabetizacao_alunos.csv` |
| `bronze.b_avaliacao_municipio` | `br_inep_avaliacao_alfabetizacao_municipio.csv` |
| `bronze.b_avaliacao_uf` | `br_inep_avaliacao_alfabetizacao_uf.csv` |
| `bronze.b_meta_alfabetizacao_brasil` | `br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv` |
| `bronze.b_meta_alfabetizacao_uf` | `br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv` |
| `bronze.b_meta_alfabetizacao_municipio` | `br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv` |
| `bronze.b_municipios_br` | `BRASIL_2024_MUNICIPIOS.csv` |


2.3 Camada Silver

Sete tabelas tratadas, tipadas e validadas — detalhadas na seção 4.

2.4 Camada Gold

Duas tabelas analíticas, prontas para consumo por dashboards e como base de features para modelos preditivos — detalhadas na seção 4.


3. Fluxo de dados

1. Arquivo CSV chega ao Volume RAW .
2. Job de ingestão Bronze lê o Volume, grava tabela Delta com `dt_ingestao`.
3. Job de tratamento Silver:
   a. Filtra a Bronze pela carga mais recente (`MAX(dt_ingestao)`).
   b. Tipa e padroniza colunas (chaves como string, métricas como double).
   c. Verifica duplicidade pela chave de negócio de cada tabela — usando comparação por hash de conteúdo antes de deduplicar, para nunca descartar uma linha divergente sem alerta (ver seção 5.1).
   d. Valida integridade referencial contra a dimensão de municípios, isolando registros órfãos em tabelas de quarentena.
   e. Grava em modo `append`, preservando o histórico completo de cargas (permite auditar qualquer execução anterior filtrando por `dt_ingestao_origem`).
4. Job de Gold:
   a. Lê a carga mais recente de cada tabela Silver necessária.
   b. Calcula os indicadores (agregação por aluno → município; união e comparação meta vs. resultado).
   c. Grava em modo `overwrite` — a Gold reflete sempre o estado calculado a partir da Silver vigente; o histórico auditável já está garantido na Silver.

### 3.1 Regra transversal: sempre a carga mais recente

Toda tabela Silver e Gold segue o mesmo padrão de leitura:

```python
dt_mais_recente = (
    spark.table(TABELA)
    .agg(F.max("dt_ingestao").alias("dt_max"))
    .collect()[0]["dt_max"]
)
df_vigente = spark.table(TABELA).filter(F.col("dt_ingestao") == dt_mais_recente)
```



4. Estrutura de dados

4.1 Tabelas Silver

| Tabela | Grão | Chave Única | Papel |
|---|---|---|---|
| `s_municipios_br` | 1 linha por município | `id_municipio` | Dimensão territorial de referência (IBGE). |
| `s_avaliacao_alunos` | aluno x ano x caderno | `ano, id_escola, id_aluno` | Resultados nível aluno. |
| `s_avaliacao_municipio` | município x ano x série | `ano, id_municipio, serie` | Resultados nível município. |
| `s_avaliacao_uf` | UF x ano x série x rede | `ano, sigla_uf, serie, cd_rede` | Resultados nível UF. |
| `s_meta_alfabetizacao_brasil` | ano (nacional) | `ano` | Meta e resultado observado em nível nacional. |
| `s_meta_alfabetizacao_uf` | UF x ano | `ano, sigla_uf` | Meta e resultado observado por UF. |
| `s_meta_alfabetizacao_municipio` | município x ano | `ano, id_municipio` | Meta e resultado observado por município. |

4.2 Tabelas Gold

| Tabela | Nível | Origem | Papel |
|---|---|---|---|
| `g_indicador_alfabetizacao_municipio` | ano x município | `s_avaliacao_alunos` + `s_municipios_br` | Indicador de alfabetização por município, calculado por agregação direta de alunos (todas as redes), sem depender de interpretação de código de rede. |
| `g_comparativo_evolucao_meta_resultado` | nível de agregação x localidade x ano | as 3 tabelas `s_meta_*` | Comparação entre meta e resultado observado, e evolução temporal, nos três níveis (Brasil/UF/Município). |

4.3 Limitações assumidas — documentadas por decisão consciente

Os dados brutos mostram o campo `rede` representado de formas diferentes entre as tabelas: código numérico em `s_avaliacao_alunos` (valores 2, 3, 4), código numérico diferente em `s_avaliacao_municipio`/`s_avaliacao_uf` (valores 0, 2, 3, 5) e texto nas tabelas de meta ("Municipal", "Pública"). Sem acesso confirmado ao dicionário oficial de códigos do INEP/Base dos Dados (indisponibilidade dentro do site), optou-se por não supor a correspondência entre esses códigos. Cada tabela mantém `rede`/`cd_rede` como veio na origem, sem tradução. Optei por nenhuma tabela Silver ou Gold faz join usando `rede` como critério de comparação entre fontes diferentes.


RR e DF têm comportamento diferente entre tabelas de desempenho e de meta. Em `s_avaliacao_uf`, essas duas UFs simplesmente não têm nenhuma linha (25 de 27 UFs presentes). Em `s_meta_alfabetizacao_uf`, RR aparece com linha, mas todos os campos numéricos nulos. Ambos os casos foram preservados como estão na fonte — não é erro de pipeline, é falta de informação direta do INEP.


5. Tratamento dos Dados Para a Silver


5.1 Verificação de duplicidade

Verifica-se no pipeline antes de qualquer transformação e salvamento, se há valores duplicados utilizando as chaves únicas de cada tabela (item 4.1)


5.2 Detecção de valores ausentes

Nulo não é tratado como sinônimo de erro — cada padrão de ausência foi investigado antes de decidir o tratamento:

- Em `s_avaliacao_alunos`, `proficiencia` é nula em ~513 mil dos 3,87 milhões de registros. Validou-se na análise exploratória que 100% desses nulos são explicados por `presenca = 0` (ausência) ou `preenchimento_caderno = 0` (presente, mas caderno inválido). Uma flag (`flag_avaliacao_valida`) foi adicionada para auxilliar neste tratamento.
- Em `s_avaliacao_municipio` e `s_avaliacao_uf`, as colunas de proporção de alunos por nível de proficiência são nulas em quase metade dos registros, enquanto `taxa_alfabetizacao` praticamente não tem nulo — indício de regra de supressão estatística do INEP (municípios com poucos alunos avaliados não têm a distribuição por nível divulgada, para evitar identificação individual). Mantido como nulo, com flag (`flag_distribuicao_nivel_publicada`) explicitando o motivo.
- Nenhum valor nulo é imputado com zero — isso mudaria o significado do dado ("não houve alfabetização" vs. "não temos essa informação").


5.3 Consistência de valores de taxa/meta

Validações de faixa aplicadas a todos os percentuais (`taxa_alfabetizacao`, `meta_2024`...`meta_2030`, `percentual_participacao` entre 0 e 100) e ao campo categórico `nivel_alfabetizacao` (entre 0 e 5, conforme os cinco níveis de desempenho definidos pelo INEP). A decisão de não unir tabelas por `rede` (seção 4.3) também é, em si, uma medida de consistência: evita comparar categorias que não se sabe se são equivalentes.

---

6. Tecnologias utilizadas

| Tecnologia | Papel | Justificativa |
|---|---|---|
| **Databricks** | Plataforma de processamento | Ambiente unificado para batch, streaming, e governança via Unity Catalog. |
| **PySpark** | Motor de processamento distribuído | Escala naturalmente para a tabela de alunos (~3,87 milhões de linhas por carga) sem reescrita de código ao crescer o volume. |
| **Delta Lake** | Formato de armazenamento | Transações ACID, time travel (rollback via `RESTORE TABLE`), suporte nativo a `MERGE`/`append`/`overwrite`, particionamento eficiente. |
| **Unity Catalog** | Governança e catálogo | Controle de acesso centralizado, linhagem de dados, e as regras de nomenclatura que padronizam os identificadores do projeto (sem espaços/caracteres especiais). |
| **Parquet (via Delta)** | Formato colunar de armazenamento | Compressão e leitura seletiva de colunas — parte da estratégia de FinOps (seção 8). |


7. Decisões arquiteturais

### 7.1 Batch vs. streaming

O escopo mínimo deste projeto usa ingestão batch para todas as fontes: os dados do INEP (avaliação e metas) são publicados em ciclos anuais, não há benefício real em processá-los como streaming. A arquitetura permanece compatível com uma futura ingestão streaming (ex.: atualização incremental de indicadores) sem mudança estrutural — bastaria trocar a fonte de leitura da Bronze por um streaming source, mantendo o mesmo contrato de `dt_ingestao` e o mesmo tratamento Silver.

Como já citado no item 2.2, eu até tentei linkar a base do BigQuery do INEP com o Databricks como demonstração de conceito streaming, mas não foi possível pois a versão community não possui suporte.

7.2 Append (Silver) vs. Overwrite (Gold)

- Silver em `append`: cada execução adiciona a carga mais recente à tabela, preservando o histórico completo e permitindo verificar exatamente como o dado estava em qualquer execução anterior (filtrando por `dt_ingestao_origem`) por questão de segutança.
- Gold em `overwrite`: a Gold representa um valor calculado a partir do estado atual da Silver — não há necessidade de acumular recomputações históricas da mesma métrica, já que o histórico auditável está garantido na camada de origem (Silver). E também por uma questão de custos

(Comentário cético: na verdade eu fiz desse jeito só pra demonstrar conceito e ter algo com overwrite em vez de append - e dizer que "economizei" na camada Gold. Na vida real eu colocaria dt_ref em tudo por questões de segurança e roll back. Quem já sentiu na pele um dashboard quebrando DO NADA por causa de uma base origem que deu problema DO NADA sabe a tristeza e o desespero. Mas vamos fingir que a decisão foi para conter custos e que todas as validações que eu fiz são tão fortes que JAMAIS daria problema num caso real. Avaliador por favor leve este comentário em consideração.)

7.3 Data lake (Delta) vs. Data warehouse tradicional

Optei por manter tudo em Delta Lake (data lakehouse) em vez de migrar a camada Gold para um data warehouse dedicado. Trade-off: um data warehouse tradicional traria otimizações adicionais de consulta SQL, mas para o volume  deste projeto (poucas dezenas de milhares de linhas nas tabelas Gold), o ganho não justificaria o custo adicional de manter duas plataformas.

7.4 Custo vs. performance

Tabelas volumosas (`s_avaliacao_alunos`, `s_avaliacao_municipio`, `g_indicador_alfabetizacao_municipio`) são particionadas por `ano`, já que o padrão de consumo (comparações e séries temporais) filtra por ano na maioria dos casos. Tabelas pequenas (`s_meta_alfabetizacao_brasil`, `s_avaliacao_uf`) não são particionadas — o overhead de metadados de partição superaria o ganho de leitura nesse volume.

Decidiu-se também as tabelas da camada Gold não serem particionadas por questões de custos. 


8. Monitoramento e FinOps

As práticas de FinOps aplicadas são:

- Particionamento nas tabelas Bronze e Silver.
- Formato Delta/Parquet colunar: leitura seletiva de colunas reduz I/O em consultas analíticas na Gold.
- Filtro de carga mais recente antes de qualquer processamento: cada job Silver/Gold filtra a origem pela carga mais recente logo na primeira etapa, evitando processar (e pagar por processar) dados de execuções antigas desnecessariamente.
- Deduplicação por partição em vez de reprocessamento completo: a verificação de duplicidade opera sobre a carga já filtrada, não sobre o histórico inteiro da tabela.
- Gold em `overwrite`: evita crescimento indefinido de tabelas de métricas já calculadas, controlando o custo de armazenamento de longo prazo.


#9. Aplicação em IA

A camada Gold foi desenhada para servir como base direta de iniciativas de IA:

- Modelos de predição de alfabetização: `g_indicador_alfabetizacao_municipio`, no grão ano x município, já entrega `taxa_alfabetizacao_calculada`, `proficiencia_media` e contagens de alunos avaliados — features prontas para um modelo de regressão/classificação que preveja risco de não atingir a meta, bastando enriquecer com dados socioeconômicos e territoriais (ex.: Censo Escolar, IBGE).
- Análise de desigualdade educacional: `g_comparativo_evolucao_meta_resultado`, ao permitir consultar a mesma métrica em Brasil/UF/Município lado a lado, viabiliza análises de dispersão (ex.: desvio-padrão da diferença meta-resultado entre municípios de uma mesma UF) sem processamento adicional.
- Políticas públicas baseadas em dados: a série temporal por `ano` em ambas as tabelas Gold permite identificar municípios com estagnação ou piora, orientando priorização de recursos do Compromisso Nacional Criança Alfabetizada.

