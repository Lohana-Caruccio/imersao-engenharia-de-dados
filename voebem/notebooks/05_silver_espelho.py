# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver — espelho governado do bronze
# MAGIC
# MAGIC A regra da casa, e ela não é negociável:
# MAGIC
# MAGIC > **A silver é o espelho do bronze com governança aplicada.**
# MAGIC > Mesmo nome de tabela, mesmo grão, **mesma contagem de linhas**.
# MAGIC
# MAGIC | | permitido na silver | proibido na silver |
# MAGIC |---|---|---|
# MAGIC | tipagem | ✅ string vira `TIMESTAMP`, `INT`, `DATE` | |
# MAGIC | legibilidade | ✅ quebrar timestamp em data e hora | |
# MAGIC | metadados | ✅ `COMMENT` em toda coluna, tags na tabela | |
# MAGIC | unificação | ✅ dois cadastros do mesmo assunto, com a origem por registro | |
# MAGIC | aritmética pura | ✅ `atraso = real - previsto` | |
# MAGIC | filtro / `WHERE` de negócio | | ❌ |
# MAGIC | `GROUP BY` / agregação | | ❌ |
# MAGIC | limiar, flag, classificação | | ❌ |
# MAGIC
# MAGIC **Por quê?** Porque a silver precisa servir várias análises, e toda linha que ela
# MAGIC descarta é uma pergunta que ninguém mais vai conseguir fazer. Filtro fecha porta.
# MAGIC
# MAGIC O teste para qualquer coluna nova: *isso embute uma decisão de negócio?*
# MAGIC `atraso_partida_min = partida_real - partida_prevista` é subtração — silver.
# MAGIC `partida_pontual = atraso <= 15` embute o número **15**, que é decisão de negócio
# MAGIC e muda por cliente — gold.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. O que precisa ser consertado na tipagem
# MAGIC
# MAGIC Antes de escrever o `CAST`, medir. Duas armadilhas escondidas no bronze:

# COMMAND ----------

# DBTITLE 1,Diagnóstico: identificar strings 'null' vs NULL verdadeiro
# ========================================
# DIAGNÓSTICO: IDENTIFICAR O PROBLEMA DOS NULOS
# ========================================
# Este código investiga como os dados "ausentes" foram representados na fonte.
# PROBLEMA DESCOBERTO: a ausência NÃO veio como NULL (valor nulo do SQL),
# mas sim como a STRING 'null' - literalmente os 4 caracteres: n, u, l, l
#
# IMPACTO: Se não tratarmos isso, um CAST direto para TIMESTAMP falhará,
# e um filtro WHERE coluna IS NULL retornará ZERO linhas quando deveria
# retornar ~29 mil (os voos cancelados sem horário real).
#
# Esta query conta:
# - Quantas linhas têm NULL de verdade (valor ausente no SQL)
# - Quantas linhas têm a string 'null' (texto que precisa ser convertido)
# ========================================

display(spark.sql("""
    SELECT
      COUNT(*)                                                        AS linhas,
      SUM(CASE WHEN partida_real     IS NULL THEN 1 ELSE 0 END)       AS partida_real_null_de_verdade,
      SUM(CASE WHEN partida_real     = 'null' THEN 1 ELSE 0 END)      AS partida_real_string_null,
      SUM(CASE WHEN partida_prevista = 'null' THEN 1 ELSE 0 END)      AS partida_prevista_string_null,
      SUM(CASE WHEN partida_prevista LIKE '%.%' THEN 1 ELSE 0 END)    AS com_fracao_de_segundo
    FROM voebem.bronze.vra
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Armadilha 1 — a ausência veio como a string `'null'`.** Quatro caracteres de texto.
# MAGIC `WHERE partida_real IS NULL` devolve **zero** numa tabela onde 29 mil voos não têm
# MAGIC horário real. Correção: `nullif(coluna, 'null')` **antes** do cast.
# MAGIC
# MAGIC **Armadilha 2 — dois formatos de timestamp no mesmo arquivo.** A maioria vem
# MAGIC `2026-01-27 19:45:00`, mas ~80 mil linhas vêm com fração de segundo de 9 casas.
# MAGIC Um `to_timestamp(col, 'yyyy-MM-dd HH:mm:ss')` fixo devolveria NULL para 8% da base,
# MAGIC em silêncio. O `try_cast(... AS TIMESTAMP)` aceita os dois formatos, e o `try_`
# MAGIC garante que um formato novo vire NULL em vez de derrubar o job.
# MAGIC
# MAGIC Note que isso é **tipagem**, não limpeza de negócio: `'null'` é a forma como a fonte
# MAGIC escreve "ausente". Traduzir isso para `NULL` é dizer a mesma coisa no tipo certo.
# MAGIC Nenhuma linha sai.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `silver.vra` — o espelho
# MAGIC
# MAGIC Repare no que **não** existe nesta query: nenhum `WHERE`, nenhum `GROUP BY`,
# MAGIC nenhum `DISTINCT`, nenhum `JOIN`. É um `SELECT` de projeção sobre o bronze inteiro.
# MAGIC
# MAGIC E repare nas três colunas do fim: `atraso_partida_min`, `atraso_chegada_min` e
# MAGIC `minutos_recuperados`. São subtrações entre colunas da própria linha. Não têm
# MAGIC limiar, não classificam nada, não escondem número mágico — e, principalmente,
# MAGIC não impedem análise nenhuma. Por isso podem morar aqui.

# COMMAND ----------

# DBTITLE 1,Criar schema silver
# MAGIC %sql
# MAGIC -- ========================================
# MAGIC -- CRIAÇÃO DO SCHEMA SILVER
# MAGIC -- ========================================
# MAGIC -- Cria o schema (database) que vai conter todas as tabelas da camada silver.
# MAGIC --
# MAGIC -- IF NOT EXISTS: comando idempotente - pode rodar múltiplas vezes sem erro
# MAGIC --
# MAGIC -- COMMENT: descreve a REGRA da camada silver:
# MAGIC -- - Espelho governado do bronze (mesma contagem de linhas)
# MAGIC -- - COM: tipagem, metadados, aritmética pura
# MAGIC -- - SEM: filtro, agregação, regra de negócio
# MAGIC -- ========================================
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS voebem.silver
# MAGIC COMMENT 'Camada silver: espelho governado do bronze com tipagem, metadados e aritmetica pura. Sem filtro, sem agregacao, sem regra de negocio.'
# MAGIC

# COMMAND ----------

# DBTITLE 1,Criar silver.vra com tratamento de nulos e tipagem
# ========================================
# CRIAÇÃO DA TABELA SILVER.VRA
# ========================================
# Esta é a célula MAIS IMPORTANTE do tratamento de dados deste pipeline.
# Aqui aplicamos 3 técnicas fundamentais de qualidade de dados:
#
# 1. TRATAMENTO DE NULOS COM nullif(coluna, 'null')
#    - Converte a STRING 'null' (4 caracteres) em NULL verdadeiro (valor ausente SQL)
#    - Sem isso, o CAST falharia e teríamos dados inválidos na tabela
#    - Exemplo: nullif(partida_real, 'null') transforma 'null' → NULL
#
# 2. CONVERSÃO SEGURA COM try_cast(... AS TIMESTAMP)
#    - try_cast() NÃO quebra o job se encontrar formato inesperado
#    - Ele simplesmente retorna NULL para valores que não consegue converter
#    - Importante porque o arquivo tem 2 formatos de timestamp misturados:
#      * Maioria: '2026-01-27 19:45:00' (sem fração de segundo)
#      * ~80 mil linhas: '2026-01-27 19:45:00.123456789' (com 9 casas decimais)
#    - try_cast() aceita AMBOS os formatos automaticamente
#
# 3. TRATAMENTO DE CÓDIGOS ESPECIAIS
#    - nullif(codigo_justificativa, 'N/A') transforma o código 'N/A' em NULL
#    - 'N/A' é outra forma que a fonte usa para dizer "não aplicável"
#
# REGRA DA SILVER: Espelho do bronze COM TIPAGEM, SEM FILTRO
# - Mesma contagem de linhas: 1.014.705 (bronze) = 1.014.705 (silver)
# - Nenhuma linha é descartada
# - Apenas traduzimos os tipos de dados para o formato correto
# ========================================

spark.sql("""
CREATE OR REPLACE TABLE voebem.silver.vra AS
-- CTE 'tipado': primeira etapa - converter strings em tipos corretos
WITH tipado AS (
  SELECT
    -- Colunas que já estão OK como string (códigos e identificadores)
    icao_empresa,
    numero_voo,
    codigo_di,
    codigo_tipo_linha,
    icao_aerodromo_origem,
    icao_aerodromo_destino,
    
    -- *** TRATAMENTO DE NULOS + CONVERSÃO DE TIMESTAMP ***
    -- Padrão: nullif(coluna, 'null') remove a string 'null', depois try_cast converte para TIMESTAMP
    try_cast(nullif(partida_prevista, 'null') AS TIMESTAMP) AS partida_prevista,
    try_cast(nullif(partida_real,     'null') AS TIMESTAMP) AS partida_real,
    try_cast(nullif(chegada_prevista, 'null') AS TIMESTAMP) AS chegada_prevista,
    try_cast(nullif(chegada_real,     'null') AS TIMESTAMP) AS chegada_real,
    
    situacao_voo,
    -- Tratamento de código especial: 'N/A' vira NULL
    nullif(codigo_justificativa, 'N/A')                     AS codigo_justificativa,
    -- Colunas de auditoria (metadata de rastreamento)
    _arquivo_origem,
    _ingerido_em
  FROM voebem.bronze.vra
)
-- SELECT final: adiciona colunas derivadas para legibilidade e cálculos
SELECT
  -- Identificadores do voo (mantidos como string)
  icao_empresa,
  numero_voo,
  codigo_di,
  codigo_tipo_linha,
  icao_aerodromo_origem,
  icao_aerodromo_destino,

  -- *** PARTIDA PREVISTA: timestamp completo + desmembramento ***
  partida_prevista,                                    -- Timestamp completo
  CAST(partida_prevista AS DATE)                     AS partida_prevista_data,  -- Só a data
  date_format(partida_prevista, 'HH:mm')             AS partida_prevista_hora,  -- Só hora:minuto

  -- *** PARTIDA REAL: timestamp completo + desmembramento ***
  partida_real,                                        -- Timestamp completo (NULL se voo cancelado)
  CAST(partida_real AS DATE)                         AS partida_real_data,      -- Só a data
  date_format(partida_real, 'HH:mm')                 AS partida_real_hora,      -- Só hora:minuto

  -- *** CHEGADA PREVISTA: timestamp completo + desmembramento ***
  chegada_prevista,                                    -- Timestamp completo
  CAST(chegada_prevista AS DATE)                     AS chegada_prevista_data,  -- Só a data
  date_format(chegada_prevista, 'HH:mm')             AS chegada_prevista_hora,  -- Só hora:minuto

  -- *** CHEGADA REAL: timestamp completo + desmembramento ***
  chegada_real,                                        -- Timestamp completo (NULL se voo cancelado)
  CAST(chegada_real AS DATE)                         AS chegada_real_data,      -- Só a data
  date_format(chegada_real, 'HH:mm')                 AS chegada_real_hora,      -- Só hora:minuto

  situacao_voo,
  codigo_justificativa,

  -- aritmetica pura: subtracao de colunas da propria linha, sem limiar e sem decisao
  -- *** MÉTRICAS DE ATRASO: ARITMÉTICA PURA (SEM REGRA DE NEGÓCIO) ***
  -- Essas colunas são permitidas na silver porque são apenas SUBTRAÇÕES.
  -- Não há limiar ("15 minutos é pontual"), não há classificação, não há decisão.
  -- São apenas números que facilitam análises posteriores na camada gold.
  
  -- Atraso na partida = minutos entre hora programada e hora real de saída
  -- Positivo = atrasou, Negativo = antecipou, NULL = voo cancelado
  CAST(timestampdiff(MINUTE, partida_prevista, partida_real) AS INT) AS atraso_partida_min,
  
  -- Atraso na chegada = minutos entre hora programada e hora real de pouso
  -- Positivo = atrasou, Negativo = antecipou, NULL = voo cancelado
  CAST(timestampdiff(MINUTE, chegada_prevista, chegada_real) AS INT) AS atraso_chegada_min,
  
  -- Minutos recuperados em voo = quanto o avião "ganhou" no ar
  -- Exemplo: saiu 20 min atrasado, chegou 10 min atrasado → recuperou 10 minutos
  -- Positivo = recuperou tempo, Negativo = perdeu mais tempo ainda
  CAST(timestampdiff(MINUTE, partida_prevista, partida_real)
     - timestampdiff(MINUTE, chegada_prevista, chegada_real) AS INT) AS minutos_recuperados,

  -- Colunas de auditoria
  _arquivo_origem,      -- De qual CSV esta linha veio
  _ingerido_em,         -- Quando entrou no bronze
  current_timestamp()   AS _transformado_em  -- Quando esta silver foi criada
FROM tipado
""")

print("silver.vra criada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. A prova que importa: mesma contagem
# MAGIC
# MAGIC Este é o critério objetivo do marco. Se a diferença não for **zero**, a silver
# MAGIC não é espelho — é recorte, e alguém em algum momento vai fazer uma pergunta que
# MAGIC ela não consegue mais responder.

# COMMAND ----------

# DBTITLE 1,Validação: contagem bronze vs silver
# ========================================
# VALIDAÇÃO CRÍTICA: MESMA CONTAGEM DE LINHAS
# ========================================
# Esta é a PROVA que a silver é um espelho fiel do bronze.
# Se a diferença NÃO for ZERO, algo foi filtrado indevidamente.
#
# REGRA: Silver = Bronze em quantidade de linhas
# - Bronze: 1.014.705 linhas
# - Silver: deve ter 1.014.705 linhas
# - Diferença: DEVE SER 0
#
# Por que isso é importante?
# - Se descartarmos linhas aqui, perdemos histórico
# - Voos cancelados importam tanto quanto voos realizados
# - Diferentes análises precisam de diferentes escopos
# - Filtro fecha porta: o que sair daqui não volta mais
# ========================================

display(spark.sql("""
    SELECT
      (SELECT COUNT(*) FROM voebem.bronze.vra) AS bronze_vra,      -- Contagem origem
      (SELECT COUNT(*) FROM voebem.silver.vra) AS silver_vra,       -- Contagem destino
      (SELECT COUNT(*) FROM voebem.bronze.vra)
        - (SELECT COUNT(*) FROM voebem.silver.vra) AS diferenca     -- DEVE SER 0
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC E a tipagem funcionou? Contagem de conversões bem-sucedidas por coluna:

# COMMAND ----------

# DBTITLE 1,Validação: contagem de conversões bem-sucedidas
# ========================================
# VALIDAÇÃO: CONVERSÕES BEM-SUCEDIDAS
# ========================================
# Esta query conta quantas linhas têm valores NÃO-NULOS em cada coluna tipada.
# 
# O que esperamos ver:
# - partida_prevista_ok: ~1.014.705 (100% - todo voo tem partida prevista)
# - partida_real_ok: ~985.000 (97% - voos cancelados não têm partida real)
# - chegada_prevista_ok: ~1.014.705 (100%)
# - chegada_real_ok: ~985.000 (97% - mesma lógica)
# - atraso_partida_ok: ~985.000 (só calculável quando ambos timestamps existem)
# - minutos_recuperados_ok: ~985.000 (precisa dos 4 timestamps)
#
# Se algum número estiver MUITO diferente, pode indicar:
# - Formato de timestamp inesperado que o try_cast() não conseguiu converter
# - Problema na lógica do nullif()
# ========================================

display(spark.sql("""
    SELECT
      COUNT(partida_prevista)     AS partida_prevista_ok,
      COUNT(partida_real)         AS partida_real_ok,
      COUNT(chegada_prevista)     AS chegada_prevista_ok,
      COUNT(chegada_real)         AS chegada_real_ok,
      COUNT(atraso_partida_min)   AS atraso_partida_ok,
      COUNT(minutos_recuperados)  AS minutos_recuperados_ok
    FROM voebem.silver.vra
"""))

# COMMAND ----------

# DBTITLE 1,Amostra: primeiras 5 linhas da silver.vra
# ========================================
# AMOSTRA: VISUALIZAÇÃO DOS DADOS PROCESSADOS
# ========================================
# Mostra as primeiras 5 linhas da silver.vra ordenadas por partida_prevista.
# 
# O que observar:
# - partida_prevista agora é TIMESTAMP (não mais string)
# - partida_prevista_data e _hora estão separadas para facilitar análises
# - atraso_partida_min, atraso_chegada_min e minutos_recuperados são números calculados
# - situacao_voo mostra REALIZADO ou CANCELADO
# - NULL nos campos de atraso indica voo cancelado (sem horário real)
# ========================================

display(spark.sql("""
    SELECT icao_empresa, numero_voo, icao_aerodromo_origem, icao_aerodromo_destino,
           partida_prevista, partida_prevista_data, partida_prevista_hora,
           atraso_partida_min, atraso_chegada_min, minutos_recuperados, situacao_voo
    FROM voebem.silver.vra
    ORDER BY partida_prevista
    LIMIT 5
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `silver.empresas` — o caso clássico dos dois sistemas
# MAGIC
# MAGIC Aqui a silver faz a única coisa que muda a forma da tabela: **unifica dois cadastros
# MAGIC do mesmo assunto**. `bronze.empresas_nacionais` e `bronze.empresas_estrangeiras` são
# MAGIC dois processos administrativos da ANAC descrevendo a mesma entidade de negócio —
# MAGIC "empresa aérea que opera no Brasil".
# MAGIC
# MAGIC Isso é permitido porque **não perde informação**: a contagem da silver é a soma exata
# MAGIC das duas, e `origem_cadastro` guarda por registro de onde ele veio. Quem quiser
# MAGIC voltar a olhar só as estrangeiras, consegue. Nada fecha.
# MAGIC
# MAGIC O que seria proibido: um `WHERE situacao = 'ATIVA'` aqui. Empresa que encerrou
# MAGIC operação continua tendo voado no período — filtrar apagaria o histórico dela.

# COMMAND ----------

# DBTITLE 1,Criar silver.empresas unificando dois cadastros
# ========================================
# CRIAÇÃO DA SILVER.EMPRESAS: UNIFICAÇÃO DE CADASTROS
# ========================================
# Esta célula demonstra o Único tipo de transformação estrutural permitido na silver:
# UNIR duas tabelas que descrevem o MESMO assunto de negócio.
#
# POR QUE ISSO É PERMITIDO?
# - A ANAC publica empresas aéreas em DOIS cadastros separados (nacional vs estrangeira)
# - São dois processos administrativos diferentes, mas descrevem a MESMA entidade: "empresa aérea"
# - Manter separadas na silver não agrega valor - apenas complica consultas
#
# COMO PRESERVAMOS RASTREABILIDADE?
# - Coluna 'origem_cadastro' marca de qual fonte cada linha veio
# - Quem quiser filtrar só estrangeiras, consegue: WHERE origem_cadastro = 'estrangeira'
# - NENHUMA INFORMAÇÃO É PERDIDA
#
# REGRA DE CONTAGEM:
# - bronze.empresas_nacionais: 729 linhas
# - bronze.empresas_estrangeiras: 148 linhas
# - silver.empresas: DEVE TER 877 linhas (729 + 148)
# ========================================

spark.sql("""
CREATE OR REPLACE TABLE voebem.silver.empresas AS
-- Primeiro SELECT: todas as empresas NACIONAIS
SELECT
  icao,
  sigla_iata,
  razao_social,
  servico,
  cidade,
  uf,
  situacao,
  'nacional'      AS origem_cadastro,    -- Marca a origem deste registro
  _arquivo_origem,
  _ingerido_em,
  current_timestamp() AS _transformado_em
FROM voebem.bronze.empresas_nacionais

UNION ALL  -- UNION ALL mantém todas as linhas (não remove duplicatas)

-- Segundo SELECT: todas as empresas ESTRANGEIRAS
SELECT
  icao,
  sigla_iata,
  razao_social,
  servico,
  cidade,
  uf,
  situacao,
  'estrangeira'   AS origem_cadastro,    -- Marca a origem deste registro
  _arquivo_origem,
  _ingerido_em,
  current_timestamp() AS _transformado_em
FROM voebem.bronze.empresas_estrangeiras
""")

# Validação: a soma das duas fontes deve ser EXATAMENTE igual à contagem da silver
# Se silver_empresas != soma_esperada, algo foi perdido ou duplicado indevidamente
display(spark.sql("""
    SELECT
      (SELECT COUNT(*) FROM voebem.bronze.empresas_nacionais)    AS bronze_nacionais,
      (SELECT COUNT(*) FROM voebem.bronze.empresas_estrangeiras) AS bronze_estrangeiras,
      (SELECT COUNT(*) FROM voebem.bronze.empresas_nacionais)
        + (SELECT COUNT(*) FROM voebem.bronze.empresas_estrangeiras) AS soma_esperada,
      (SELECT COUNT(*) FROM voebem.silver.empresas)              AS silver_empresas
"""))

# COMMAND ----------

# DBTITLE 1,Análise: distribuição por origem de cadastro
# ========================================
# ANÁLISE: DISTRIBUIÇÃO POR ORIGEM
# ========================================
# Esta query AGRUPA para CONFERÊNCIA - não é materializada na tabela.
# A proibição de GROUP BY na silver se refere ao que é GRAVADO,
# não a queries de validação posteriores.
#
# O que essa query revela:
# - Quantas linhas vieram de cada origem (nacional vs estrangeira)
# - Quantas empresas de cada tipo têm código ICAO preenchido
#
# ACHADO IMPORTANTE:
# - Empresas nacionais: 729 linhas, mas apenas ~20 têm ICAO
#   (porque o cadastro inclui aviação agrícola, táxi aéreo, aeroclube)
# - Empresas estrangeiras: 148 linhas, quase todas têm ICAO
#   (porque são companhias de linha regular internacional)
# ========================================

display(spark.sql("""
    SELECT origem_cadastro,
           COUNT(*) AS linhas,
           COUNT(CASE WHEN icao IS NOT NULL AND icao <> '' THEN 1 END) AS com_icao
    FROM voebem.silver.empresas
    GROUP BY origem_cadastro
    ORDER BY origem_cadastro
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC > Este `GROUP BY` é **conferência**, não construção. A tabela já está escrita; o
# MAGIC > agrupamento aqui só serve para eu olhar o resultado. A proibição vale para o que
# MAGIC > é **materializado** na silver.
# MAGIC
# MAGIC Só 20 das 729 empresas nacionais têm código ICAO — o cadastro é dominado por aviação
# MAGIC agrícola, táxi aéreo e aeroclube, que não têm código de três letras. Quem voa linha
# MAGIC regular tem. Isso volta no marco-07, quando o join com o VRA for medido.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `silver.aerodromos` e `silver.codigos_operacao` — espelhos
# MAGIC
# MAGIC Uma tabela de referência para cada uma do bronze, tipada e documentada. Duas coisas
# MAGIC valem comentário:
# MAGIC
# MAGIC - `altitude` vem como `"193,0"` — vírgula decimal. Vira `DOUBLE` com um `replace`.
# MAGIC - a coluna que o cabeçalho chama de `UF` contém `"Acre"`, `"São Paulo"`: é o **nome
# MAGIC   da unidade federativa por extenso**, não a sigla. Quem escrever `WHERE uf = 'SP'`
# MAGIC   recebe zero linhas e vai achar que o dado sumiu. O nome da coluna passa a dizer a
# MAGIC   verdade (`uf_nome`) e o `COMMENT` avisa. Renomear e documentar é governança;
# MAGIC   inventar a sigla seria transformação de negócio.

# COMMAND ----------

# DBTITLE 1,Criar silver.aerodromos e silver.codigos_operacao
# ========================================
# CRIAÇÃO DA SILVER.AERODROMOS
# ========================================
# Espelho governado da tabela bronze.aerodromos com:
#
# 1. RENOMEAÇÃO PARA CLAREZA
#    - 'uf' vira 'uf_nome' porque contém "São Paulo", não "SP"
#    - Evita confusão: alguém que escrever WHERE uf = 'SP' receberá ZERO linhas
#    - O nome da coluna agora diz a verdade sobre o conteúdo
#
# 2. TIPAGEM DE ALTITUDE
#    - No bronze vem como string "193,0" (vírgula decimal)
#    - replace(altitude, ',', '.') troca vírgula por ponto
#    - try_cast(...AS DOUBLE) converte para número decimal
#    - Agora pode ser usado em cálculos e comparações numéricas
#
# 3. PRESERVAÇÃO DE COORDENADAS ORIGINAIS
#    - latitude e longitude ficam como vieram: graus, minutos, segundos (DMS)
#    - Não convertemos para decimal porque isso seria TRANSFORMAÇÃO DE NEGÓCIO
#    - A conversão (se necessária) fica para a gold, conforme caso de uso
# ========================================

spark.sql("""
CREATE OR REPLACE TABLE voebem.silver.aerodromos AS
SELECT
  icao,                                             -- Código ICAO (chave)
  ciad,                                             -- Código ANAC
  nome,
  municipio,
  uf                                            AS uf_nome,              -- RENOMEADO: contém nome por extenso
  municipio_servido,
  uf_servido                                    AS uf_servido_nome,      -- RENOMEADO: idem
  latitude                                      AS latitude_dms,          -- Mantido como string DMS
  longitude                                     AS longitude_dms,         -- Mantido como string DMS
  try_cast(replace(altitude, ',', '.') AS DOUBLE) AS altitude_m,        -- TIPADO: vírgula → ponto → DOUBLE
  situacao,
  _ingerido_em,
  current_timestamp()                           AS _transformado_em
FROM voebem.bronze.aerodromos
""")

# ========================================
# CRIAÇÃO DA SILVER.CODIGOS_OPERACAO
# ========================================
# Espelho da seed table de códigos.
# 
# O QUE É ESTA TABELA?
# - Seed table: dado de referência pequeno, estável e curado manualmente
# - Traduz os códigos cripícos do VRA em descrições legíveis
#   Exemplo: codigo_di="0" → "Etapa Regular"
#            codigo_tipo_linha="N" → "Doméstica Mista"
#
# POR QUE NÃO ESTÁ EMBUTIDO NO CÓDIGO?
# - Poderia ser um CASE WHEN gigante dentro das queries
# - MAS: isso espalharia o mapeamento por todo o código
# - Centralizar em tabela permite atualização em um só lugar
# - Permite JOIN para enriquecer os dados na camada gold
# ========================================

spark.sql("""
CREATE OR REPLACE TABLE voebem.silver.codigos_operacao AS
SELECT
  dominio,              -- A qual coluna do VRA pertence (codigo_di ou codigo_tipo_linha)
  codigo,               -- O código como aparece no VRA ("0", "N", "I", etc.)
  descricao,            -- Descrição oficial da ANAC
  current_timestamp() AS _transformado_em
FROM voebem.bronze.codigos_operacao
""")

# Validação: aerodromos e codigos_operacao devem ter contagem idêntica bronze vs silver
# São tabelas de referência - nenhuma linha é filtrada ou agregada
display(spark.sql("""
    SELECT 'aerodromos' AS tabela,
           (SELECT COUNT(*) FROM voebem.bronze.aerodromos) AS bronze,
           (SELECT COUNT(*) FROM voebem.silver.aerodromos) AS silver
    UNION ALL
    SELECT 'codigos_operacao',
           (SELECT COUNT(*) FROM voebem.bronze.codigos_operacao),
           (SELECT COUNT(*) FROM voebem.silver.codigos_operacao)
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Metadados gerenciados
# MAGIC
# MAGIC Documentação não é enfeite: o consumidor final deste pipeline é um **LLM**, e o
# MAGIC `COMMENT` é literalmente o que ele lê para decidir qual coluna usar. Coluna sem
# MAGIC comentário é coluna que a IA vai usar errado.
# MAGIC
# MAGIC O comentário descreve **significado de negócio**, não tipo de dado. "TIMESTAMP da
# MAGIC partida" não ajuda ninguém; "horário em que a aeronave efetivamente saiu do solo"
# MAGIC ajuda.

# COMMAND ----------

# DBTITLE 1,Aplicar comentários nas colunas da VRA
# ========================================
# DOCUMENTAÇÃO: COMENTÁRIOS NAS COLUNAS DA VRA
# ========================================
# Esta célula aplica comentários de negócio em TODAS as 26 colunas da silver.vra.
#
# POR QUE ISSO É CRÍTICO?
# - O consumidor final deste pipeline é um LLM (modelo de linguagem)
# - O LLM lê os comentários para decidir qual coluna usar em cada query
# - Sem comentários, o LLM "adivinha" e usa a coluna errada
#
# O QUE FAZ UM BOM COMENTÁRIO?
# - Descreve SIGNIFICADO DE NEGÓCIO, não tipo de dado
# - Ruim: "TIMESTAMP da partida"
# - Bom: "Horário em que a aeronave efetivamente saiu do solo"
# - Menciona particularidades: "Nulo em voo cancelado"
# - Aponta relacionamentos: "Chave para silver.empresas"
# ========================================

COMENTARIOS_VRA = {
    "icao_empresa":            "Codigo ICAO de tres letras da empresa aerea que operou a etapa. Chave para silver.empresas.",
    "numero_voo":              "Numero do voo divulgado pela companhia. Identificador comercial, nao numerico: pode ter zero a esquerda e se repete entre datas.",
    "codigo_di":               "Codigo de autorizacao (DI) da etapa: distingue etapa regular, extra, de retorno, charter. Descricao em silver.codigos_operacao (dominio codigo_di).",
    "codigo_tipo_linha":       "Codigo do tipo de linha: N e C domesticas, I e G internacionais. Descricao em silver.codigos_operacao (dominio codigo_tipo_linha).",
    "icao_aerodromo_origem":   "Codigo ICAO do aerodromo de onde a etapa partiu. Chave para silver.aerodromos - aeroportos estrangeiros nao constam no cadastro da ANAC.",
    "icao_aerodromo_destino":  "Codigo ICAO do aerodromo onde a etapa pousou. Mesma observacao de cobertura da origem.",
    "partida_prevista":        "Horario de partida programado pela companhia, na hora local do aeroporto de origem.",
    "partida_prevista_data":   "Data da partida programada, separada para facilitar analise por dia.",
    "partida_prevista_hora":   "Hora e minuto da partida programada (HH:mm), separada para analise por faixa horaria.",
    "partida_real":            "Horario em que a aeronave efetivamente saiu. Nulo em voo cancelado, que nao chegou a partir.",
    "partida_real_data":       "Data da partida efetiva.",
    "partida_real_hora":       "Hora e minuto da partida efetiva (HH:mm).",
    "chegada_prevista":        "Horario de chegada programado, na hora local do aeroporto de destino.",
    "chegada_prevista_data":   "Data da chegada programada.",
    "chegada_prevista_hora":   "Hora e minuto da chegada programada (HH:mm).",
    "chegada_real":            "Horario em que a aeronave efetivamente pousou. Nulo em voo cancelado.",
    "chegada_real_data":       "Data da chegada efetiva.",
    "chegada_real_hora":       "Hora e minuto da chegada efetiva (HH:mm).",
    "situacao_voo":            "Situacao informada pela companhia: REALIZADO quando a etapa aconteceu, CANCELADO quando nao.",
    "codigo_justificativa":    "Motivo declarado do atraso. Deixou de ser exigido pela ANAC em abril de 2020 com a revogacao da IAC 1504: vem vazio em toda a janela deste projeto.",
    "atraso_partida_min":      "Minutos entre a partida programada e a partida efetiva. Positivo e atraso, negativo e antecipacao. Aritmetica pura: nao aplica limiar de pontualidade.",
    "atraso_chegada_min":      "Minutos entre a chegada programada e a chegada efetiva. Positivo e atraso, negativo e antecipacao.",
    "minutos_recuperados":     "Minutos que a etapa recuperou em voo: atraso de partida menos atraso de chegada. Positivo significa que chegou menos atrasada do que saiu.",
    "_arquivo_origem":         "Auditoria: nome do arquivo CSV mensal da ANAC de onde a linha veio.",
    "_ingerido_em":            "Auditoria: momento em que a linha entrou no bronze.",
    "_transformado_em":        "Auditoria: momento em que a silver foi reconstruida a partir do bronze.",
}

# Loop que aplica cada comentário na respectiva coluna via ALTER TABLE
# ALTER COLUMN ... COMMENT adiciona metadata na tabela Unity Catalog
# Essa metadata fica visível em DESCRIBE, na UI do Catalog Explorer e para LLMs
for coluna, comentario in COMENTARIOS_VRA.items():
    spark.sql(f"ALTER TABLE voebem.silver.vra ALTER COLUMN {coluna} COMMENT '{comentario}'")

print(f"{len(COMENTARIOS_VRA)} colunas comentadas em silver.vra")

# COMMAND ----------

# DBTITLE 1,Aplicar comentários nas outras tabelas silver
# ========================================
# DOCUMENTAÇÃO: COMENTÁRIOS NAS DEMAIS TABELAS SILVER
# ========================================
# Esta célula documenta as 3 tabelas de referência:
# - silver.empresas (11 colunas)
# - silver.aerodromos (13 colunas)
# - silver.codigos_operacao (4 colunas)
#
# Mesma filosofia: descrever significado de negócio, não tipo de dado.
# ========================================

# Dicionário de comentários para a tabela EMPRESAS
COMENTARIOS_EMPRESAS = {
    "icao":            "Codigo ICAO de tres letras da empresa. Vazio para operadores sem codigo (aviacao agricola, taxi aereo, aeroclube).",
    "sigla_iata":      "Sigla de duas letras da empresa no padrao IATA, como publicada pela ANAC.",
    "razao_social":    "Razao social da empresa aerea. E o nome que aparece para quem consome o produto final.",
    "servico":         "Tipo de servico autorizado pela ANAC: transporte regular, nao regular, aeroagricola, taxi aereo.",
    "cidade":          "Municipio da sede ou do representante legal no Brasil.",
    "uf":              "Sigla da unidade federativa da sede.",
    "situacao":        "Situacao do registro na ANAC: ATIVA ou nao. Registro inativo permanece na tabela porque a empresa pode ter voado no periodo analisado.",
    "origem_cadastro": "De qual dos dois cadastros da ANAC este registro veio: nacional ou estrangeira. E a coluna que preserva a fronteira entre as duas fontes depois da uniao.",
    "_arquivo_origem": "Auditoria: arquivo CSV de origem.",
    "_ingerido_em":    "Auditoria: momento da ingestao no bronze.",
    "_transformado_em":"Auditoria: momento da construcao da silver.",
}

# Dicionário de comentários para a tabela AERODROMOS
COMENTARIOS_AERODROMOS = {
    "icao":              "Codigo ICAO (OACI) do aerodromo. Chave de ligacao com origem e destino do VRA.",
    "ciad":              "Codigo de identificacao do aerodromo no cadastro da ANAC.",
    "nome":              "Nome do aerodromo como publicado pela ANAC.",
    "municipio":         "Municipio onde o aerodromo esta fisicamente localizado.",
    "uf_nome":           "Nome da unidade federativa POR EXTENSO (Acre, Sao Paulo), nao a sigla: e assim que a ANAC publica.",
    "municipio_servido": "Municipio principal atendido pelo aerodromo, que pode ser diferente do municipio onde ele fica.",
    "uf_servido_nome":   "Nome por extenso da UF do municipio servido.",
    "latitude_dms":      "Latitude em graus, minutos e segundos, como publicada pela ANAC.",
    "longitude_dms":     "Longitude em graus, minutos e segundos, como publicada pela ANAC.",
    "altitude_m":        "Altitude do aerodromo em metros. Na origem vem com virgula decimal.",
    "situacao":          "Situacao do aerodromo no cadastro da ANAC.",
    "_ingerido_em":      "Auditoria: momento da ingestao no bronze.",
    "_transformado_em":  "Auditoria: momento da construcao da silver.",
}

# Dicionário de comentários para a tabela CODIGOS_OPERACAO
COMENTARIOS_CODIGOS = {
    "dominio":          "A qual coluna do VRA este codigo pertence: codigo_di ou codigo_tipo_linha.",
    "codigo":           "O codigo como aparece no VRA.",
    "descricao":        "Descricao oficial do codigo, curada da pagina de descricao de variaveis da ANAC.",
    "_transformado_em": "Auditoria: momento da construcao da silver.",
}

# Loop que aplica os comentários nas 3 tabelas de referência
# Itera sobre cada tabela e seu respectivo dicionário de comentários
for tabela, mapa in [
    ("voebem.silver.empresas",         COMENTARIOS_EMPRESAS),
    ("voebem.silver.aerodromos",       COMENTARIOS_AERODROMOS),
    ("voebem.silver.codigos_operacao", COMENTARIOS_CODIGOS),
]:
    # Para cada coluna do dicionário, aplica o comentário via ALTER TABLE
    for coluna, comentario in mapa.items():
        spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN {coluna} COMMENT '{comentario}'")
    print(f"{len(mapa)} colunas comentadas em {tabela}")

# COMMAND ----------

# MAGIC %md
# MAGIC Comentário de tabela e **tags**. Tag é metadado de busca e de política: é como alguém
# MAGIC que nunca viu este projeto encontra "todas as tabelas da camada silver" ou "tudo que
# MAGIC é do domínio aviação" sem precisar perguntar para a gente.

# COMMAND ----------

# DBTITLE 1,Aplicar comentários e tags nas tabelas
# ========================================
# GOVERNANÇA: COMENTÁRIOS E TAGS DE TABELA
# ========================================
# Esta célula adiciona metadata de TABELA (não coluna):
#
# 1. COMMENT ON TABLE: descrição geral da tabela
#    - Qual é o propósito da tabela
#    - Qual é o grão (o que cada linha representa)
#    - Regras especiais (ex: mesma contagem que bronze)
#
# 2. TAGS: metadados estruturados para busca e política
#    - 'camada': bronze, silver ou gold
#    - 'dominio': assunto de negócio (aviação, vendas, etc.)
#    - 'fonte': de onde veio o dado original
#    - 'grao': o que cada linha representa
#
# POR QUE TAGS SÃO IMPORTANTES?
# - Permitem buscar "todas as tabelas da camada silver"
# - Permitem aplicar políticas de acesso por domínio
# - Facilitam descoberta de dados sem conhecer nomes de tabelas
# ========================================

TABELAS = {
    "voebem.silver.vra": (
        "Silver - espelho governado de bronze.vra. Mesmo grao (uma linha por etapa de voo) e "
        "MESMA contagem de linhas do bronze: sem filtro, sem agregacao e sem regra de negocio. "
        "Traz tipagem, data e hora separadas e as tres metricas de aritmetica pura de atraso. "
        "Pontualidade, escopo e exclusoes ficam na gold.",
        {"camada": "silver", "dominio": "aviacao", "fonte": "ANAC-VRA", "grao": "etapa_de_voo"},
    ),
    "voebem.silver.empresas": (
        "Silver - cadastro unificado de empresas aereas: uniao dos dois cadastros do bronze "
        "(nacionais e estrangeiras) com a coluna origem_cadastro preservando a fonte de cada registro. "
        "Contagem igual a soma exata das duas tabelas de origem.",
        {"camada": "silver", "dominio": "aviacao", "fonte": "ANAC-Operador-Aereo", "grao": "empresa"},
    ),
    "voebem.silver.aerodromos": (
        "Silver - espelho governado do cadastro de aerodromos publicos da ANAC. Cobre apenas "
        "aerodromos brasileiros: aeroportos estrangeiros do VRA nao constam aqui, e isso e "
        "propriedade da fonte, nao defeito.",
        {"camada": "silver", "dominio": "aviacao", "fonte": "ANAC-Aerodromos", "grao": "aerodromo"},
    ),
    "voebem.silver.codigos_operacao": (
        "Silver - espelho da seed table de codigos de operacao (DI e tipo de linha) com as "
        "descricoes oficiais da ANAC.",
        {"camada": "silver", "dominio": "aviacao", "fonte": "ANAC-seed", "grao": "codigo"},
    ),
}

# Loop que aplica comentários e tags em cada tabela silver
for tabela, (comentario, tags) in TABELAS.items():
    # Adiciona o comentário descritivo da tabela
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{comentario}'")
    
    # Formata as tags no padrão 'chave' = 'valor', 'chave2' = 'valor2'
    pares = ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())
    
    # Aplica todas as tags de uma vez via ALTER TABLE SET TAGS
    spark.sql(f"ALTER TABLE {tabela} SET TAGS ({pares})")
    
    print(f"{tabela}: comentario + {len(tags)} tags")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Auditoria da governança: 100% das colunas comentadas?
# MAGIC
# MAGIC "Documentei tudo" é afirmação, não fato. O `information_schema` responde de verdade:

# COMMAND ----------

# DBTITLE 1,Auditoria: percentual de colunas documentadas
# ========================================
# AUDITORIA: VERIFICAÇÃO DE DOCUMENTAÇÃO COMPLETA
# ========================================
# Esta query consulta o information_schema (catálogo de metadata do Unity Catalog)
# para verificar se TODAS as colunas de TODAS as tabelas silver têm comentários.
#
# O QUE PROCURAMOS:
# - pct_documentado = 100.0 para TODAS as tabelas
# - sem_comentario = 0 para TODAS as tabelas
#
# Se alguma tabela tiver < 100%, significa que:
# - Esquecemos de documentar alguma coluna, OU
# - Adicionamos uma coluna nova e não atualizamos o dicionário de comentários
#
# Esta query é a PROVA objetiva de que a governança foi aplicada.
# "Documentei tudo" é afirmação; esta query é FATO.
# ========================================

display(spark.sql("""
    SELECT table_name,
           COUNT(*)                                                          AS colunas,
           SUM(CASE WHEN comment IS NULL OR comment = '' THEN 1 ELSE 0 END)  AS sem_comentario,
           ROUND(100.0 * SUM(CASE WHEN comment IS NOT NULL AND comment <> '' THEN 1 ELSE 0 END)
                 / COUNT(*), 1)                                              AS pct_documentado
    FROM voebem.information_schema.columns
    WHERE table_schema = 'silver'
    GROUP BY table_name
    ORDER BY table_name
"""))

# COMMAND ----------

# DBTITLE 1,Auditoria: verificação de tags aplicadas
# ========================================
# AUDITORIA: VERIFICAÇÃO DE TAGS APLICADAS
# ========================================
# Esta query consulta information_schema.table_tags para listar
# todas as tags que foram aplicadas nas tabelas silver.
#
# O QUE ESPERAMOS VER:
# - Cada tabela deve ter 4 tags: camada, dominio, fonte, grao
# - Todas devem ter camada='silver'
# - Todas devem ter dominio='aviacao'
#
# Tags permitem:
# - Busca: "mostre todas as tabelas silver" ou "tudo de aviação"
# - Políticas: aplicar regras de acesso por domínio
# - Descoberta: encontrar dados sem saber o nome da tabela
# ========================================

display(spark.sql("""
    SELECT table_name, tag_name, tag_value
    FROM voebem.information_schema.table_tags
    WHERE schema_name = 'silver'
    ORDER BY table_name, tag_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Fechamento do marco
# MAGIC
# MAGIC A silver tem quatro tabelas, todas espelho do bronze, todas documentadas, e a `vra`
# MAGIC com exatamente a mesma contagem de linhas da origem.
# MAGIC
# MAGIC O que **não** está aqui, de propósito: `partida_pontual`, `escopo`, qualquer
# MAGIC agregação. O limiar de 15 minutos é uma decisão do cliente — outra seguradora pode
# MAGIC trabalhar com 30. Se ele estivesse cravado na silver, atender esse outro cliente
# MAGIC significaria reprocessar a camada inteira. Na gold, é uma linha de SQL.

# COMMAND ----------

# DBTITLE 1,Listar todas as tabelas silver criadas
# ========================================
# FECHAMENTO: LISTAR TODAS AS TABELAS SILVER
# ========================================
# Comando simples que lista todas as tabelas criadas no schema silver.
# 
# O QUE ESPERAMOS VER (4 tabelas):
# - aerodromos: cadastro de aeroportos brasileiros
# - codigos_operacao: seed table de descrições de códigos
# - empresas: cadastro unificado (nacionais + estrangeiras)
# - vra: tabela principal de voos (1.014.705 linhas)
#
# Se aparecer algo diferente:
# - Tabela faltando: alguma célula de criação não foi executada
# - Tabela extra: pode ser teste/rascunho que deve ser removido
# ========================================

display(spark.sql("SHOW TABLES IN voebem.silver"))