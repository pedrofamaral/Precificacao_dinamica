-----------------------------------------------------------------------
-- sql_validations.sql  (SQLite)
-- Validações de esquema, proveniência e sanidade dos dados
-- Rodar com: sqlite3 unified_products.db < sql_validations.sql
-----------------------------------------------------------------------

.headers on
.mode column

-----------------------------------------------------------------------
-- 0) Info geral do arquivo
-----------------------------------------------------------------------
SELECT 'SQLite version' AS what, sqlite_version() AS value;

-----------------------------------------------------------------------
-- 1) Esquema esperado das tabelas principais
-----------------------------------------------------------------------
PRINT '--- PRAGMA unified_listings ---';
PRAGMA table_info(unified_listings);

PRINT '--- PRAGMA canonical_summary ---';
PRAGMA table_info(canonical_summary);

-----------------------------------------------------------------------
-- 2) Presença de colunas-chave (falha se não existir)
-----------------------------------------------------------------------
-- unified_listings precisa conter: marketplace, title, price, url, brand, model, size, canonical_key, collected_at, seller, source_file
SELECT 'missing_cols_unified_listings' AS check,
       GROUP_CONCAT(col) AS missing
FROM (
  SELECT 'marketplace' AS col UNION ALL
  SELECT 'title' UNION ALL
  SELECT 'price' UNION ALL
  SELECT 'url' UNION ALL
  SELECT 'brand' UNION ALL
  SELECT 'model' UNION ALL
  SELECT 'size' UNION ALL
  SELECT 'canonical_key' UNION ALL
  SELECT 'collected_at' UNION ALL
  SELECT 'seller' UNION ALL
  SELECT 'source_file'
)
WHERE col NOT IN (SELECT name FROM pragma_table_info('unified_listings'));

-- canonical_summary precisa conter: canonical_key, brand, model, size, n_listings, marketplaces, min_price, max_price, mean_price, median_price, p10, p90, media_correta, evidence_files
SELECT 'missing_cols_canonical_summary' AS check,
       GROUP_CONCAT(col) AS missing
FROM (
  SELECT 'canonical_key' AS col UNION ALL
  SELECT 'brand' UNION ALL
  SELECT 'model' UNION ALL
  SELECT 'size' UNION ALL
  SELECT 'n_listings' UNION ALL
  SELECT 'marketplaces' UNION ALL
  SELECT 'min_price' UNION ALL
  SELECT 'max_price' UNION ALL
  SELECT 'mean_price' UNION ALL
  SELECT 'median_price' UNION ALL
  SELECT 'p10' UNION ALL
  SELECT 'p90' UNION ALL
  SELECT 'media_correta' UNION ALL
  SELECT 'evidence_files'
)
WHERE col NOT IN (SELECT name FROM pragma_table_info('canonical_summary'));

-----------------------------------------------------------------------
-- 3) Nulos e vazios críticos em unified_listings
-----------------------------------------------------------------------
PRINT '--- Nulos/vazios críticos em unified_listings ---';
SELECT 'rows_without_source_file' AS metric, COUNT(*) AS n
FROM unified_listings
WHERE source_file IS NULL OR TRIM(source_file) = '';

SELECT 'rows_without_canonical_key' AS metric, COUNT(*) AS n
FROM unified_listings
WHERE canonical_key IS NULL OR TRIM(canonical_key) = '';

SELECT 'rows_without_brand_or_model_or_size' AS metric, COUNT(*) AS n
FROM unified_listings
WHERE (brand IS NULL OR TRIM(brand) = '')
   OR (model IS NULL OR TRIM(model) = '')
   OR (size  IS NULL OR TRIM(size)  = '');

-----------------------------------------------------------------------
-- 4) Contagens gerais
-----------------------------------------------------------------------
PRINT '--- Contagens por marketplace e size ---';
SELECT marketplace, COUNT(*) AS n
FROM unified_listings
GROUP BY marketplace
ORDER BY n DESC;

SELECT size, COUNT(*) AS n
FROM unified_listings
GROUP BY size
ORDER BY n DESC
LIMIT 100;

-----------------------------------------------------------------------
-- 5) Casos suspeitos: brand == model
-----------------------------------------------------------------------
PRINT '--- Casos onde brand == model ---';
SELECT brand, model, size, COUNT(*) AS n
FROM unified_listings
WHERE brand <> '' AND model <> '' AND LOWER(brand) = LOWER(model)
GROUP BY 1,2,3
ORDER BY n DESC, brand, model
LIMIT 50;

-----------------------------------------------------------------------
-- 6) Sanidade de preços
-----------------------------------------------------------------------
PRINT '--- Preços negativos, zero ou outliers (resumo) ---';
SELECT 'negative_or_zero_prices' AS metric, COUNT(*) AS n
FROM unified_listings
WHERE price IS NULL OR price <= 0;

SELECT brand, model, size, n_listings, min_price, p10, media_correta, p90, max_price
FROM canonical_summary
WHERE n_listings >= 5
ORDER BY (max_price - min_price) DESC
LIMIT 50;

-----------------------------------------------------------------------
-- 7) Amostras de evidence_files (proveniência agregada)
-----------------------------------------------------------------------
PRINT '--- Amostras de evidence_files ---';
SELECT brand, model, size, n_listings, marketplaces,
       SUBSTR(evidence_files, 1, 200) AS evidence_snippet
FROM canonical_summary
WHERE evidence_files IS NOT NULL AND TRIM(evidence_files) <> ''
ORDER BY n_listings DESC
LIMIT 50;

-----------------------------------------------------------------------
-- 8) Conferir tails esperados dos diretórios de entrada
--    Ajuste os termos se seus diretórios forem diferentes.
-----------------------------------------------------------------------
PRINT '--- Presença de MercadoLivre/data/raw ---';
SELECT COUNT(*) AS n
FROM canonical_summary
WHERE evidence_files LIKE '%MercadoLivre/data/raw/%';

PRINT '--- Presença de MagazineLuiza/data/raw ---';
SELECT COUNT(*) AS n
FROM canonical_summary
WHERE evidence_files LIKE '%MagazineLuiza/data/raw/%';

PRINT '--- Presença de pneustore/dados/raw ---';
SELECT COUNT(*) AS n
FROM canonical_summary
WHERE evidence_files LIKE '%pneustore/dados/raw/%';

-----------------------------------------------------------------------
-- 9) Verificar que source_file usa sempre '/' (compat Windows/Unix)
-----------------------------------------------------------------------
PRINT '--- source_file com \\ (indesejado) ---';
SELECT COUNT(*) AS n_with_backslash
FROM unified_listings
WHERE source_file LIKE '%\\%';

-----------------------------------------------------------------------
-- 10) Checagens de consistência canonical_key <-> (brand, model, size)
-----------------------------------------------------------------------
PRINT '--- canonical_key mapeando para múltiplos (brand,model,size) ---';
SELECT canonical_key, COUNT(DISTINCT brand || '|' || model || '|' || size) AS distinct_triplets
FROM unified_listings
GROUP BY canonical_key
HAVING COUNT(DISTINCT brand || '|' || model || '|' || size) > 1
ORDER BY distinct_triplets DESC
LIMIT 50;

PRINT '--- (brand,model,size) com múltiplos canonical_key ---';
SELECT brand, model, size, COUNT(DISTINCT canonical_key) AS n_keys
FROM unified_listings
GROUP BY brand, model, size
HAVING COUNT(DISTINCT canonical_key) > 1
ORDER BY n_keys DESC, brand, model, size
LIMIT 50;

-----------------------------------------------------------------------
-- 11) Duplicatas exatas de listing (mesmo url + marketplace)
-----------------------------------------------------------------------
PRINT '--- Duplicatas potenciais (url, marketplace) ---';
SELECT marketplace, url, COUNT(*) AS n
FROM unified_listings
GROUP BY marketplace, url
HAVING COUNT(*) > 1
ORDER BY n DESC, marketplace
LIMIT 50;

-----------------------------------------------------------------------
-- 12) Amostras de proveniência cruzada: mesmo (brand,model,size) vindo de múltiplos arquivos
-----------------------------------------------------------------------
PRINT '--- Itens com múltiplas fontes ---';
WITH agg AS (
  SELECT brand, model, size,
         COUNT(*) AS n_rows,
         COUNT(DISTINCT source_file) AS n_sources,
         GROUP_CONCAT(DISTINCT source_file, ' | ') AS sources
  FROM unified_listings
  WHERE brand <> '' AND model <> '' AND size <> ''
  GROUP BY brand, model, size
)
SELECT brand, model, size, n_rows, n_sources, SUBSTR(sources, 1, 300) AS sources_snippet
FROM agg
WHERE n_sources >= 2
ORDER BY n_sources DESC, n_rows DESC
LIMIT 100;

-----------------------------------------------------------------------
-- 13) Verificar se evidence_files cobre as mesmas fontes do unified_listings
--     (amostra por TOP 100 n_listings)
-----------------------------------------------------------------------
PRINT '--- Cobertura de evidence_files vs fontes reais (amostra) ---';
WITH top AS (
  SELECT brand, model, size, n_listings, evidence_files
  FROM canonical_summary
  ORDER BY n_listings DESC
  LIMIT 100
),
src AS (
  SELECT brand, model, size,
         GROUP_CONCAT(DISTINCT source_file, ',') AS real_sources
  FROM unified_listings
  GROUP BY brand, model, size
)
SELECT t.brand, t.model, t.size,
       LENGTH(t.evidence_files) AS len_evidence,
       LENGTH(s.real_sources)   AS len_real,
       SUBSTR(t.evidence_files, 1, 200) AS evidence_snippet,
       SUBSTR(s.real_sources, 1, 200)   AS real_snippet
FROM top t
JOIN src s USING (brand, model, size)
ORDER BY t.n_listings DESC
LIMIT 50;

-----------------------------------------------------------------------
-- 14) Marketplaces listados na canonical_summary vs unified_listings
-----------------------------------------------------------------------
PRINT '--- Divergência de marketplaces (amostra) ---';
WITH src AS (
  SELECT brand, model, size,
         GROUP_CONCAT(DISTINCT marketplace, ',') AS real_mkts
  FROM unified_listings
  GROUP BY brand, model, size
),
cmp AS (
  SELECT cs.brand, cs.model, cs.size, cs.marketplaces, s.real_mkts
  FROM canonical_summary cs
  JOIN src s USING (brand, model, size)
)
SELECT brand, model, size,
       SUBSTR(marketplaces, 1, 80) AS marketplaces_summary,
       SUBSTR(real_mkts, 1, 80)    AS marketplaces_real
FROM cmp
WHERE marketplaces IS NOT NULL
  AND TRIM(marketplaces) <> ''
  AND marketplaces <> real_mkts
LIMIT 50;

-----------------------------------------------------------------------
-- 15) Amostras cruas (debug)
-----------------------------------------------------------------------
PRINT '--- 10 linhas cruas de unified_listings ---';
SELECT *
FROM unified_listings
LIMIT 10;

PRINT '--- 10 linhas cruas de canonical_summary ---';
SELECT *
FROM canonical_summary
LIMIT 10;

-----------------------------------------------------------------------
-- FIM
-----------------------------------------------------------------------
