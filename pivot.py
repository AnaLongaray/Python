%py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# Consulta SQL
sql_query = """
SELECT 
    year(data) AS Ano,
    codigo,
    cargo,
    count(pessoa) AS Contagem
FROM 
    table 
WHERE 
    data IS NOT NULL 
    AND data >= '2022-01-01' AND month(data) <> 12 and descricao is not null
GROUP BY 
    year(data), codigo, Cargo

# Executa a consulta SQL e carrega os resultados em um DataFrame
df = spark.sql(sql_query)

# Substituir valores nulos em todas as colunas por um valor específico (por exemplo, 0)
df_filled = df.na.fill(0)
#df_filled.display()
# Pivote o DataFrame
pivoted_df = df_filled.groupBy("Ano", "codigo").pivot("Cargo").sum("Contagem")


#pivoted_df1 = pivoted_df.na.fill(0)
# Exibe o DataFrame pivoteado
pivoted_df1.display()
