# Cria dataFrame dos itens da lista para serem usados no Databricks

# transformar as tabelas em df

tableList = ["Ana","Bia","Carol"]
start_index = 1

for index, table in enumerate(tableList, start=start_index):
    print(f"Posição: {index}, Valor: {table}")
    df_name = f"df_{table}" 
    
    locals()[df_name] = spark.read.table(f"bronze.{table}")
    
    
