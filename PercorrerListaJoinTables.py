# Função para percorrer a lista
# Altera o nome da lista de tabelas para cada uma em sua 
# função pro merge do que tem no dataframe lido da ultima DataCarga - definido no synapse
# Para o que tem na camada bronze

# como dentro da landing temos os arquivos salvos em pastas por ano / mes / dia
import datetime

# Obtendo a data atual
data_atual = datetime.date.today()

# Obtendo o dia atual com zero à esquerda
dia = str(data_atual.day).zfill(2)

# Obtendo o mês atual com zero à esquerda
mes = str(data_atual.month).zfill(2)

# Obtendo o ano atual
ano = data_atual.year

# Exibindo a data formatada
data_formatada = f"{dia}/{mes}/{ano}"
print("Data atual:", data_formatada)

dicio_pks = {
    "Ana" : ["ID","OUTRO"],
    "Bia" : ["ID"],
    "Carol" : ["ID","OUTRO"]
}

# Realiza o merge entre a bronze e os dados de hoje
tableList = ["Ana","Bia","Carol"]
start_index = 1

for index, table in enumerate(tableList, start=start_index):
    print(f"Posição: {index}, Valor: {table}")
    mergeCond = ""
    spark.read.parquet(f"/mnt/lake/landing/{table}/{ano}/{mes}/{dia}/").createOrReplaceTempView("novo")
    
    # Construir a condição de mesclagem exclusiva para cada tabela
    for c in dicio_pks[table]:
        if mergeCond == "":
            mergeCond = mergeCond + f"t.{c} = s.{c}"
        else:
            mergeCond = mergeCond + f" and t.{c} = s.{c}"
    
    spark.sql(f"""
    MERGE INTO bronze.{table} t
    USING novo s
    ON {mergeCond}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"{table}, antigo: antigo, novo: novo, mergeCond: {mergeCond}")
