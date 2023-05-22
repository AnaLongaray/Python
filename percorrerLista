tableList("Ana","Bia","Carol")
start_index = 1

for index, value in enumerate(tableList, start=start_index):

    print(index)
    print(value)
    print(start_index)
    print(f"Posição: {index}, Valor: {value}")
    mergeCond = ""

        # Construir a condição de mesclagem exclusiva para cada tabela
    for table in tableList:
        mergeCond = ""
        for c in dicio_pks[table]:
            if mergeCond == "":
                mergeCond = mergeCond + f"t.{c} = s.{c}"
            else:
                mergeCond = mergeCond + f" and t.{c} = s.{c}"
        mergeCond = ""
        spark.sql(f"""
        MERGE INTO bronze.livroDa_{table} t
        USING novo s
        ON {mergeCond}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """)

        print(f"{table}, antigo: antigo, novo: novo, mergeCond: {mergeCond}")
        
# Função para percorrer a lista
# Altera o nome da lista de tabelas para cada uma em sua 
# função pro merge do que tem no dataframe lido da ultima DataCarga - definido no synapse
# Para o que tem na camada bronze
