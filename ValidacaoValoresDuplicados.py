def verificar_duplicatas(tabela, coluna):
    # Contar o número de ocorrências de cada valor na coluna
    contagem = tabela.groupBy(coluna).agg(count(coluna).alias('count'))
    
    # Filtrar os valores com contagem maior que 1 (duplicatas)
    duplicatas = contagem.filter(contagem['count'] > 1)
    
    if duplicatas.count() > 0:
        print(f"A coluna '{coluna}' contém valores duplicados.")
        duplicatas.show()
    else:
        print(f"A coluna '{coluna}' não contém valores duplicados.")

# Exemplo de uso
tabela = sua_tabela  # Insira o nome da sua tabela 
coluna_verificar = "sua_coluna" # Insira o nome da coluna que deseja verificar

verificar_duplicatas(tabela, coluna_verificar)
