import requests
import json
from pyspark.sql.functions import *
import pandas as pd

# Requisição de API e Transformar em DataFrame dentro do DataBricks
url = "http://api-sua-url.com"
token = "senhaTokenDeAcesso"
headers = {'Authorization': f'Bearer {token}'} # aqui pode ser Basic, vai depender do disponibilizador dos dados se houver mais alguma informação, como veremos mais abaixo poderão ser incrementador parâmetros também

def traz_dados():
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
            data = response.json() 
            print(data)
    else:
            print("Erro: ", response.status_code, response.text)
    return response.content

dicionario = json.loads(traz_dados())
    #print(dicionarioplanosAcao)

dicionario = [(key, value) for dict in dicionario['data'] for key, value in dict.items()]
dataFrame = pd.DataFrame(dicionario['data'], columns=['column1', 'column2','column3'])


