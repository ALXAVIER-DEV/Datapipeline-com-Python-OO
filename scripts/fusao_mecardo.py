import json
import csv


from data_processed import Data
        
# Path dos arquivos
path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'
path_data_combined = 'data_processed/data_combined.csv'

# Extract data
data_companyA = Data(path_json, 'json')
print(f" Nome das Colunas CompanyA: {data_companyA.columns_name}")
print(f"Tamanho dos dados CompanyA: {data_companyA.size}")

data_companyB= Data(path_csv, 'csv')
print(f" Nome das Colunas CompanyB: {data_companyB.columns_name}")
print(f"Tamanho dos dados CompanyB: {data_companyB.size}")

data_fusion = Data.join(data_companyA, data_companyB)
print(data_fusion.columns_name)
print(data_fusion.size)

# Load data

data_fusion.savedata_to_csv(path_data_combined)
print(f"Os dados foram salvos em {path_data_combined}")

# Transformacao dos dados

key_mapping ={'Nome do Item': 'Nome do Produto',
              'Classificação do Produto': 'Categoria do Produto',
              'Valor em Reais (R$)': 'Preço do Produto (R$)',
              'Quantidade em Estoque': 'Quantidade em Estoque',
              'Nome da Loja': 'Filial',
              'Data da Venda': 'Data da Venda'}

data_companyB.rename_columns(key_mapping)
print(f" Nome das Colunas Dados csv: {data_companyB.columns_name}")





        
        
