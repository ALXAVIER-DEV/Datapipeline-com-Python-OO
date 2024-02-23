import json
import csv


def get_columns(data):
    return list(data[-1].keys())

def read_json(path_json):
    data_json = []
    with open(path_json, 'r') as f:
        data_json = json.load(f)
    return data_json    
        
def read_csv(path_csv):
    data_csv = []
    with open(path_csv, 'r') as f:
        spamreader = csv.DictReader(f, delimiter=',')
        for row in spamreader:
            data_csv.append(row)
    return data_csv

# Função para ler os dados
def read_data(path, type_file):
    if path.endswith('.json'):
        return read_json(path)
    elif path.endswith('.csv'):
        return read_csv(path)

def rename_columns(data, key_mapping):
    new_data_csv = []

    for old_dict in data_csv:
        dict_temp = {}
        for old_key, value in old_dict.items():
            dict_temp[key_mapping[old_key]] = value
        new_data_csv.append(dict_temp)
    return new_data_csv

def size_data(data):
    return len(data)

def join(dataA, dataB):
    data_combined_list = []
    data_combined_list.extend(dataA)
    data_combined_list.extend(dataB)
    return data_combined_list

def transform_to_table(data, collumns_name):
    data_combined_table = [collumns_name]

    for row in data:
        line = []
        for collumn in collumns_name:
            line.append(row.get(collumn, 'Indisponível'))
        data_combined_table.append(line)
    return data_combined_table

def savedata_to_csv(data, path):
    with open(path, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(data) 
        

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'
path_data_combined = 'data_processed/data_combined.csv'


data_json = read_data(path_json,'json')
size_data_json = len(data_json)
columns_name_json = get_columns(data_json)    
print(f"Nome das colunas de dados json: {columns_name_json}")
print(f"Tamanho dos dados json: {size_data(data_json)}")

data_csv = read_data(path_csv,'csv')
size_data_csv = len(data_csv)
columns_name_csv = get_columns(data_csv)    
print(columns_name_csv)

key_mapping ={'Nome do Item': 'Nome do Produto',
              'Classificação do Produto': 'Categoria do Produto',
              'Valor em Reais (R$)': 'Preço do Produto (R$)',
              'Quantidade em Estoque': 'Quantidade em Estoque',
              'Nome da Loja': 'Filial',
              'Data da Venda': 'Data da Venda'}

# Transformacao dos dados

data_csv = rename_columns(data_csv, key_mapping)
columns_name_csv = get_columns(data_csv)    
print(columns_name_csv)

data_fusion = join(data_json, data_csv)
columns_name_fusion = get_columns(data_fusion)
len_data_fusion = size_data(data_fusion)
print(columns_name_fusion)
print(len_data_fusion)
    
    
# Save data to csv

data_fusion_table = transform_to_table(data_fusion, columns_name_fusion)    

savedata_to_csv(data_fusion_table, path_data_combined)

print(f"Os dados foram salvos em {path_data_combined}")

        
        
