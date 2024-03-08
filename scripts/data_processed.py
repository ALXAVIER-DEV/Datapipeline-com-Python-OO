import json
import csv

class Data:
    def __init__(self, path, data_type) :
        self.path = path
        self.data_type = data_type
        self.data = self.read_data()
        self.columns_name = self.get_columns()
        self.size = self.size_data()
        
        
    def read_json(self):
        data_json = []
        with open(self.path, 'r') as f:
            data_json = json.load(f)
        return data_json    
        
    def read_csv(self):
        data_csv = []
        with open(self.path, 'r') as f:
            spamreader = csv.DictReader(f, delimiter=',')
            for row in spamreader:
                data_csv.append(row)
        return data_csv

    # Função para ler os dados
    def read_data(self):
        data = []
        if self.data_type == 'json':
            data = self.read_json()
            
        elif self.data_type == 'csv':
            data = self.read_csv()
        elif self.data_type == 'list':
            data = self.path
            self.path = 'list in memory'
            
        return data
    
    def get_columns(self):
        return list(self.data[-1].keys())
    
    def rename_columns(self, key_mapping):
        new_data = []

        for old_dict in self.data:
            dict_temp = {}
            for old_key, value in old_dict.items():
                dict_temp[key_mapping[old_key]] = value
            new_data.append(dict_temp)
            
        self.data = new_data
        self.columns_name = self.get_columns()
        
    def size_data(self):
        return len(self.data)
    
    def join(dataA, dataB):
        data_combined_list = []
        data_combined_list.extend(dataA.data)
        data_combined_list.extend(dataB.data)
        return Data(data_combined_list, 'list')
    
    def transform_to_table(self):
        
        data_combined_table = [self.columns_name]

        for row in self.data:
            line = []
            for collumn in self.columns_name:
                line.append(row.get(collumn, 'Indisponível'))
            data_combined_table.append(line)
        return data_combined_table
    
    def savedata_to_csv(self,path):
        
        data_combined_table = self.transform_to_table()
        
        with open(path, 'w') as f:
            writer = csv.writer(f)
            writer.writerows(data_combined_table) 
