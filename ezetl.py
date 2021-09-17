import sys
import argparse
import os
import shutil
import glob
import csv
import json
import xml.etree.ElementTree as ET

def breakHeader(header):
    result = []
    for element in header:
        text = ''
        number = ''
        for char in element:
            if str.isdigit(char) == True:
                number += char
            else:
                text += char
        
        result.append([text, int(number)])

    return result

def restoreHeader(header):
    result = []
    for element in header:
        text = element[0]
        number = element[1]

        result.append(text + str(number))
    return result

def sortHeader(header):
    return restoreHeader(sorted(breakHeader(header)))

def createParser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', '-i', default='./source', nargs='?')
    parser.add_argument('--output', '-o', default='.', nargs='?')
    parser.add_argument('--quiet', '-q', action='store_const', const=False)
    parser.add_argument('--recursiveSearch', '-r', action='store_const', const=False)
    parser.add_argument('--tableName', '-T', default=['*.csv','*.xml','*.json'], nargs='+')
    # TODO: добавить -h если будет время

    return parser
    
def find_files(path, names, is_recursive):
    result = []
   
    for name in names:
        if is_recursive == True:
            result += glob.glob(f'{path}/{name}')
            result += glob.glob(f'{path}/**/{name}')
        else:
            result += glob.glob(f'{path}/{name}')
    return result

#TODO Переделать систему выдачи имен
class TableConvertor(object):
    def __init__(self, save_path):
        self.save_path = save_path 
    
    def convert_table(self,table_path):
        if table_path.endswith('.csv'):
            self.convert_csv_to_tsv(table_path)
            return True
        elif  table_path.endswith('.json'):
            self.convert_json_to_tsv(table_path)
            return True
        elif  table_path.endswith('.xml'):
            self.convert_xml_to_tsv(table_path)
            return True
        else:
            return False

    def convert_csv_to_tsv(self,table_path):
        copy_table_path = self.getTempFileName()

        with open(table_path, 'r') as source, open(copy_table_path, 'w+', newline='') as target:
            reader = csv.reader(source)
            writer = csv.writer(target, delimiter='\t')
                
            for row in reader:
                if len(row) != 0:
                    writer.writerow(row)
        
        self.temp_table_count += 1 

    def convert_json_to_tsv(self, table_path):
        copy_table_path = self.getTempFileName()
        with open(table_path, 'r') as source, open(copy_table_path, 'w+', newline='') as target:
            table_data = json.load(source)
            # Нужно как-то решить проблемму с выбором колекции
            colection = table_data['fields']

            writer = csv.writer(target, delimiter='\t')
            header_flag = False
            for obj in colection:
                if header_flag == False:
                    writer.writerow(obj.keys())
                    header_flag = True
                writer.writerow(obj.values())
        
        self.temp_table_count += 1

    def convert_xml_to_tsv(self,table_path):
        copy_table_path = self.getTempFileName()
        with open(table_path, 'r') as source, open(copy_table_path, 'w+', newline='') as target:
            writer = csv.writer(target, delimiter='\t')
            tree = ET.parse(source)
            tree_root = tree.getroot()

            header = []
            columns = []
            for object in tree_root.find('objects'):
                header.append(object.get('name'))
                column = []
                for value in object.findall('value'):
                    column.append(value.text)
                columns.append(column)
            
            writer.writerow(header)
            rows_count = len(columns[0])

            for i in range(rows_count):
                temp_row = []
                for column in columns:
                    temp_row.append(column[i])
                writer.writerow(temp_row)

        self.temp_table_count += 1 

    def getTempFileName(self):
        copy_table_path = f'{self.save_path}/temp_file_{self.temp_table_count}.tsv'
        return copy_table_path
        
    save_path = '.'
    temp_table_count = 0

# TODO Модифицировать функцию сортировки заголовков
class TableUnifier(object):
    def __init__(self, source_path, save_path):
        self.source_path = source_path
        self.save_path = save_path           

    def getUnitedHeader(self):
        for table in glob.glob(f'{self.source_path}/*.tsv'):
            with open(table, 'r') as tb:
                reader = csv.reader(tb, delimiter='\t')
                self.united_header += next(reader)
        
        self.united_header = sortHeader(set(self.united_header))
        return self.united_header

    def transformTableForUnion(self,table_path):
        columns = []
        rows = []
        columns_count = 0
        rows_count = 0
        old_header = []
        
        # Чтение таблицы и разбитие ее на столбцы
        with open(table_path, 'r') as table:            
            reader = csv.reader(table, delimiter='\t')
            for row in reader:
                rows.append(row)
        
        rows_count = len(rows)
        old_header = rows[0]
        columns_count = len(old_header) 
        
        for i in range(columns_count):
            temp_column = []
            for row in rows:
                temp_column.append(row[i])
            columns.append(temp_column)

        # Поиск недостающих столбцов и добавление их, если отсутствуют
        # В результате, в таблице будут столбцы из всех объединяемых таблиц
        # Примечание: Если изначально столбец отстутствует, то он добавится без данных, только заголовок
        for element in self.united_header:
            if element in old_header == False:
                new_column = [element] + [' ' for i in range(rows_count - 1)]
                columns.append(new_column)

        # Сортировка столбцов
        columns = sorted(columns)

        # Запись обработанной таблицы обратно в файл
        with open(table_path, 'w', newline='') as table:
            writer = csv.writer(table, delimiter='\t')        
            
            for i in range(rows_count):
                temp_row = []
                for column in columns:
                    temp_row.append(column[i])
                writer.writerow(temp_row)

    def uniteTables(self):
        # Получение общего заголовка
        self.getUnitedHeader()

        # Преобразование таблиц для объединения
        for table in glob.glob(f'{self.source_path}/*.tsv'):
            self.transformTableForUnion(table)

        # Открытие/создание файла для результирующей таблицы
        with open(f'{self.save_path}/ezetl_result.tsv', 'w', newline='') as result_file:
            writer = csv.writer(result_file, delimiter='\t')    
            # Запись заголовка таблицы
            writer.writerow(self.united_header)

            # Обход всех таблиц и запись их содержимого в файл (кроме заголовков)
            for table in glob.glob(f'{self.source_path}/*.tsv'):
                with open(table, 'r') as tb:
                    reader = csv.reader(tb, delimiter='\t')
                    next(reader)
                    for row in reader:
                        if len(row) != 0:
                            writer.writerow(row)
        
        return f'{self.save_path}/ezetl_result.tsv'

    save_path = '.'
    source_path = '.'
    united_header = []

if __name__ == '__main__':
   # Этап 0. Принятие и обработка параметров
    parser = createParser()
    namespace = parser.parse_args(sys.argv[1:])

    # TODO: Добввить обработку исключений и вывод сообщений через консоль 
    if os.path.exists(namespace.input) == False:
        sys.exit(-1)

    if os.path.exists(namespace.output) == False:
        sys.exit(-1)

    if os.path.exists('temp') == True:
        shutil.rmtree('temp')

    # Этап 1. поиск подходящих таблиц
    search_result = find_files(namespace.input, namespace.tableName, namespace.recursiveSearch)
    
    if len(search_result) == 0:
        print(f"Warning! File(s) {namespace.tableName} in {namespace.input} not found.")
        if namespace.recursiveSearch == False:
            print("Try use -r flag for recursive search in directory")
        sys.exit(0)

    # Нужна валидация файлов на этом этапе, иначе некоректное преобразование таблиц
    # Этап 2. Копирование файлов из списка и преобразование в формат `.tsv` 
    
    if os.path.exists('temp') == False:
        os.mkdir('temp')

    convertor = TableConvertor('./temp')
    for file in search_result:
        convertor.convert_table(file)

    # Этап 3. Объединение файлов
    unifer = TableUnifier('./temp', namespace.output)
    result_file = unifer.uniteTables()

    # Этап 4. Сортировка таблицы
    # TODO Создать класс для операций над таблицей

    # Этап 5. Операции над данными таблицы

    # Этап N. Удаление временных файлов и завершение программы
    if os.path.exists('temp') == True:
        shutil.rmtree('temp')
    sys.exit(0)

    