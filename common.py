import os
import json
import yaml
import collections

def conditionalMkdir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def getFilePathDict(directory, file_type):
    file_path_dict = dict()

    os_walk_paths = list(os.walk(directory))

    for os_walk_path in os_walk_paths:
        file_dir, _, f_names = os_walk_path
        f_names = [f_name for f_name in f_names if f_name.endswith(file_type)]
        for f_name in f_names:
            file_path = os.path.join(file_dir, f_name)
            file_path_dict[f_name] = file_path
    return file_path_dict

def saveDictToJsonfile(input_dict, json_file):
    od_dict = collections.OrderedDict(sorted(input_dict.items()))
    write_json = json.dumps(od_dict, indent = 4)
    with open(json_file, 'w') as f:
        f.write(write_json)

def saveDictToYamlfile(input_dict, yaml_file):
    od_dict = collections.OrderedDict(sorted(input_dict.items()))
    write_yaml = yaml.dump(od_dict)
    with open(yaml_file, 'w') as f:
        f.write(write_yaml)

def readJsonFile(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)
    return data
