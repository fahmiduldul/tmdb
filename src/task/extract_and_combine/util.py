from os import listdir
from os.path import isfile, join

def get_files_in_dir(path: str):
    return [f for f in listdir(path) if isfile(join(path, f))]

def combine_all_file(dir: str, out: str):
    joined_file = open(out, "a+")
    file_paths = get_files_in_dir(dir)

    for file_path in file_paths[:100]:
        with open(join(dir, file_path)) as f:
            payload = f.read()
            joined_file.write(payload+'\n')

    joined_file.close()