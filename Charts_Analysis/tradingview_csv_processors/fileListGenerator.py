import os

def generate_file_list():
    files = os.listdir('.')
    csv_files = [f for f in files if f.endswith('.csv') and '_' in f]
    
    formatted_list = ',\n        '.join([f"'{f}'" for f in csv_files])
    
    print(f"{formatted_list}")

if __name__ == "__main__":
    generate_file_list()
