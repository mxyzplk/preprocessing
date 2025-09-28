from preprocessing import Data
from config import Config
from itertools import combinations
import os
from functools import reduce

def compare_datasets(datasets):

    main_dir = os.path.dirname(os.path.abspath(__file__))
    results_dir = os.path.join(main_dir, '../results')
    filepath = os.path.join(results_dir, 'results.log')    
    
    with open(filepath, "w") as f:
        for (df1, name1), (df2, name2) in combinations(datasets, 2):
            check = 0

            # Compara schemas
            if df1.schema != df2.schema:
                f.write(f"{name1}, {name2}: Different schemas\n")
                check = 1

            # Compara número de linhas
            if df1.count() != df2.count():
                f.write(f"{name1}, {name2}: Different number of lines\n")
                check = 1

            # Compara conteúdo
            diff1 = df1.subtract(df2)
            diff2 = df2.subtract(df1)
            if diff1.count() != 0 or diff2.count() != 0:
                f.write(f"{name1}, {name2}: Different data\n")
                check = 1

            # Se não houve diferenças
            if check == 0:
                f.write(f"{name1}, {name2}: Identical datasets\n")

def main():
    run = Config()

    formatted_data = []

    labels = run.config.get('labels', [])
    substitutions = run.modifications.get('substitutions', {})
    creations = run.modifications.get('creations', {})
    calculations = run.modifications.get('calculations', {})

    main_dir = os.path.dirname(os.path.abspath(__file__))
    results_dir = os.path.join(main_dir, '../results')
    os.makedirs(results_dir, exist_ok=True)

    for entry in run.config['data']:

        print(entry['file'])

        i_data = Data()

        i_data.read_csv(entry['file'], labels)

        # ------ remove quotes -------
        for label in labels:
            
            i_data.remove_quotes(label)

        # ------ substitutions --------
        if (run.config['substitutions']):

            for label in labels:

                if label in substitutions:

                    orig_vals, new_vals = substitutions[label]['parameters']

                    i_data.substitute(label, orig_vals, new_vals, substitutions[label]['dtype'])

                    
        # ------ calculations ---------
        if (run.config['calculations']):

            for label in labels:

                if label in calculations:

                    method = calculations[label]['method']

                    i_data.calculate(label, method)

        # ------ creations -----------
        if (run.config['creations']):

            for label in creations:

                creation_cfg = creations[label]
                creation_method = creation_cfg["function"]
                
                if creation_method in ["sum_columns", "mean_columns", "max_columns", "min_columns"]:
                    creation_cols = creation_cfg["columns"]
                    i_data.create_column(label, creation_cols, creation_method)
                
                elif creation_method == "custom_expr":
                    expression_str = creation_cfg["expression"]
                    i_data.create_column_custom(label, expression_str)
        
        formatted_data.append([i_data.data, i_data.filename])

        if run.config['print_results']:
            opath = os.path.join(results_dir, entry['output_folder'])
            os.makedirs(opath, exist_ok=True)
            i_data.data.write.csv(opath, header=True, mode="overwrite")    

    
    compare_datasets(formatted_data)

    dfs = [item[0] for item in formatted_data]

    if run.config["concatenate_lines"]:
        result_data = reduce(lambda a, b: a.unionByName(b), dfs)
        if run.config['print_results']:
            opath = os.path.join(results_dir, "total")
            os.makedirs(opath, exist_ok=True)
            result_data.write.csv(opath, header=True, mode="overwrite")    


                      
if __name__ == "__main__":
    main()      