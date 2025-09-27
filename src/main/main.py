from preprocessing import Data
from config import Config
from itertools import combinations

def compare_datasets(datasets):
    
    for d1, d2 in combinations(datasets, 2):
        check = 0

        if d1.schema != d2.schema:
            print(d1.filename, d2.filename, "Different schemas")
            check = 1

        if d1.count() != d2.count():
            print(d1.filename, d2.filename, "Different number of lines")
            check = 1
        
        diff1 = d1.subtract(d2)
        diff2 = d2.subtract(d1)
        
        if diff1.count() != 0 or diff2.count() != 0:
            print(d1.filename, d2.filename, "Different data")
            check = 1
        
        if check == 0:
            print(d1.filename, d2.filename, "Identical datasets")


def main():
    run = Config()

    formatted_data = []

    n_data_files = len(run.config['data'])

    labels = run.config.get('labels', [])
    substitutions = run.modifications.get('substitutions', {})
    creations = run.modifications.get('creations', {})
    calculations = run.modifications.get('calculations', {})

    for data_file in n_data_files:

        i_data = Data()

        i_data.read_csv(data_file, labels)

        # ------ substitutions --------
        if (run.config['substitutions']):

            for label in labels:

                if label in substitutions:

                    orig_vals, new_vals = substitutions[label]

                    i_data.substitute(label, orig_vals, new_vals)

                    
        # ------ calculations ---------
        if (run.config['calculations']):

            for label in labels:

                if label in calculations:

                    orig_vals, new_vals = calculations[label]

                    i_data.calculate(label, orig_vals, new_vals[0])

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
        
        formatted_data.append(i_data)
    
                      
if __name__ == "__main__":
    main()  