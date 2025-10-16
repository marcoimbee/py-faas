import json
import os
import argparse

def setup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Simple tool to crerate a PyFaaS workflow skeleton for chained function execution')
    parser.add_argument('-n', '--name', default='workflow_template.json', help='The name of the workflow skeleton that will be created')
    parser.add_argument('-p', '--path', default='..', help='Path to the target directory')
    parser.add_argument('-f', '--function_count', type=int, help='How many function the chain will be composed of')
    return parser

def main():
    parser = setup_parser()
    args = parser.parse_args()

    filename = args.name
    path = args.path
    function_count = args.function_count

    if function_count is None:
        print(f'Error: required number of functions (-f)')
        return
    if function_count < 2:
        print(f'Error: a chain is composed of at least two functions')
        return
    if filename != 'workflow_template.json':
        extension = filename.split('.')[-1]
        if extension != 'json':
            print(f"Error: invlid filename {filename}. The file must have extension '.json'")
            return

    structure = {
        'id': '<workflow-id>',
        'entry_function': '<entry-function-name>',
        'functions': {}
    }

    for i in range(function_count):
        structure['functions'][f'f{i+1}'] = {
            'positional_args': [],
            'default_args': {},
            'next': '<next-function-name>' if i < function_count - 1 else '',
            'cache_result': True
        }

    workflow_path = os.path.join(path, filename)
    with open(workflow_path, 'w') as f:
        json.dump(structure, f, indent=4)

    print(f'Created a workflow template for a {function_count}-functions chain in {workflow_path}')


if __name__ == '__main__':
    main()
