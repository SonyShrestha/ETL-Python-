from utilities.variables import Variables
from module.etl import load_data
import argparse


def main(action):
    try:
        if action=='etl':
            load_data.main(action)
        if action=='load_master':
            load_data.main(action)
        if action=='load_transaction':
            load_data.main(action)
        if action=='load_summary':
            load_data.main(action)
        if action == 'load_loan':
            load_data.main(action)
    except Exception as e:
        raise e

if __name__ == "__main__":
    actions=["load_master","load_transaction","load_summary","load_loan","etl"]
    parser=argparse.ArgumentParser()
    parser.add_argument('action',type=str, choices=actions,help="Actions that can be performed")
    args = parser.parse_args()
    action=args.action

    try:
        main(action)
    except Exception as e:
        raise e
