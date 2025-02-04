import pandas as pd
import argparse


def main():
    parser = argparse.ArgumentParser(description='Read an Excel file and print its contents')
    parser.add_argument('file', type=str, help='Path to the Excel file')
    args = parser.parse_args()

    try:
        # Read the Excel file (default engine openpyxl is automatically used for .xlsx files)
        df = pd.read_excel(args.file)
        print('Excel file loaded successfully:')
        print(df)
    except Exception as e:
        print('Error reading the Excel file222:', e)


if __name__ == '__main__':
    main()
