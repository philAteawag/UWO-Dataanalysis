# coding: utf-8

"""Check how to merge files, check if data is consitent (read source data from life system and compare to sqlite), check if only the data is in there that we want to publish, is there unnecessary data, export some example images and check them, ..."""


import argparse


def main(args: argparse.Namespace) -> None:
    print("main!")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-sd", "--sourcedirectory", default="/path/to/db_file")
    parser.add_argument("-fn", "--filename", default="dp.sqlite")
    parser.add_argument("-cl", "--contentlist", default="package_information.csv")

    args = parser.parse_args()

    main(args)
