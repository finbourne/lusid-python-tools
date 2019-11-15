import argparse
import sys
from lusidtools.logger import LusidLogger


def parse_args():
    ap = argparse.ArgumentParser()
    # ap.add_argument()
    ap.add_argument(
        "-d", "--debug", help=r"print debug messages, expected input: 'debug'"
    )
    return vars(ap.parse_args())


def main(argv):
    args = parse_args(sys.argv[1:])
    LusidLogger(args["debug"])
    # my_func(args)

    return 0


if __name__ == "__main__":
    main(sys.argv)
