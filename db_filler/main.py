from argparse import ArgumentParser
from configparser import ConfigParser

from load_records import load_records
from initialize_db import initialize_db


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "--is_test", type=bool, default=False, help="If True, than smaller table will be created. Default: False"
    )
    parser.add_argument("--db_config", type=str, default="db.ini", help="Path to database config. Default: db.ini")

    args = parser.parse_args()
    config = ConfigParser()
    config.read(args.db_config)

    initialize_db(config, args.is_test)
    print("Database created")

    load_records(config, args.is_test)
    print("Subtitles loaded")


if __name__ == "__main__":
    main()
