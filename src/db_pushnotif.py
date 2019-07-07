from db_subscriber.db_subscriber import DBSubscriber
import argparse
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from utils.logger import Logger


DESCRIPTION = "Utility for oracle table subscription"

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='DESCRIPTION')
    parser.add_argument('-c', '--cfg', help='JSON configuration file', required=False, default="settings.json")
    args = vars(parser.parse_args())

    cfg_file = args["cfg"]
    db_sub = DBSubscriber(json_config_dir=cfg_file)
    try:
        db_sub.subscribe()
        input("Press any key to kill the main thread\n")
    except Exception as e:
        Logger.log.exception(e)
