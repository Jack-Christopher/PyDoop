# import libraries
import json
from collections import OrderedDict

# load config file into a object
# read from config.json
CONFIG = None
with open('config.json', 'r') as f:
    CONFIG = json.load(f, object_pairs_hook=OrderedDict)


# if is master import master otherwise import slave
LOCAL = None
if CONFIG['is_master']:
    from master import Master
    LOCAL = Master(CONFIG)
else:
    from slave import Slave
    LOCAL = Slave(CONFIG)


def main():
    if CONFIG['is_master']:
        LOCAL.start_server()
    else:
        LOCAL.start_client()



# Ejecutar programa principal
main()
