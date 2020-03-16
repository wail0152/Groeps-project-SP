from MongoDB_to_MySQL import *

buids = {}


def get_buids():
    counter = 0
    for entry in db["profiles"].find():
        try:
            for buid in entry["buids"]:
                buids[buid] = counter
        except KeyError:
            continue
        counter += 1
    print("Done with the setup of the buids.")


def link_profile_session(entry):
    return buids[entry["buid"][0]]
