import os
import pickle

def find(order=0):    # 0: not order, 1: asc, 2: desc
    base = "data/filtered/"
    dirs = ["%s%s" %(base, _) for _ in os.listdir(base)]
    data = {}
    rev = {1: False, 2: True}
    for fn in dirs:
        with open (fn, "rb") as f:
            data[(fn.split("/")[-1]).replace(".pkl", "")] = len(pickle.load(f))
    if order == 0:
        return data
    else:
        return dict(sorted(data.items(), key=lambda x: x[1], reverse = rev[order]))

if __name__ == "__main__":
    data = find(1)
    for k, v in data.items():
        print("%s\t%d" % (k, v))
