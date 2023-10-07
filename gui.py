import tkinter as tk
import os
from Utils.PySqlite import *
from Utils.Metrics import *
from myconfig import *
from Utils.Preprocess import *
from Utils.Apriori import CustomApriori

from m3ar import M3arAlgorithm
from mm3ar import MM3arAlgorithm
from otherAlgo import OtherAlgorithm

import pandas as pd
import csv
import matplotlib.pyplot as plt
from prettyprinter import cpprint as pp
import pickle
# from apyori import apriori
import os
import sys
import subprocess
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)


algos = [
    "mm3ar",
    "m3ar",
    "oka",
    "mondrian",
    "knn"
]


def messageOut(mes):
    txtMessage.insert(tk.END, mes + '\n')


def getInput():
    algo = selectedAlgo.get()
    k = inpK.get()

    try:
        k = int(k)
        if (k <= 0):
            messageOut("Invalid k")
            return None, None
    except ValueError:
        messageOut("Invalid k")
        return None, None
    messageOut(f"Selected algo='{algo}', k={k}")
    return algo, k


def preprocessData():
    if not os.path.isfile(PRE_DATA_PATH):
        os.makedirs(os.path.dirname(PRE_DATA_PATH), exist_ok=True)
        df = Preprocess(INPUT_DATA_PATH, PRE_DATA_PATH, DATA_SAMPLE)

    df = pd.read_csv(PRE_DATA_PATH, names=USE_COLUMNS,
                     index_col=False, skipinitialspace=True)
    return df


def findAssociationRules(df):
    if os.path.isfile(R_INIT_PATH+'.data'):
        with open(R_INIT_PATH+'.data', 'rb') as f:  # load saved RInitial
            R_inital = pickle.load(f)
        with open(R_INIT_PATH+'-low.data', 'rb') as f:  # load saved RInitial
            R_inital_lower = pickle.load(f)
    else:
        _, R_inital, R_inital_lower = CustomApriori(
            df, MIN_SUP, MIN_CONF, True)
        os.makedirs(os.path.dirname(R_INIT_PATH), exist_ok=True)
        file = open(R_INIT_PATH+'.data', 'wb')
        pickle.dump(R_inital, file)
        file.close()

        file = open(R_INIT_PATH + '-low.data', 'wb')
        pickle.dump(R_inital_lower, file)
        file.close()

    with open(R_INIT_PATH+'.csv', 'w', newline='') as f:
        write = csv.writer(f)
        write.writerow(['lhs', 'rhs', 'sup', 'conf',
                       'ruleStr', 'supAB', 'supA'])
        write.writerows(R_inital)

    return R_inital, R_inital_lower


def runDataAnonymization():
    algo, k = getInput()
    if k is None:
        return

    # run algo
    df = preprocessData()
    messageOut(f"Loaded data with total row:{df.shape[0]}")

    RInital, RInitalLower = findAssociationRules(df)
    messageOut(
        f"R_inital: {len(RInital)}, R_inital_lower: {len(RInitalLower)}")

    anonymityRs = None
    if algo == 'm3ar':
        anonymity = M3arAlgorithm()
        anonymityRs = anonymity.Anonymize(
            df=df, RInitial=RInital, k=k, algo=algo)
    elif algo == 'mm3ar':
        anonymity = MM3arAlgorithm()
        anonymityRs = anonymity.Anonymize(
            df=df, RInitial=RInital, RInitalLow=RInitalLower, k=k, algo=algo)
    else:
        # run data anonymization from other kaylode project, data result at kaylode project
        cmdCommand = f"py E:/shared_folder/kaylode-k-anonymity-main/anonymize.py --method={algo} --k={k}"
        if algo in ["knn", "oka", "kmember"]:
            cmdCommand = f"py E:/shared_folder/kaylode-k-anonymity-main/anonymize.py --method=cluster --k={k} --submethod={algo}"
        print(f'Sending command "{cmdCommand}"')
        process = subprocess.Popen(cmdCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        if error is not None:
            messageOut(str(error))
            return
        messageOut(str(output))
        anonymity = OtherAlgorithm()
        anonymityRs = anonymity.Anonymize(algo=algo, RInitial=RInital, k=k)

    groups = anonymityRs['groups']

    dfAnonymity = pd.read_csv(KANONYMITY_DATA_PATH.format(algo, k), names=USE_COLUMNS,
                              index_col=False, skipinitialspace=True)
    _, RInitalNew, _ = CustomApriori(dfAnonymity, MIN_SUP, MIN_CONF)
    messageOut(f"RInitalNew: {len(RInitalNew)}")
    with open(R_INIT_PATH+'-new.csv', 'w', newline='') as f:
        write = csv.writer(f)
        write.writerow(['lhs', 'rhs', 'sup', 'conf',
                       'ruleStr', 'supAB', 'supA'])
        write.writerows(RInitalNew)

    lrp = LRP(RInital, RInitalNew)
    nrp = NRP(RInital, RInitalNew)
    drp = DRP(RInital, RInitalNew)
    cavg = CAVG(groups, k)
    messageOut('LRP: {} NRP: {} DRP:{} CAVG: {}'.format(lrp, nrp, drp, cavg))
    messageOut("=======================================================")
    messageOut("=======================================================")
    return


if __name__ == "__main__":
    root = tk.Tk()
    root.geometry('670x350')
    root.title("Demo Data Anonymization ")

    tk.Label(root, text="Choose Algo:").grid(
        row=0, column=0, columnspan=3, sticky='W', padx=10, pady=4)
    selectedAlgo = tk.StringVar(root)
    selectedAlgo.set(algos[0])  # default value
    optAlgos = tk.OptionMenu(root, selectedAlgo, *algos).grid(row=0,
                                                              column=1, columnspan=2, sticky='ew', padx=10, pady=4)

    tk.Label(root, text="Input K:").grid(row=1, column=0,
                                         columnspan=3, sticky='W', padx=10, pady=4)
    inpK = tk.Entry(root)
    inpK.insert(0, 20)
    inpK.grid(row=1, column=1, columnspan=2, sticky='EW', padx=10, pady=4)

    # tk.Label(root, text="Input MinSup:").grid(row=2, column=0, columnspan=3, sticky='W', padx=10, pady=4)
    # inpMinSup = tk.Entry(root)
    # inpMinSup.insert(0, 0.5)
    # inpMinSup.grid(row=2, column=1, columnspan=3, sticky='W', padx=10, pady=4)

    # tk.Label(root, text="Input MinConf:").grid(row=3, column=0, columnspan=3, sticky='W', padx=10, pady=4)
    # inpMinConf = tk.Entry(root)
    # inpMinConf.insert(0, 0.5)
    # inpMinConf.grid(row=3, column=1, columnspan=3, sticky='W', padx=10, pady=4)

    tk.Button(root, text='Submit', command=runDataAnonymization, width=20).grid(
        row=4, column=0, columnspan=6, sticky='W', padx=10, pady=4)

    txtMessage = tk.Text(root, height=10)
    txtMessage.grid(row=5, column=0, columnspan=6, sticky='W', padx=10, pady=4)

    root.mainloop()
