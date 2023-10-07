import random
import pandas as pd
import time
import numpy as np
from myconfig import *
import os


class Group:
    id: int
    quasiVals: set
    oriLen: int
    oriTuples: list
    recieveTuples: list
    rulesRelated: list

    def __init__(self, id, quasiValues, dataRows, rulesRelated):
        self.id = id
        self.quasiVals = quasiValues
        self.oriLen = len(dataRows)
        self.oriTuples = dataRows
        self.recieveTuples = []
        self.rulesRelated = rulesRelated

    def __str__(self):
        return f"id:{self.id}, quasiVals: {self.quasiVals},lenOri= {self.oriLen},lenRep= {len(self.recieveTuples)},len(rulesRelated)= {len(self.rulesRelated)}"


class OtherAlgorithm:
    """Run data anonymization on different algorithm using repo https://github.com/kaylode/k-anonymity """
    def groupsTotalTuples(self, groups):
        total = 0
        for g in groups:
            total += self.groupLengh(g)
        return total

    def groupLengh(self, gr):
        return len(gr.oriTuples) + len(gr.recieveTuples)

    def initGroups(self, df, k, RCare=None):
        grID = 1
        kSafe = []
        kUnsafe = []

        groupsByQuasi = df.groupby(QUASI)
        for v, g in groupsByQuasi:
            dataRows = []
            for r in g.iterrows():
                i, data = r
                data.oriGrID = grID
                dataRows.append(data)

            quasiValues = set()
            for attr in QUASI:
                quasiValues.add('{}|{}'.format(attr, data.get(attr)))

            rulesRelated = []
            if RCare is not None:
                for ruleID, r in RCare.items():
                    if r.lhs.intersection(quasiValues) or r.rhs.intersection(quasiValues):
                        rulesRelated.append(ruleID)

            myGroup = Group(grID, quasiValues, dataRows, rulesRelated)

            if len(dataRows) >= k:
                kSafe.append(myGroup)
            else:
                kUnsafe.append(myGroup)

            grID += 1

        return kSafe, kUnsafe

    def Anonymize(self, RInitial, k, algo):

        print("Start ", algo)
        df = pd.read_csv(
            f'E:/shared_folder/kaylode-k-anonymity-main/results/adult/{algo}/adult_anonymized_{k}.csv', index_col=0, delimiter=';')
        # print(df.head())

        SG, UG = self.initGroups(df, k)
        ALL_GROUPS = SG+UG
        UM = []

        lenOfGroupsBefore = {'SG': len(SG), 'SGTotal': self.groupsTotalTuples(SG),
                             'UG': len(UG), 'UGTotal': self.groupsTotalTuples(UG),
                             'UM': len(UM), 'UMTotal': self.groupsTotalTuples(UM), }
        exportDataPath = KANONYMITY_DATA_PATH.format(algo, k)
        os.makedirs(os.path.dirname(exportDataPath), exist_ok=True)
        df.to_csv(exportDataPath, index=False, header=False)

        print('Finish algorithm')
        print('=====================================================')
        print('=====================================================')
        return {
            'groups': SG+UG,
            'totalTime': self.getTotalTime('data/eval_other_result.txt', k, algo),
            'lenOfGroupsBefore': {'SG': 0, 'SGTotal': 0,
                                  'UG': 0, 'UGTotal': 0,
                                  'UM': 0, 'UMTotal': 0},
            'lenOfGroupsUM': {'SG': 0, 'SGTotal': 0,
                              'UG': 0, 'UGTotal': 0,
                              'UM': 0, 'UMTotal': 0, },
            'lenOfGroupsAfter': {'SG': 0, 'SGTotal': 0,
                                 'UG': 0, 'UGTotal': 0,
                                 'UM': 0, 'UMTotal': 0, },
        }

    def getTotalTime(self, dataPath, k, algo):
        df = pd.read_csv(dataPath, index_col=False,
                         delimiter=',', names=['algo', 'k', 'total', 'time_exe'])
        df = df[(df['algo'] == algo)
                & (df['k'] == k)]
        return df.iloc[-1].time_exe
