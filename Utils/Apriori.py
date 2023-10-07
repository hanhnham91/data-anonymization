from itertools import chain, combinations
from collections import defaultdict
import pandas as pd
from pandas.api.types import is_numeric_dtype
import numpy as np


def powerset(s):  # sub-set of a set [a,b,c] -> [a],[b],[c],[a,b],[a,c],[b,c]
    return chain.from_iterable(combinations(s, r) for r in range(1, len(s)))


def getAboveMinSup(itemSet, itemSetList, minSup, globalItemSetWithSup):
    freqItemSet = set()
    localItemSetWithSup = defaultdict(int)

    for item in itemSet:
        for itemSet in itemSetList:
            if item.issubset(itemSet):
                globalItemSetWithSup[item] += 1
                localItemSetWithSup[item] += 1

    for item, supCount in localItemSetWithSup.items():
        support = float(supCount / len(itemSetList))
        if (support >= minSup):
            freqItemSet.add(item)

    return freqItemSet


def getUnion(itemSet, length):
    return set([i.union(j) for i in itemSet for j in itemSet if len(i.union(j)) == length])


def pruning(candidateSet, prevFreqSet, length):
    tempCandidateSet = candidateSet.copy()
    for item in candidateSet:
        subsets = combinations(item, length)
        for subset in subsets:
            # if the subset is not in previous K-frequent get, then remove the set
            if (frozenset(subset) not in prevFreqSet):
                tempCandidateSet.remove(item)
                break
    return tempCandidateSet


def associationRule(freqItemSet, itemSetWithSup, minConf, lenTotal: int):
    rules = []
    for k, itemSet in freqItemSet.items():
        for item in itemSet:
            subsets = powerset(item)
            for s in subsets:
                # Rule A->B
                # s = count(A+B)/Total
                # c= count(A+B)/count(A)

                countAandB = itemSetWithSup[item]
                countA = itemSetWithSup[frozenset(s)]

                support = itemSetWithSup[item]/lenTotal

                confidence = float(
                    itemSetWithSup[item] / itemSetWithSup[frozenset(s)])
                if (confidence > minConf):
                    sortedlhs = sorted(set(s))
                    sortedrhs = sorted(set(item.difference(s)))

                    rules.append([set(s),
                                  set(item.difference(s)),
                                  support,
                                  confidence,
                                  ','.join(sortedlhs) + '->' +
                                  ','.join(sortedrhs),
                                  countAandB,
                                  countA])
    return rules


def getItemSetFromList(itemSetList):
    tempItemSet = set()

    for itemSet in itemSetList:
        for item in itemSet:
            tempItemSet.add(frozenset([item]))

    return tempItemSet


def CustomApriori(df, minSup, minConf, isGetLow=False):

    if isGetLow:
        minSup -= 0.1
        minConf -= 0.1

    # print(df.head())
    df1 = df.copy(deep=True)

    # print(df1.info())
    # print(df1.head())
    # # region convert numeric attribute
    # # age: cut into levels Young (0-25), Middle-aged (26-45), Senior (46-65) and Old (66+)
    # if 'age' in df1.columns and is_numeric_dtype(df1['age']):
    #     # df1['age'] = pd.to_numeric(df1['age'])# convert column to numeric

    #     # df1['age'] = pd.cut(df1['age'], [-1, 31.6, 46.2, 60.8, 75.4, 90.0], labels=[
    #     #     '<31', '31-46', '46-60', '60-75', '>75'])

    #     # [17.0, 24.3, 31.6, 38.9, 46.2, 53.5, 60.8, 68.1, 75.4, 82.7, 90.0]
    #     df1['age'] = pd.cut(df1['age'], [-1, 24.3, 31.6, 38.9, 46.2, 53.5, 60.8, 68.1, 75.4, 82.7, 100], labels=[
    #         '<24', '24-31', '31-38', '38-46', '46-53', '53-60', '60-68', '68-75', '75-82', '>82'])

    # # hours-per-week: cut into levels Part-time (0-25), Full-time (25-40), Over-time (40-60) and Too-much (60+)
    # if 'hours-per-week' in df1.columns:

    #     # df1['hours-per-week'] = pd.to_numeric(df1['hours-per-week'])# convert column to numeric
    #     # 1. , 10.8, 20.6, 30.4, 40.2, 50. , 59.8, 69.6, 79.4, 89.2, 99.
    #     df1['hours-per-week'] = pd.cut(df1['hours-per-week'], [-1, 10.8, 20.6, 30.4, 40.2, 50, 59.8, 69.6, 79.4, 89.2, 100.], labels=[
    #         '<10', '10-20', '20-30', '30-40', '40-50', '50-60', '60-70', '70-80', '80-90', '>90'])
    #     # df1['hours-per-week'] = pd.cut(df1['hours-per-week'], [0, 25, 40, 60, 168], labels=[
    #     #     "Part-time", "Full-time", "Over-time", "Workaholic"])

    # # capital-gain and capital-loss: each cut into levels None (0), Low (0 < median of the values greater zero < max) and High (>=max)
    # if 'capital-gain' in df1.columns:

    #     # df1['capital-gain'] = pd.to_numeric(df1['capital-gain'])# convert column to numeric

    #     # print("max capital gain: ", df['capital-gain'].max())
    #     # select where value >0 to cal median
    #     # extract_capital_gain = df.loc[df['capital-gain'] > 0]
    #     # med_capital_gain = extract_capital_gain.median()  # = 7298
    #     # df1['capital-gain'] = pd.cut(df1['capital-gain'],
    #     #                              [-1, 0, 7298, 100000], labels=["Zero", "Low", "High"])

    #     # [0, 9999.9,19999.8,29999.7,39999.6,49999.5,59999.4,69999.3,79999.2,89999.1,99999. ]
    #     df1['capital-gain'] = pd.cut(df1['capital-gain'], [-1, 9999.9, 19999.8, 29999.7, 39999.6, 49999.5, 59999.4, 69999.3, 79999.2, 89999.1, 100000],
    #                                  labels=['<10k', '10k-20k', '20k-30k', '30k-40k', '40k-50k', '50k-60k', '60k-70k', '70k-80k', '80k-90k', '>90k'])

    # if 'capital-loss' in df1.columns:
    #     # df1['capital-loss'] = pd.to_numeric(df1['capital-loss'])# convert column to numeric
    #     # # select where value >0 to cal median
    #     # extract_capital_loss = df.loc[df['capital-loss'] > 0]
    #     # med_capital_loss = extract_capital_loss.median()  # = 1887
    #     # df1['capital-loss'] = pd.cut(df1['capital-loss'],
    #     #                              [-1, 0, 1887, 100000], labels=["Zero", "Low", "High"])
    #     # 0. ,  435.6,  871.2, 1306.8, 1742.4, 2178. , 2613.6, 3049.2,       3484.8, 3920.4, 4356.
    #     df1['capital-loss'] = pd.cut(df1['capital-loss'],
    #                                  [-1, 435.6, 871.2, 1306.8, 1742.4, 2178,
    #                                      2613.6, 3049.2, 3484.8, 3920.4, 4356],
    #                                  labels=['<435', '435-871', '871-1306', '1306-1742', '1742-2178', '2178-2613', '2613-3049', '3049-3484', '3484-3920', '>3920'])

    # # endregion

    # concat with column name to remove column name
    for column in df1.columns:
        df1[column] = column + "|" + df1[column].astype(str)

    # format to dataframe to array
    arr_df = df1.to_numpy()

    C1ItemSet = getItemSetFromList(arr_df)
    # Final result global frequent itemset
    globalFreqItemSet = dict()
    # Storing global itemset with support count
    globalItemSetWithSup = defaultdict(int)

    L1ItemSet = getAboveMinSup(
        C1ItemSet, arr_df, minSup, globalItemSetWithSup)
    currentLSet = L1ItemSet
    k = 2

    # Calculating frequent item set
    while (currentLSet):
        # Storing frequent itemset
        globalFreqItemSet[k-1] = currentLSet
        # Self-joining Lk
        candidateSet = getUnion(currentLSet, k)
        # Perform subset testing and remove pruned supersets
        candidateSet = pruning(candidateSet, currentLSet, k-1)
        # Scanning itemSet for counting support
        currentLSet = getAboveMinSup(
            candidateSet, arr_df, minSup, globalItemSetWithSup)
        k += 1

    rules = associationRule(
        globalFreqItemSet, globalItemSetWithSup, minConf,  len(arr_df))

    # rules.sort(key=lambda x: x[2])

    if not isGetLow:
        return globalFreqItemSet, rules, None

    minSup += 0.1
    minConf += 0.1
    rulesUpper = [r for r in rules if r[2] >= minSup and r[3] >= minConf]
    rulesLower = [r for r in rules if r[2] < minSup or r[3] < minConf]
    return globalFreqItemSet, rulesUpper, rulesLower
