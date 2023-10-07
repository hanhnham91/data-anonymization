import random
import pandas as pd
import time
import numpy as np
from myconfig import *
import os
import copy

class ConsiderMMGroup:

    def __init__(self, riskReduce, numMigrate, cost, migrateDirect, bestGroupID, GroupLengh, R_affected):
        self.riskReduce = riskReduce
        self.numMigrate = numMigrate
        self.migrateDirect = migrateDirect
        self.bestGroupID = bestGroupID
        self.GroupLengh = GroupLengh
        self.R_affected = R_affected
        self.cost = cost

    def __str__(self):
        return f"riskReduce= {self.riskReduce},numMigrate= {self.numMigrate},cost= {self.cost},migrateDirect= {self.migrateDirect},bestGroupID= {self.bestGroupID},GroupLengh= {self.GroupLengh}"


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


class Rule:
    id: int
    lhs: set
    rhs: set
    sup: float
    conf: float
    budget: int
    ruleStr: str

    def __init__(self, id, lhs, rhs, sup, conf, budget, ruleStr):
        self.id = id
        self.lhs = lhs
        self.rhs = rhs
        self.sup = sup
        self.conf = conf
        self.budget = budget
        self.ruleStr = ruleStr

    def __str__(self):
        return f"{self.lhs} -> {self.rhs},sup= {self.sup},conf= {self.conf},budget= {self.budget}"


class M3arAlgorithm:
    """The implemetation of algorithm on paper `Protecting Privacy While Discovering and Maintaining Association Rules`"""

    def groupsTotalTuples(self, groups):
        total = 0
        for g in groups:
            total += self.groupLengh(g)
        return total

    def groupLengh(self, gr):
        return len(gr.oriTuples) + len(gr.recieveTuples)

    def removeGroupFromList(self, grID, groups):
        item = next((x for x in groups if x.id == grID), None)
        if item is not None:
            groups.remove(item)

    def addGroupToList(self, gr, groups):

        item = next((x for x in groups if x.id == gr.id), None)
        if item is not None:
            groups.remove(item)

        groups.append(gr)

    def migrateTuples(self, grGive, grRecieve, numMigrate):
        grRecieve.recieveTuples.extend(grGive.oriTuples[:numMigrate])
        grGive.oriTuples = grGive.oriTuples[numMigrate:]

    def contructRCareAndCalBudget(self, RInitial, lenTotal):
        RCare = {}
        ruleID = 1
        RInitialCopy = copy.deepcopy(RInitial)
        for rule in RInitialCopy:
            isQuasiLeft = False
            isQuasiRight = False
            budget = .0

            lhs = rule[0]
            rhs = rule[1]
            s = rule[2]
            c = rule[3]
            ruleStr = rule[4]

            for l in lhs:
                split = l.split("|")
                tempIsQuasiLeft = split[0] in QUASI
                if tempIsQuasiLeft:
                    isQuasiLeft = True

            for r in rhs:
                split = r.split("|")
                tempIsQuasiRight = split[0] in QUASI
                if tempIsQuasiRight:
                    isQuasiRight = True

            if isQuasiLeft or isQuasiRight:  # rule that contain at least 1 quasi attr
                if isQuasiRight:
                    budget = min(s - MIN_SUP, s*(c - MIN_CONF) / c)
                else:
                    budget = min(
                        s - MIN_SUP, s*(c - MIN_CONF) / (c*(1 - MIN_CONF)))

                budget = int(budget*lenTotal)

                myRule = Rule(ruleID, lhs, rhs, s, c, budget,
                              ruleStr)
                RCare[ruleID] = myRule
                ruleID += 1

        return RCare

    # def formarNumericRange(self,number, attr):
    #     if attr == 'age':
    #         rangeOfNum = [0, 24.3, 31.6, 38.9, 46.2,53.5, 60.8, 68.1, 75.4, 82.7, 100]
    #         labels = ['<24', '24-31', '31-38', '38-46', '46-53','53-60', '60-68', '68-75', '75-82', '>82']
    #         for i, num in enumerate(rangeOfNum):
    #             if number >= num and number < rangeOfNum[i+1]:
    #                 return labels[i]
    #     elif attr == 'hours-per-week':
    #         rangeOfNum = [0, 10.8, 20.6, 30.4, 40.2,50, 59.8, 69.6, 79.4, 89.2, 100.]
    #         labels = ['<10', '10-20', '20-30', '30-40', '40-50','50-60', '60-70', '70-80', '80-90', '>90']
    #         for i, num in enumerate(rangeOfNum):
    #             if number >= num and number < rangeOfNum[i+1]:
    #                 return labels[i]
    #     elif attr == 'capital-gain':
    #         rangeOfNum = [-1, 9999.9, 19999.8, 29999.7, 39999.6,49999.5, 59999.4, 69999.3, 79999.2, 89999.1, 100000]
    #         labels = ['<10k', '10k-20k', '20k-30k', '30k-40k', '40k-50k','50k-60k', '60k-70k', '70k-80k', '80k-90k', '>90k']
    #         for i, num in enumerate(rangeOfNum):
    #             if number >= num and number < rangeOfNum[i+1]:
    #                 return labels[i]
    #     elif attr == 'capital-loss':
    #         rangeOfNum = [-1, 435.6, 871.2, 1306.8, 1742.4,2178, 2613.6, 3049.2, 3484.8, 3920.4, 4356]
    #         labels = ['<435', '435-871', '871-1306', '1306-1742', '1742-2178','2178-2613', '2613-3049', '3049-3484', '3484-3920', '>3920']
    #         for i, num in enumerate(rangeOfNum):
    #             if number >= num and number < rangeOfNum[i+1]:
    #                 return labels[i]

    def initGroups(self, df, RCare, k):
        grID = 1
        kSafe = []
        kUnsafe = []

        # Create new GroupID column, later disperse move tuples back to oriGroup
        df['oriGrID'] = np.nan
        # df['copy_age'] = df['age']
        # df['copy_hours-per-week'] = df['hours-per-week']
        # df['copy_capital-gain'] = df['capital-gain']

        # if 'age' in df.columns:
        #     df['age'] = pd.cut(df['age'], [-1, 24.3, 31.6, 38.9, 46.2, 53.5, 60.8, 68.1, 75.4, 82.7, 100], labels=[
        #         '<24', '24-31', '31-38', '38-46', '46-53', '53-60', '60-68', '68-75', '75-82', '>82'])

        # if 'hours-per-week' in df.columns:
        #     df['hours-per-week'] = pd.cut(df['hours-per-week'], [-1, 10.8, 20.6, 30.4, 40.2, 50, 59.8, 69.6, 79.4, 89.2, 100.], labels=[
        #         '<10', '10-20', '20-30', '30-40', '40-50', '50-60', '60-70', '70-80', '80-90', '>90'])

        # # capital-gain and capital-loss: each cut into levels None (0), Low (0 < median of the values greater zero < max) and High (>=max)
        # if 'capital-gain' in df.columns:
        #     df['capital-gain'] = pd.cut(df['capital-gain'], [-1, 9999.9, 19999.8, 29999.7, 39999.6, 49999.5, 59999.4, 69999.3, 79999.2, 89999.1, 100000],
        #         labels=['<10k','10k-20k','20k-30k','30k-40k','40k-50k','50k-60k','60k-70k','70k-80k','80k-90k','>90k'])

        # if 'capital-loss' in df.columns:
        #     df['capital-loss'] = pd.cut(df['capital-loss'],
        #         [-1, 435.6,871.2,1306.8, 1742.4, 2178,2613.6,3049.2,3484.8, 3920.4, 4356],
        #         labels=['<435','435-871','871-1306','1306-1742','1742-2178','2178-2613','2613-3049','3049-3484','3484-3920','>3920'])

        groupsByQuasi = df.groupby(QUASI)
        for v, g in groupsByQuasi:
            dataRows = []
            for row in g.iterrows():
                i, data = row
                data.oriGrID = grID
                dataRows.append(data)

            quasiValues = set()
            for attr in QUASI:
                quasiValues.add('{}|{}'.format(attr, data.get(attr)))

            rulesRelated = []
            for ruleID, r in RCare.items():# rule that this group have related 
                if r.lhs.intersection(quasiValues) or r.rhs.intersection(quasiValues):
                    rulesRelated.append(ruleID)

            myGroup = Group(grID, quasiValues, dataRows, rulesRelated)

            if len(dataRows) >= k:
                kSafe.append(myGroup)
            else:
                kUnsafe.append(myGroup)

            grID += 1

        return kSafe, kUnsafe

    def ableToMigrate(self, grGive, grRecieve, k):
        # only apply for unsafe groups

        # giving group cant have recieve tuples before
        if self.groupLengh(grGive) < k and len(grGive.recieveTuples) > 0:
            return False
        # recieving group cant giving tuples before
        if self.groupLengh(grRecieve) < k and len(grRecieve.oriTuples) < grRecieve.oriLen:
            return False

        return True

    # num of tuples need to become K-anonymity, or all tuples can give/recieve
    def getNumOfTuples(self, grGive, grRecieve, k):
        # both unsafe
        if self.groupLengh(grGive) < k and self.groupLengh(grRecieve) < k:
            return min(self.groupLengh(grGive),
                       k - self.groupLengh(grRecieve))
        elif self.groupLengh(grGive) >= k:  # grGive safe, grRecieve unsafe
            return min(k-self.groupLengh(grRecieve),
                       self.groupLengh(grGive)-k,
                       len(grGive.oriTuples))
        else:  # grGive unsafe
            return self.groupLengh(grGive)

    def findRulesEffect(self, grGive, grRecieve, RCARE):
        REffect = []
        aNotb = grRecieve.quasiVals - grGive.quasiVals
        for ruleID in grGive.rulesRelated:
            r = RCARE[ruleID]
            if r.lhs.intersection(aNotb) or r.rhs.intersection(aNotb):
                REffect.append(ruleID)

        return REffect

    def riskReduction(self, grGive, grRecieve, numOfTuples, k):
        grGiveRiskBf = 0 if self.groupLengh(grGive) >= k \
            else 2*k - self.groupLengh(grGive)
        grRecieveRiskBf = 0 if self.groupLengh(grRecieve) >= k \
            else 2*k - self.groupLengh(grRecieve)

        grGiveLenAf = self.groupLengh(grGive) - numOfTuples
        grGiveRiskAf = 0 if grGiveLenAf >= k or grGiveLenAf == 0 \
            else 2 * k - grGiveLenAf

        grRecieveLenAfter = self.groupLengh(grRecieve) + numOfTuples
        grRecieveRiskAfter = 0 if grRecieveLenAfter >= k or grRecieveLenAfter == 0 \
            else 2 * k - grRecieveLenAfter

        return (grGiveRiskBf+grRecieveRiskBf) - (grGiveRiskAf+grRecieveRiskAfter)

    def findBestMMGroup(self, selGroup, remainingGroups, RCare, k):
        considerGroups = []
        # if self.groupLengh(selGroup) == 0:
        #     return None

        for g in remainingGroups:
            if self.ableToMigrate(selGroup, g, k):  # case: selGroup->g
                numOfTuples = self.getNumOfTuples(selGroup, g, k)
                if numOfTuples > 0:
                    REffectIDs = self.findRulesEffect(selGroup, g, RCare)
                    cost = len(REffectIDs)

                    if len(REffectIDs) > 0:
                        REffect = [RCare[x] for x in REffectIDs]
                        minBudget = (
                            min(REffect, key=lambda x: x.budget)).budget
                        numOfTuples = min(numOfTuples, minBudget-1)

                    if numOfTuples > 0:
                        riskReduce = self.riskReduction(
                            selGroup, g, numOfTuples, k)
                        considerGroups.append(ConsiderMMGroup(riskReduce, numOfTuples, cost, True,
                                                              g.id, self.groupLengh(g), REffectIDs))

            if self.ableToMigrate(g, selGroup, k):  # Case: g->selGroup
                numOfTuples = self.getNumOfTuples(g, selGroup, k)
                if numOfTuples > 0:
                    REffectIDs = self.findRulesEffect(g, selGroup, RCare)
                    cost = len(REffectIDs)
                    if len(REffectIDs) > 0:
                        REffect = [RCare[x] for x in REffectIDs]
                        minBudget = (
                            min(REffect, key=lambda x: x.budget)).budget
                        numOfTuples = min(numOfTuples, minBudget-1)
                    if numOfTuples > 0:
                        riskReduce = self.riskReduction(
                            g, selGroup, numOfTuples, k)
                        considerGroups.append(ConsiderMMGroup(riskReduce, numOfTuples, cost, False,
                                                              g.id, self.groupLengh(g), REffectIDs))
        if len(considerGroups) == 0:
            return None

        # min cost, max reduce, min numMrt
        considerGroups.sort(key=lambda x: (-x.riskReduce,
                                           x.numMigrate))
        return considerGroups[0]

    def disperse(self, g, RCare, ALL_GROUPS, UM, SG):
        while len(g.recieveTuples) > 0:  # return recieveTuples to their origin group
            # find origroup of this data
            currentTuple = g.recieveTuples[0]
            oriGroup = next(
                (x for x in ALL_GROUPS if x.id == currentTuple.oriGrID), None)
            if oriGroup is None:
                raise Exception('cant find origin group')
            # construct_r_affected_by_a_migration
            REffectIDs = self.findRulesEffect(oriGroup, g, RCare)
            # tuple disperse
            g.recieveTuples.remove(currentTuple)
            oriGroup.oriTuples.append(currentTuple)
            for ruleID in REffectIDs:  # REffect buget +1 for each tuple return
                RCare[ruleID].budget += 1

            if self.groupLengh(oriGroup) == 1:
                self.addGroupToList(oriGroup, UM)

        while len(g.oriTuples) > 0:
            currentTuple = g.oriTuples[0]
            # find best group in SG to move => min REffect
            minLenREffect = 99999999999999
            currentChooseGroup = None
            currentChooseREffect = None
            for sg in SG:
                REffectIDs = self.findRulesEffect(g, sg, RCare)
                if len(REffectIDs) < minLenREffect:
                    minLenREffect = len(REffectIDs)
                    currentChooseGroup = sg
                    currentChooseREffect = REffectIDs

            # tuple disperse
            g.oriTuples.remove(currentTuple)
            currentChooseGroup.recieveTuples.append(currentTuple)

            # REffect buget -1 for each tuple return
            for ruleID in currentChooseREffect:
                RCare[ruleID].budget -= 1  # R_effect budget -1

    def export(self, groups, exportDataPath):
        arr = []
        for g in groups:
            quasiDict = {}  # get quasi value
            for a in g.quasiVals:
                split = a.split("|", 1)
                quasiDict[split[0]] = split[1]

            if len(g.oriTuples) > 0:
                for tuple in g.oriTuples:
                    for attr in QUASI:  # set group quasi value
                        setattr(tuple, attr, quasiDict[attr])
                    arr.append(tuple)

            if len(g.recieveTuples) > 0:
                for tuple in g.recieveTuples:
                    for attr in QUASI:  # set group quasi value
                        setattr(tuple, attr, quasiDict[attr])
                    arr.append(tuple)

        rs = pd.DataFrame(arr)
        # remove added column to origin columns
        rs = rs.drop(columns=['oriGrID'])

        os.makedirs(os.path.dirname(exportDataPath), exist_ok=True)
        rs.to_csv(exportDataPath, index=False, header=False)

    def Anonymize(self, df, RInitial, k, algo):
        startAt = time.time()
        lenTotal = df.shape[0]
        print("Start M3ar", 'lenTotal', lenTotal)

        UM = []

        # region INIT_STAGE
        RCare = self.contructRCareAndCalBudget(RInitial, lenTotal)
        print("RInitial:", len(RInitial), "RCare:", len(RCare))

        SG, UG = self.initGroups(df, RCare, k)
        ALL_GROUPS = SG+UG

        lenOfGroupsBefore = {'SG': len(SG), 'SGTotal': self.groupsTotalTuples(SG),
                             'UG': len(UG), 'UGTotal': self.groupsTotalTuples(UG),
                             'UM': len(UM), 'UMTotal': self.groupsTotalTuples(UM), }
        print("Begin with ", lenOfGroupsBefore)
        # endregion

        # region PROCESS_STAGE
        selGr = None
        loopCount = 0

        while (len(UG) > 0) or selGr != None:
            # print('-------------loopCount',loopCount)
            # print('SG',len(SG),'UGs',len(UG))
            loopCount += 1
            # Randomly pick a group SelG from unsafe groups set
            if selGr is None:
                selGr = random.choice(UG)
                self.removeGroupFromList(selGr.id, UG)

            remainingGroups = UG + SG

            rsFindBestGr = self.findBestMMGroup(selGr, remainingGroups, RCare, k)

            if rsFindBestGr is None:
                if self.groupLengh(selGr)>0:
                    UM.append(selGr)
                selGr = None
            else:
                bestMMGr = next((x for x in remainingGroups if x.id == rsFindBestGr.bestGroupID), None)
                if bestMMGr is None:
                    raise Exception('Error: cant find best group in list')

                if rsFindBestGr.migrateDirect == True:  # selG->G
                    self.migrateTuples(selGr, bestMMGr, rsFindBestGr.numMigrate)
                    for ruleID in rsFindBestGr.R_affected:
                        # budget -1 for each tuple migrate
                        RCare[ruleID].budget -= rsFindBestGr.numMigrate
                    if self.groupLengh(bestMMGr) >= k:
                        self.addGroupToList(bestMMGr, SG)
                        self.removeGroupFromList(bestMMGr.id, UG)
                    if self.groupLengh(selGr) == 0:
                        selGr = None
                        continue
                else:  # selG<-G
                    self.migrateTuples(bestMMGr, selGr, rsFindBestGr.numMigrate)
                    for ruleID in rsFindBestGr.R_affected:
                        # budget -1 for each tuple migrate
                        RCare[ruleID].budget -= rsFindBestGr.numMigrate
                    if self.groupLengh(selGr) >= k:
                        self.addGroupToList(selGr, SG)
                        # self.removeGroupFromList(selGr.id, UG)
                    if self.groupLengh(bestMMGr) == 0:
                        self.removeGroupFromList(bestMMGr.id, UG)

                if self.groupLengh(selGr) < k:  # Continue with current group
                    continue
                # Continue with g
                elif  self.groupLengh(bestMMGr) > 0 and self.groupLengh(bestMMGr) < k:
                    selGr = bestMMGr
                    self.removeGroupFromList(bestMMGr.id, UG)
                else:  # choose another group in UGs to process
                    selGr = None

        lenOfGroupsUM = {'SG': len(SG), 'SGTotal': self.groupsTotalTuples(SG),
                         'UG': len(UG), 'UGTotal': self.groupsTotalTuples(UG),
                         'UM': len(UM), 'UMTotal': self.groupsTotalTuples(UM), }
        print("After migrate ", lenOfGroupsUM)
        # endregion

        # region DISPERSE_STAGE
        while len(UM) > 0:
            g = UM.pop(0)
            if self.groupLengh(g) > 0:
                self.disperse(g, RCare, ALL_GROUPS, UM, SG)
        # endregion

        lenOfGroupsAfter = {'SG': len(SG), 'SGTotal': self.groupsTotalTuples(SG),
                            'UG': len(UG), 'UGTotal': self.groupsTotalTuples(UG),
                            'UM': len(UM), 'UMTotal': self.groupsTotalTuples(UM), }
        print("After disperse ", lenOfGroupsAfter)
        totalTime = round((time.time()-startAt), 2)
        print("Done, Total time: {}s".format(totalTime))

        # export dataset
        self.export(SG, KANONYMITY_DATA_PATH.format(algo, k))

        return {
            'groups': SG,
            'totalTime': totalTime,
            'lenOfGroupsBefore': lenOfGroupsBefore,
            'lenOfGroupsUM': lenOfGroupsUM,
            'lenOfGroupsAfter': lenOfGroupsAfter,
        }
