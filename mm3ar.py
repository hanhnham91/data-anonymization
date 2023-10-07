import pandas as pd
import time
import numpy as np
from myconfig import *
import os
import copy

class ConsiderMMGroup:

    def __init__(self, riskReduce, numMigrate, migrateDirect, bestGroupID, GroupLengh, R_affected, R_affected_low, supChange=0, confChange=0):
        self.riskReduce = riskReduce
        self.numMigrate = numMigrate
        self.migrateDirect = migrateDirect
        self.bestGroupID = bestGroupID
        self.GroupLengh = GroupLengh
        self.R_affected = R_affected
        self.R_affected_low = R_affected_low
        # new added
        self.supChange = supChange
        self.confChange = confChange

    def __str__(self):
        return "riskReduce={},supChange={},confChange={},numMigrate={},migrateDirect={},bestGroupID={},GroupLengh={}".format(self.riskReduce,
                                                                                                                             self.supChange,
                                                                                                                             self.confChange,
                                                                                                                             self.numMigrate,                                                                                                                                                                                    self.numMigrate,
                                                                                                                             self.migrateDirect,
                                                                                                                             self.bestGroupID,
                                                                                                                             self.GroupLengh
                                                                                                                             )


class Group:
    id: int
    quasiVals: set
    oriLen: int
    oriTuples: list
    recieveTuples: list
    rulesRelated: list
    rulesRelatedLow: list

    def __init__(self, id, quasiVals, dataRows, rulesRelated, rulesRelatedLow):
        self.id = id
        self.quasiVals = quasiVals
        self.oriLen = len(dataRows)
        self.oriTuples = dataRows
        self.recieveTuples = []
        self.rulesRelated = rulesRelated
        self.rulesRelatedLow = rulesRelatedLow

    def __str__(self):
        return f"id:{self.id}, quasiVals: {self.quasiVals},lenOri= {self.oriLen},lenRep= {len(self.recieveTuples)},len(rulesRelated)= {len(self.rulesRelated)}, len(rulesRelatedLow)={len(self.rulesRelatedLow)}"


class Rule:
    id: int
    lhs: set
    rhs: set
    sup: float
    conf: float
    budget: int
    ruleStr: str

    def __init__(self, id, lhs, rhs, sup, conf, budget, ruleStr, countAandB, countA, lenTotal):
        self.id = id
        self.lhs = lhs
        self.rhs = rhs
        self.sup = sup
        self.conf = conf
        self.budget = budget
        self.ruleStr = ruleStr
        self.countAandB = countAandB
        self.countA = countA
        self.lenTotal = lenTotal

    def __str__(self):
        return f"{self.lhs} -> {self.rhs},sup= {self.sup},conf= {self.conf},budget= {self.budget}"


class MM3arAlgorithm:
    """The main algorithm of this project"""

    R_CARE = []
    R_CARE_LOW = []
    SG = []
    UG = []
    UM = []
    ALL_GROUPS = []
    K_ANONYMITY = 999999

    def groupsTotalTuples(self, groups):
        total = 0
        for g in groups:
            total += self.groupLengh(g)
        return total

    def groupLengh(self, gr):
        """Return total lenght of group originTuples + receiveTuples

        Parameters:
            gr (Group): a group"""

        return len(gr.oriTuples) + len(gr.recieveTuples)

    def removeGroupFromList(self, grID, groups):
        item = next((x for x in groups if x.id == grID), None)
        if item is not None:
            groups.remove(item)

    def addGroupToList(self, gr, groups):
        item = next((x for x in groups if x.id == gr.id), None)
        if item is None:
            groups.append(gr)

    def migrateTuples(self, grGive, grRecieve, numMigrate):
        """Transfer n tuples from a group to another group

        Parameters:
            grGive (Group): a group
            grRecieve (Group): a group
            numMigrate (int): number of tuples to transfer"""
        grRecieve.recieveTuples.extend(grGive.oriTuples[:numMigrate])
        grGive.oriTuples = grGive.oriTuples[numMigrate:]

    def findRCareAndCalBudget(self, RInitial, lenTotal):
        """Return list rule that have at least 1 quasi attribute 

        Parameters:
            RInitial (list): association rules mine by apriori algorithms with the min_sup, min_conf
            lenTotal (int): total lenght of dataset"""
        RCare = {}
        ruleID = 1
        RInitialTemp = copy.deepcopy(RInitial)
        for rule in RInitialTemp:
            isQuasiLeft = False
            isQuasiRight = False
            budget = .0

            lhs = rule[0]
            rhs = rule[1]
            s = rule[2]
            c = rule[3]
            ruleStr = rule[4]
            countAandB = rule[5]
            countA = rule[6]

            for l in lhs:
                split = l.split("|")
                if split[0] in QUASI:
                    isQuasiLeft = True

            for r in rhs:
                split = r.split("|")
                if split[0] in QUASI:
                    isQuasiRight =True

            if isQuasiLeft or isQuasiRight:  # rule that contain at least 1 quasi attr
                if isQuasiRight:
                    budget = min(s - MIN_SUP, s*(c - MIN_CONF) / c)
                else:
                    budget = min(s - MIN_SUP, s*(c - MIN_CONF) /
                                 (c*(1 - MIN_CONF)))

                budget = int(budget*lenTotal)

                myRule = Rule(ruleID, lhs, rhs, s, c, budget,
                              ruleStr, countAandB, countA, lenTotal)
                RCare[ruleID] = myRule
                ruleID += 1

        return RCare

    def findRCareAndCalBudgetLow(self, RInitialLow, lenTotal):
        """Return list rule that have quasi attribute on both side of rule(A->B)

        Parameters:
            RInitialLow (list): association rules mine by apriori algorithms with the support between (min_sup-z, min_sup) or (min_conf-z, min_conf)
            lenTotal (int): total lenght of dataset"""
        RCareLow = {}
        ruleID = 1
        RInitialLowTemp = copy.deepcopy(RInitialLow)
        for rule in RInitialLowTemp:
            isQuasiLeft = False
            isQuasiRight = False
            budget = .0

            lhs = rule[0]
            rhs = rule[1]
            s = rule[2]
            c = rule[3]
            ruleStr = rule[4]

            countAandB = rule[5]
            countA = rule[6]

            for l in lhs:
                split = l.split("|")
                if split[0] in QUASI:
                    isQuasiLeft =True

            for r in rhs:
                split = r.split("|")
                if split[0] in QUASI:
                    isQuasiRight =True

            if isQuasiLeft and isQuasiRight:  # rule that contain quasi attr in both sides

                if s < MIN_SUP and c < MIN_CONF:
                    budget = min(MIN_SUP-s, s*(MIN_CONF-c) / (c*(1-MIN_CONF)))
                elif c >= MIN_CONF:  # s below thresholds
                    budget = MIN_SUP-s
                else:  # c below thresholds
                    budget = s*(MIN_CONF-c) / (c*(1-MIN_CONF))

                budget = int(budget*lenTotal)

                myRule = Rule(ruleID,  lhs, rhs, s, c, budget,
                              ruleStr, countAandB, countA, lenTotal)
                RCareLow[ruleID] = myRule
                # print('rule', myRule)
                ruleID += 1

        return RCareLow

    def initGroups(self, df, RCare, RCareLow):
        """Inital group, group dataset into list groups by quasi attributes, and spilt into k-safe SG, and k-unsafe UG, a group hold list ruleRelated which may change the budget when Mirgate tuples

        Parameters:
            df (dataframe): association rules mine by apriori algorithms with the support between (min_sup-z, min_sup) or (min_conf-z, min_conf)
            RCare (list): list of RCare
            RCareLow (list): list of RCareLow"""
        groupID = 1
        kSafe = []
        kUnsafe = []

        # Create new GroupID column, later disperse move tuples back to oriGroup
        df['oriGroupID'] = np.nan

        groupsByQuasi = df.groupby(QUASI)
        for v, g in groupsByQuasi:
            dataRows = []
            for r in g.iterrows():
                i, data = r
                data.oriGroupID = groupID
                dataRows.append(data)

            quasiValues = set()
            for attr in QUASI:
                quasiValues.add('{}|{}'.format(attr, data.get(attr)))

            # List Rules of current group may loss sig_rule if give tuples
            rulesRelated = []
            for ruleID, r in RCare.items():
                if r.lhs.intersection(quasiValues) or r.rhs.intersection(quasiValues):
                    rulesRelated.append(ruleID)

            # List Rules of current group may become sig_rule if recieve tuples
            rulesRelatedLow = []
            for ruleID, r in RCareLow.items():
                if r.lhs.intersection(quasiValues) and r.rhs.intersection(quasiValues):
                    rulesRelatedLow.append(ruleID)

            myGroup = Group(groupID, quasiValues, dataRows,
                            rulesRelated, rulesRelatedLow)

            if len(dataRows) >= K_ANONYMITY:
                kSafe.append(myGroup)
            else:
                kUnsafe.append(myGroup)

            groupID += 1

        return kSafe, kUnsafe

    def ableToMigrate(self, grGive, grRecieve):
        """Check the condition: 
            only apply for unsafe groups, 
            grGive can not have received tuples before, 
            grRecieve can not giving tuples before

        Parameters:
            grGive (Group): a group that giving tuples 
            grRecieve (Group): a group that recieving tuples"""
        if self.groupLengh(grGive) < K_ANONYMITY and len(grGive.recieveTuples) > 0:
            return False
        if self.groupLengh(grRecieve) < K_ANONYMITY and len(grRecieve.oriTuples) < grRecieve.oriLen:
            return False

        return True

    def getNumOfTuples(self, grGive, grRecieve):
        """Return number of tuples need to become K-anonymity, or max tuples able to give/recieve

        Parameters:
            grGive (Group): a group that giving tuples 
            grRecieve (Group): a group that recieving tuples"""
        if self.groupLengh(grGive) < K_ANONYMITY and self.groupLengh(grRecieve) < K_ANONYMITY:  # both unsafe
            return min(self.groupLengh(grGive),
                       K_ANONYMITY - self.groupLengh(grRecieve))
        elif self.groupLengh(grGive) >= K_ANONYMITY:  # grGive safe, grRecieve unsafe
            return min(K_ANONYMITY-self.groupLengh(grRecieve),
                       self.groupLengh(grGive)-K_ANONYMITY,
                       len(grGive.oriTuples))
        else:  # grGive unsafe
            return self.groupLengh(grGive)

    def findRulesEffect(self, grGive, grRecieve):
        # grGive(a,b,c)-> grRecieve(x,y,c)
        # attr(a,b) reduce numOfTuples
        # attr(c,x,y) same or increase numOfTuples
        # => only care attr(a,b)
        REffect = []
        aNotb = grRecieve.quasiVals - grGive.quasiVals
        for ruleID in grGive.rulesRelated:
            r = R_CARE[ruleID]
            # which rules may loss sig rules: effect when any sides of rule change
            if r.lhs.intersection(aNotb) or r.rhs.intersection(aNotb):
                REffect.append(ruleID)

        return REffect

    def findRulesEffectLow(self, grGive, grRecieve):
        # opposite of FindRulesEffect
        REffectLow = []
        bNota = grGive.quasiVals - grRecieve.quasiVals
        for ruleID in grRecieve.rulesRelatedLow:
            # print('rulesRelatedLow', ruleID)
            r = R_CARE_LOW[ruleID]
            # which rules may become sig rules: only have effect when both sides of rule change
            if r.lhs.intersection(bNota) and r.rhs.intersection(bNota):
                REffectLow.append(ruleID)

        return REffectLow

    def riskReduction(self, grGive, grRecieve, numOfTuples):
        grGiveRiskBf = 0 if self.groupLengh(grGive) >= K_ANONYMITY \
            else 2*K_ANONYMITY - self.groupLengh(grGive)
        grRecieveRiskBf = 0 if self.groupLengh(grRecieve) >= K_ANONYMITY \
            else 2*K_ANONYMITY - self.groupLengh(grRecieve)

        grGiveLenAf = self.groupLengh(grGive) - numOfTuples
        grGiveRiskAf = 0 if grGiveLenAf >= K_ANONYMITY or grGiveLenAf == 0 \
            else 2 * K_ANONYMITY - grGiveLenAf

        grRecieveLenAfter = self.groupLengh(grRecieve) + numOfTuples
        grRecieveRiskAfter = 0 if grRecieveLenAfter >= K_ANONYMITY or grRecieveLenAfter == 0 \
            else 2 * K_ANONYMITY - grRecieveLenAfter

        return (grGiveRiskBf+grRecieveRiskBf) - (grGiveRiskAf+grRecieveRiskAfter)

    def dangerDegree(self, grGive, grRecieve, numOfTuples, REffect):
        """calculate and return the total change of support and confident when having migration

        Parameters:
            grGive (Group): a group that giving tuples 
            grRecieve (Group): a group that recieving tuples
            numOfTuples (int): number of tuples to transfer
            REffect (list): list rule affect the tuples migration"""
        aNotb = grRecieve.quasiVals - grGive.quasiVals

        totalSupChange = 0
        totalConfChange = 0
        for r in REffect:
            if r.lhs.intersection(aNotb) and r.rhs.intersection(aNotb):  # effect both side
                newcountAandB = r.countAandB - numOfTuples
                newcountA = r.countA - numOfTuples

                newSup = newcountAandB / r.lenTotal
                newConf = newcountAandB / newcountA

            elif r.lhs.intersection(aNotb):  # migrate effect left hand of rule
                # countAandB notchange
                # countA change
                newcountAandB = r.countAandB
                newcountA = r.countA - numOfTuples
                newSup = newcountAandB / r.lenTotal
                newConf = newcountAandB / r.countA

            else:  # migrate only effect right hand of rule
                # sup and conf no change
                newSup = r.sup
                newConf = r.conf

            changeOfSup = r.sup - newSup
            changeOfConf = r.conf - newConf
            if changeOfConf < 0:
                changeOfConf = 0
            totalSupChange += changeOfSup
            totalConfChange += changeOfConf
            # print('---------------')

        return {'totalSupChange': totalSupChange, 'totalConfChange': totalConfChange}

    def findBestMMGroup(self, selGroup, remainingGroups):
        """Find a best group in list remainingGroups, considers two way migration

        Parameters:
            selGroup (Group): a group 
            remainingGroups (list): list remainingGroup

        Return:
            groupID (int): groupID
            numMigrate (int): number of tuples migrate between selG and best group
            migrateDirect (bool): direction of mirgate
            REffect (list): list rule affected when migrate
            REffectLow (list):  list rule affected when migrate"""
        considerGroups = []
        if self.groupLengh(selGroup) == 0:
            return None

        for g in remainingGroups:
            if self.ableToMigrate(selGroup, g):  # case: selGroup->g
                numOfTuples = self.getNumOfTuples(selGroup, g)
                if numOfTuples > 0:
                    REffectID = self.findRulesEffect(selGroup, g)
                    REffectLowID = self.findRulesEffectLow(selGroup, g)

                    # migrate without impact any rules
                    if len(REffectID) == 0 and len(REffectLowID) == 0:
                        riskReduce = self.riskReduction(
                            selGroup, g, numOfTuples)
                        considerGroups.append(ConsiderMMGroup(riskReduce, numOfTuples, True,
                                                              g.id, self.groupLengh(g), REffectID, REffectLowID))
                    else:
                        REffect = [R_CARE[x] for x in REffectID]
                        REffectLow = [R_CARE_LOW[x] for x in REffectLowID]
                        if len(REffectID) == 0:
                            minBudgetLow = (
                                min(REffectLow, key=lambda x: x.budget)).budget
                            numMigrate = min(numOfTuples,  minBudgetLow)
                        elif len(REffectLowID) == 0:
                            minBudget = (
                                min(REffect, key=lambda x: x.budget)).budget
                            numMigrate = min(numOfTuples,  minBudget)
                        else:
                            minBudget = (
                                min(REffect, key=lambda x: x.budget)).budget
                            minBudgetLow = (
                                min(REffectLow, key=lambda x: x.budget)).budget
                            numMigrate = min(
                                numOfTuples, minBudget, minBudgetLow)

                        # Find numOfTuple with less effect to rules(change of support and confident)
                        for n in range(1, numMigrate+1):
                            riskReduce = self.riskReduction(selGroup, g, n)
                            dangerDegree = self.dangerDegree(
                                selGroup, g, n, REffect)

                            considerGroups.append(ConsiderMMGroup(riskReduce, n, True,
                                                                  g.id, self.groupLengh(g), REffectID, REffectLowID, dangerDegree['totalSupChange'], dangerDegree['totalConfChange']))

            if self.ableToMigrate(g, selGroup):  # Case: g->selGroup
                numOfTuples = self.getNumOfTuples(g, selGroup)
                if numOfTuples > 0:
                    REffectID = self.findRulesEffect(g, selGroup)
                    REffectLowID = self.findRulesEffectLow(g, selGroup)
                    # migrate without impact any rule
                    if len(REffectID) == 0 and len(REffectLowID) == 0:
                        riskReduce = self.riskReduction(
                            g, selGroup, numOfTuples)
                        considerGroups.append(ConsiderMMGroup(riskReduce, numOfTuples, False,
                                                              g.id, self.groupLengh(g), REffectID, REffectLowID))
                    else:
                        REffect = [R_CARE[x] for x in REffectID]
                        REffectLow = [R_CARE_LOW[x] for x in REffectLowID]
                        if len(REffectID) == 0:
                            minBudgetLow = (
                                min(REffectLow, key=lambda x: x.budget)).budget
                            numMigrate = min(numOfTuples,  minBudgetLow)
                        elif len(REffectLowID) == 0:
                            minBudget = (
                                min(REffect, key=lambda x: x.budget)).budget
                            numMigrate = min(numOfTuples,  minBudget)
                        else:
                            minBudget = (
                                min(REffect, key=lambda x: x.budget)).budget
                            minBudgetLow = (
                                min(REffectLow, key=lambda x: x.budget)).budget
                            numMigrate = min(
                                numOfTuples, minBudget, minBudgetLow)

                        # Find numOfTuple with less effect to rules(change of support and confident)
                        for n in range(1, numMigrate+1):
                            riskReduce = self.riskReduction(selGroup, g, n)
                            dangerDegree = self.dangerDegree(
                                selGroup, g, n, REffect)

                            considerGroups.append(ConsiderMMGroup(riskReduce, n, False,
                                                                  g.id, self.groupLengh(g), REffectID, REffectLowID, dangerDegree['totalSupChange'], dangerDegree['totalConfChange']))

        if len(considerGroups) == 0:
            return None

        # max reduce , min supChange ,min confChange ,min numMrt
        considerGroups.sort(key=lambda x: (-x.riskReduce,
                                           x.supChange,
                                           x.confChange,
                                           x.numMigrate))
        # if rs[0].supChange>0 or rs[0].confChange>0 :
        #     print(rs[0])
        #     print(rs[1])
        #     print(rs[2])

        # print('====================================')
        return considerGroups[0]

    def disperse(self, g):
        while len(g.recieveTuples) > 0:  # return recieveTuples to their origin group
            # find origroup of this data
            currentTuple = g.recieveTuples[0]
            oriGroup = next(
                (x for x in ALL_GROUPS if x.id == currentTuple.oriGroupID), None)
            if oriGroup is None:
                raise Exception('cant find origin group')
            # construct_r_affected_by_a_migration
            REffectID = self.findRulesEffect(oriGroup, g)
            # REffectLowID = FindRulesEffectLow(oriGroup, g)
            # tuple disperse
            g.recieveTuples.remove(currentTuple)
            oriGroup.oriTuples.append(currentTuple)
            for ruleID in REffectID:  # REffect buget +1 for each tuple return
                R_CARE[ruleID].budget += 1
            # for ruleID in REffectLowID:  # REffect buget +1 for each tuple return
            #     R_CARE_LOW[ruleID].budget += 1

            if self.groupLengh(oriGroup) == 1:
                self.addGroupToList(oriGroup, UM)

        while len(g.oriTuples) > 0:
            currentTuple = g.oriTuples[0]
            # find best group in SG to move => min REffect
            minLenREffect = 99999999999999
            selectedGroup = None
            selectedREffectIDs = None
            for sg in SG:
                REffectIDs = self.findRulesEffect(g, sg)
                if len(REffectIDs) < minLenREffect:
                    minLenREffect = len(REffectIDs)
                    selectedGroup = sg
                    selectedREffectIDs = REffectIDs

            # tuple disperse
            g.oriTuples.remove(currentTuple)
            selectedGroup.recieveTuples.append(currentTuple)

            # REffect buget -1 for each tuple return
            for ruleID in selectedREffectIDs:
                R_CARE[ruleID].budget -= 1  # R_effect budget -1

    def export(self, groups, exportDataPath):
        """Export result of anonimity to dataset(csv,txt), and convert quasi values of foreign tuples to origin tuples

        Parameters:
        groups (list): all groups after anonymity 
        exportDataPath (str): destination of export path """
        arr = []
        for g in groups:
            arr.extend(g.oriTuples)  # add all ori data

            if len(g.recieveTuples) > 0:
                quasiDict = {}  # get quasi value
                for a in g.quasiVals:
                    split = a.split("|", 1)
                    quasiDict[split[0]] = split[1]

                for tuple in g.recieveTuples:
                    for attr in QUASI:  # quasi value migration
                        setattr(tuple, attr, quasiDict[attr])
                    arr.append(tuple)

        rs = pd.DataFrame(arr)
        # remove added column to origin columns
        rs = rs.drop(columns=['oriGroupID'])

        os.makedirs(os.path.dirname(exportDataPath), exist_ok=True)
        rs.to_csv(exportDataPath, index=False, header=False)

    def Anonymize(self, df, RInitial, RInitalLow, k, algo):
        startAt = time.time()
        lenTotal = df.shape[0]
        print("Start MM3ar", 'lenTotal', lenTotal)

        global R_CARE, R_CARE_LOW, SG, UG, UM, ALL_GROUPS, K_ANONYMITY
        K_ANONYMITY = k
        UM = []

        # region INIT_STAGE
        R_CARE = self.findRCareAndCalBudget(RInitial, lenTotal)
        R_CARE_LOW = self.findRCareAndCalBudgetLow(RInitalLow, lenTotal)

        print("RInitial:", len(RInitial), "RCare:", len(R_CARE))
        print("RInitialLow:", len(RInitalLow), "RCareLow:", len(R_CARE_LOW))
        # print(RCareLow[0])
        # print(RCareLow[1])
        # print(RCareLow[2])
        # raise

        SG, UG = self.initGroups(df, R_CARE, R_CARE_LOW)
        ALL_GROUPS = SG+UG

        lenOfGroupsBefore = {'SG': len(SG), 'SGTotal': self.groupsTotalTuples(SG),
                             'UG': len(UG), 'UGTotal': self.groupsTotalTuples(UG),
                             'UM': len(UM), 'UMTotal': self.groupsTotalTuples(UM), }
        print("Begin with ", lenOfGroupsBefore)
        # endregion

        # region PROCESS_STAGE
        # begin from UGs where len near K_ANONYMITY
        UG.sort(key=lambda g: -self.groupLengh(g))
        selGr = None
        while (len(UG) > 0) or selGr != None:
            if selGr is None:
                selGr = UG.pop(0)

            remainingGroups = UG + SG

            rsFindBestGr = self.findBestMMGroup(selGr, remainingGroups)

            if rsFindBestGr is None:
                UM.append(selGr)
                selGr = None
            else:
                bestMMGr = next(
                    (x for x in remainingGroups if x.id == rsFindBestGr.bestGroupID), None)
                if bestMMGr is None:
                    raise Exception('Error: cant find best group in list')

                if rsFindBestGr.migrateDirect == True:  # selG->G
                    self.migrateTuples(selGr, bestMMGr,
                                       rsFindBestGr.numMigrate)
                    for ruleID in rsFindBestGr.R_affected:
                        # budget -1 for each tuple migrate
                        R_CARE[ruleID].budget -= rsFindBestGr.numMigrate*1
                    for ruleID in rsFindBestGr.R_affected_low:
                        # budget -1 for each tuple migrate
                        R_CARE_LOW[ruleID].budget -= rsFindBestGr.numMigrate*1
                    if self.groupLengh(bestMMGr) >= K_ANONYMITY:
                        self.addGroupToList(bestMMGr, SG)
                        self.removeGroupFromList(bestMMGr.id, UG)
                    if self.groupLengh(selGr) == 0:
                        selGr = None
                        continue
                else:  # selG<-G
                    self.migrateTuples(bestMMGr, selGr,
                                       rsFindBestGr.numMigrate)
                    for ruleID in rsFindBestGr.R_affected:
                        # budget -1 for each tuple migrate
                        R_CARE[ruleID].budget -= rsFindBestGr.numMigrate*1
                    for ruleID in rsFindBestGr.R_affected_low:
                        # budget -1 for each tuple migrate
                        R_CARE_LOW[ruleID].budget -= rsFindBestGr.numMigrate*1
                    if self.groupLengh(selGr) >= K_ANONYMITY:
                        self.addGroupToList(selGr, SG)
                        self.removeGroupFromList(selGr.id, UG)
                    if self.groupLengh(bestMMGr) == 0:
                        self.removeGroupFromList(bestMMGr.id, UG)

                if self.groupLengh(selGr) < K_ANONYMITY:  # Continue with current group
                    continue
                elif self.groupLengh(bestMMGr) < K_ANONYMITY and self.groupLengh(bestMMGr) > 0:
                    selGr = bestMMGr
                    self.removeGroupFromList(bestMMGr.id, UG)
                else:  # choose another group in UG to process
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
                self.disperse(g)
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
            'groups': SG,  # SG
            'totalTime': totalTime,
            'lenOfGroupsBefore': lenOfGroupsBefore,
            'lenOfGroupsUM': lenOfGroupsUM,
            'lenOfGroupsAfter': lenOfGroupsAfter,
        }
