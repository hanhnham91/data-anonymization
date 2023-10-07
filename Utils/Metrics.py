

def GroupLengh(g):
    return (len(g.oriTuples) + len(g.recieveTuples))


def LRP(oldRules: list, newRules: list):  # Lost rules percentage(low is good)
    # find numOfRulesLost
    oldRules = [x[4] for x in oldRules]
    newRules = [x[4] for x in newRules]

    oldDffRules = set(oldRules) - set(newRules)

    rs = len(oldDffRules) / len(oldRules)  # (R - R')/R
    return round(rs, 2)


def CAVG(groups: list, k: int):  # Average Group Size(high is good)
    totalLengthOfGroups = 0
    numOfGroups = len(groups)
    for g in groups:
        totalLengthOfGroups += GroupLengh(g)

    rs = totalLengthOfGroups/numOfGroups/k
    return round(rs, 2)


def NRP(oldRules: list, newRules: list):    # New rules percentage(low is good)
    # find numOfRulesNew
    oldRules = [x[4] for x in oldRules]
    newRules = [x[4] for x in newRules]

    newDffRules = set(newRules) - set(oldRules)

    rs = len(newDffRules) / len(oldRules)  # (R' - R)/R
    return round(rs, 2)


def DRP(oldRules: list, newRules: list):  # Different rules percentage(low is good)
    oldRules = [x[4] for x in oldRules]
    newRules = [x[4] for x in newRules]

    newDffRules = set(newRules) - set(oldRules)
    oldDffRules = set(oldRules) - set(newRules)

    rs = (len(newDffRules) + len(oldDffRules)) / len(oldRules)  # =((R' - R) + (R -R'))/R
    return round(rs, 2)
