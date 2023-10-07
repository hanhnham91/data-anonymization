import pandas as pd
from myconfig import *
import matplotlib.pyplot as plt


# preprocess dataset
def Preprocess(dataPath: str, newDataPath: str, dataSample):
    df = pd.read_csv(dataPath, names=ALL_COLUMNS,
                     index_col=False, skipinitialspace=True)
    df = df[USE_COLUMNS]
    for col in USE_COLUMNS:
        df = df.drop(df[df[col] == '?'].index)
    # _ = plt.hist(df['age'], bins='auto')
    # _ = plt.hist(df['hours-per-week'], bins=10)
    # plt.show()

    if dataSample > 0:
        df = df.sample(n=dataSample)
    print("Total row:", df.shape[0])

    df.to_csv(newDataPath, index=False, header=False)
    return df
