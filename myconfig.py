INPUT_DATA_PATH = 'data/origin/adult.data'
PRE_DATA_PATH = 'data/process/adult.data'
KANONYMITY_DATA_PATH = 'data/result/{}/adult-anon-{}.data'
DATA_SAMPLE = 0
R_INIT_PATH = 'cache/r_inital'
ALL_COLUMNS = ['age', 'workclass', 'fnlwgt', 'education', 'education-num', 'marital-status', 'occupation',
               'relationship', 'race', 'sex', 'capital-gain', 'capital-loss', 'hours-per-week', 'native-country', 'income']
# paper m3ar
USE_COLUMNS =['age', 'sex', 'marital-status', 'native-country', 'race', 'education',
              'capital-gain', 'capital-loss','relationship','occupation']
 

# USE_COLUMNS = ['age', 'workclass', 'education', 'marital-status', 'occupation', 'relationship',   'race', 'sex', 'capital-gain', 'capital-loss', 'hours-per-week', 'native-country']
MIN_SUP = 0.5
MIN_CONF = 0.5

QUASI = ['age', 'sex', 'marital-status', 'native-country', 'race', 'education']
# ['age', 'education', 'occupation','relationship', 'sex', 'native-country']

# compair others algo
# USE_COLUMNS = ['sex', 'age', 'race', 'marital-status', 'education',
#                'native-country', 'workclass', 'occupation', 'income']
# QUASI = USE_COLUMNS[:-1]

# QUASI = ['age','workclass','education','marital-status','occupation','race','sex','native-country']
DATABASE_PATH = "data/eval_result.db"
EVALUATE_FIGURE = "data/eval_figure.png"
