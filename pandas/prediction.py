import numpy as np
import pandas as pd

class prediction(object):
    def __init__(self, dataPath='./pm2.5Taiwan.csv'):
        self.data = pd.read_csv(dataPath)

    def function(self):
        print(self.data.shape) # (465317, 27)
        


if __name__ == '__main__':
    prediction_obj = prediction()
    prediction_obj.function()
