from scipy import interpolate
import numpy as np

class YieldCurve:
    """Build Curve with given points and Calculate roll_down with given tenor and roll_down period"""
    acceptableKeyList = ['3m', '6m', '9m', '1y', '2y', '3y', '4y', '5y', '6y', '7y', '8y', '9y', '10y', '20y', '30y']
    tenorDict = {'3m': 0.25, '6m': 0.5, '9m': 0.75, '1y': 1, '2y': 2, '3y': 3, '4y': 4, '5y': 5, '6y': 6, '7y': 7,
                 '8y': 8, '9y': 9, '10y': 10, '20y': 20, '30y': 30}

    def __init__(self, **kwargs):
        """Initialize a dictionary with given yield
        Keyword Argument:
        kwargs -- list of yield points, in form of {'1y': yield}
        """
        self.yieldDict = {}
        if kwargs is not None:
            for key in kwargs.keys():
                if key in YieldCurve.acceptableKeyList:
                    self.yieldDict[key] = kwargs[key]

    def build_curve(self, interpolate_tenor):
        """Build a cubic spline curve with given yields

        Argument:
        interpolate_tenor -- list of points in form of double number eg.[3.74,3.75]

        Output: List of yields with respect to the given tenors
        """

        tenors = []
        yields = []
        for key in self.yieldDict.keys():
            tenors.append(YieldCurve.tenorDict[key])
            yields.append(self.yieldDict[key])
        #print(tenors)
        #print(yields)
        try:
            for each in interpolate_tenor:
                if each<np.min(tenors):
                    raise ValueError
            fit_curve = interpolate.interp1d(tenors, yields, kind='cubic')  # cubit spline interpolation
            #print(list(fit_curve(interpolate_tenor)))
            return list(fit_curve(interpolate_tenor))

        except (Exception) :
            print("Error: one of the interpolate tenor is too small")




    def calc_roll_down(self, tenor, roll_down):
        """Calculate roll_down with given parameters

        Arguments:
        tenor -- list of tenors compatible with acceptableKeyList eg. ['1y','3y']
        roll_down -- list of roll_down periods compatible with acceptableKeyList eg. ['3m','3m']

        Output: List of roll_down
        """
        t = [YieldCurve.tenorDict[x]for x in tenor]
        rd =[YieldCurve.tenorDict[x]for x in roll_down]
        yields1 = self.build_curve(t)
        zip_tenors = zip(t,rd)
        yields2 = self.build_curve([x-y for x,y in zip_tenors])
        zip_lists = zip(yields1, yields2)
        return [x-y for x,y in zip_lists]



