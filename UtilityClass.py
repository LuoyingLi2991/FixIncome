import numpy as np
from scipy import stats

class UtilityClass:
    """Calculate Z_score of a given time series"""
    @staticmethod
    def calc_z_score(series_list, symmetric):
        """Calculate Z_score

        Arguments:
        series_list -- list of returns to be analysed
        symmetric -- a boolean variable indicating whether the series is symmetric
        """
        mu = np.mean(series_list)
        sigma = np.std(series_list)
        if symmetric:
            return (series_list[-1]-mu)/sigma
        # if the series is not symmetric, compute two different sigma
        else:
            below_aver = list(filter(lambda x: x < mu, series_list))
            above_aver = list(filter(lambda x: x > mu, series_list))
            sigma_neg = np.std(below_aver)
            sigma_pos = np.std(above_aver)
            if series_list[-1]>mu:
                return (series_list[-1]-mu)/sigma_pos
            else:
                return (series_list[-1]-mu)/sigma_neg

    @staticmethod
    def calc_percentile(series_list):
        """return the percentile of the last point in the list

        Argument:
        series_list -- list of returns to be analysed
        """
        return stats.percentileofscore(series_list,series_list[-1],kind='weak')


    @staticmethod
    def calc_percentile_level(series_list, perctl):
        """return the percentile level of a given list

        Argument:
        series_list -- list of returns to be analysed
        perctl -- target percentile e.g 25 means the method returns a value of 25% percentile of the list
        """
        return np.percentile(series_list, perctl)




