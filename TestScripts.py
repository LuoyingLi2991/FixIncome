# -*- coding: utf-8 -*-
"""
Created on Wed Sep 27 14:34:18 2017

@author: luoying.li
"""

import unittest
from YieldCurve import YieldCurve
from SpotCurve import SpotCurve
from UtilityClass import UtilityClass
import numpy as np
import pandas as pd
import datetime 


class TestYieldCurve(unittest.TestCase):
    """Unittest For YieldCurve Class"""
    kwargs = {'2y': np.log(2), '3y': np.log(3), '4y': np.log(4), '5y': np.log(5),
              '8y': np.log(8), '9y': np.log(9), '10y': np.log(10)}
    
    forward_pts = {'2y': np.log2(2), '3y': np.log2(3), '4y': np.log2(4), '6y': np.log2(6),
                   '8y': np.log2(8), '9y': np.log2(9), '10y': np.log2(10)}
    
    
    yy = YieldCurve(**kwargs)  # Initiate a module with points extracted from 'log(x)' function
    
    def test_init(self):
        """test __init__"""
        self.assertEqual(TestYieldCurve.yy.yieldDict['2y'], np.log(2))
        self.assertEqual(TestYieldCurve.yy.yieldDict['3y'], np.log(3))

    def test_build_curve(self):
        """test build_curve method"""
        test_tenor = [2, 4, 4.5]
        for i,each in enumerate(TestYieldCurve.yy.build_curve(test_tenor)):
            self.assertAlmostEqual(each,np.log(test_tenor[i]),places=2)
            # Approx Equal up to 2 decimal places

    def test_calc_roll_down(self):
        """test calc_roll_down method"""
        tenor = ['4y','5y']
        roll_down = ['3m','3m']
        test_ans=np.log([4/3.75, 5/4.75])  # True roll_down should be 'log(4)-log(3.75)'
        for i,each in enumerate(TestYieldCurve.yy.calc_roll_down(tenor,roll_down)):
            self.assertAlmostEqual(each, test_ans[i],places=2)
        # Approx Equal up to 2 decimal places
        ans2=[np.log(4)-np.log2(4),np.log(5)-np.log2(5)]
        for i,each in enumerate(TestYieldCurve.yy.calc_roll_down(tenor,roll_down,TestYieldCurve.forward_pts,'3m')):
            self.assertAlmostEqual(each, ans2[i],places=2)
        
class TestSpotCurve(unittest.TestCase):
    spot_pts = {'1y': np.log(1), '3y': np.log(3), '4y': np.log(4), '5y': np.log(5),
                '8y': np.log(8), '9y': np.log(9), '10y': np.log(10)}
    forward_pts = {'2y': np.log2(2), '3y': np.log2(3), '4y': np.log2(4), '6y': np.log2(6),
                   '8y': np.log2(8), '9y': np.log2(9), '10y': np.log2(10)}
    test_object = SpotCurve(spot_pts, forward_pts)

    def test_calc_roll_down(self):
        """Test calc roll down"""
        tenor ='4y'
        roll_down ='3m'
        test_ans = np.log(4 / 3.75)  # True roll_down should be 'log(4)-log(3.75)'
        self.assertAlmostEqual(TestSpotCurve.test_object.calc_roll_down(tenor, roll_down), test_ans, places=2)
        tenor = ['4y', '5y']
        roll_down = ['3m', '3m']
        test_ans = np.log([4 / 3.75,5/4.75])
        for i, each in enumerate(TestSpotCurve.test_object.calc_roll_down(tenor, roll_down)):
            self.assertAlmostEqual(each, test_ans[i], places=2)

    def test_calc_total_return(self):
        """test total return"""
        tenor = ['4y']
        roll_down = ['3m']
        test_ans = [np.log2(3.75)-np.log(3.75)]
        for i, each in enumerate(TestSpotCurve.test_object.calc_total_return(tenor, roll_down)):
            self.assertAlmostEqual(each, test_ans[i], places=2)
        tenor ='4y'
        roll_down ='3m'
        test_ans = np.log2(3.75)-np.log(3.75)  # True roll_down should be 'log(4)-log(3.75)'
        self.assertAlmostEqual(TestSpotCurve.test_object.calc_total_return(tenor, roll_down), test_ans, places=2)
   
        
    def test_calc_carry(self):
        """test carry"""
        tenor = ['4y']
        roll_down_p = ['3m']
        carry = TestSpotCurve.test_object.calc_carry(tenor, roll_down_p)
        roll_down = TestSpotCurve.test_object.calc_roll_down(tenor, roll_down_p)
        zip_tenors = zip(carry, roll_down)
        yields1 = [x + y for x, y in zip_tenors]
        yields2 = TestSpotCurve.test_object.calc_total_return(tenor, roll_down_p)
        for i,each in enumerate(yields2):
            self.assertAlmostEqual(each,yields1[i],places=2)
        tenor ='4y'
        roll_down_p ='3m'
        carry = TestSpotCurve.test_object.calc_carry(tenor, roll_down_p)
        roll_down = TestSpotCurve.test_object.calc_roll_down(tenor, roll_down_p)
        test_ans = carry+roll_down
        self.assertAlmostEqual(TestSpotCurve.test_object.calc_total_return(tenor, roll_down_p), test_ans, places=2)
        

class TestUtilityClass(unittest.TestCase):
    ob=UtilityClass()
    today=datetime.date.today()
    dates=[]
    for i in range(100):
        dd=today-datetime.timedelta(days=1+i)
        dates.insert(0,dd)
    s = pd.DataFrame(range(100), index=dates)
    def test_calc_z_score_sym(self):
        [lvl,z]=TestUtilityClass.ob.calc_z_score(TestUtilityClass.s, True,'1d','1w')
        a_lvl=[99, 98, 92]
        a_z=[1.7148160424389376, 1.7146428199482247, 1.7135256673787986]
        for i in range(3):
            self.assertAlmostEqual(lvl[i],a_lvl[i],places=2)
            self.assertAlmostEqual(z[i],a_z[i],places=2)
    def test_calc_z_score_asy(self):
        [lvl,z]=TestUtilityClass.ob.calc_z_score(TestUtilityClass.s, False,'1d','1w')
        a_lvl=[99, 98, 92]
        a_z=[3.4301466969424235, 3.4648232278140827, 3.4649204549116632]
        for i in range(3):
            self.assertAlmostEqual(lvl[i],a_lvl[i],places=2)
            self.assertAlmostEqual(z[i],a_z[i],places=2)
    def test_calc_percentile(self):
        p=TestUtilityClass.ob.calc_percentile(TestUtilityClass.s,'1d','1w')
        a_p=[1.0, 1.0, 1.0]
        for i in range(3):
            self.assertAlmostEqual(p[i],a_p[i],places=2)
    def test_calc_percentile_level(self):
        l=TestUtilityClass.ob.calc_percentile_level(TestUtilityClass.s,95,'1d','1w')
        a_l=[94.049999999999997, 93.099999999999994, 87.399999999999991]
        for i in range(3):
            self.assertAlmostEqual(l[i],a_l[i],places=2)  






test_suite = unittest.TestSuite()
test_suite.addTest(unittest.makeSuite(TestSpotCurve))     
test_suite.addTest(unittest.makeSuite(TestYieldCurve)) 
test_suite.addTest(unittest.makeSuite(TestUtilityClass)) 
runner=unittest.TextTestRunner()
print runner.run(test_suite)

