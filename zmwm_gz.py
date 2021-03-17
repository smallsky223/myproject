#!/usr/bin/python3
# -*- coding: utf-8 -*-

import h5py
import numpy as np
import os


def main():
    inputfile = 'm140928_104939_ethan_c100699582550000001823139903261541_s1_p0.1.bax.h5'
    f = h5py.File(inputfile, 'r')
    # 数据集名
    dname = ['BaseIpd',
             'BaseRate',
             'BaseRateVsT',
             'BaseWidth',
             'CmBasQv',
             'CmDelQv',
             'CmInsQv',
             'CmSubQv',
             'DarkBaseRate',
             'HQRegionBpzvar',
             'HQRegionBpzvarw',
             'HQRegionDyeSpectra',
             'HQRegionEndTime',
             'HQRegionEstPkmid',
             'HQRegionEstPkstd',
             'HQRegionIntraPulseStd',
             'HQRegionPkzvar',
             'HQRegionPkzvarw',
             'HQRegionSNR',
             'HQRegionStartTime',
             'LocalBaseRate',
             'Pausiness',
             'ReadScore',
             'SpectralDiagRR',
             'BaseFraction',
             'RmBasQv',
             'RmDelQv',
             'RmInsQv',
             'RmSubQv',
             'NumBaseVsT',
             'NumPauseVsT',
             'ReadType',
             ]
    os.mkdir('./zmwm')
    for i in range(0, 32):
        if i <= 29:
            temp = np.array(f['PulseData/BaseCalls/ZMWMetrics/' + dname[i]], dtype='float32')
            np.save('./zmwm/' + dname[i], temp)
        elif i <= 30:
            temp = np.array(f['PulseData/BaseCalls/ZMWMetrics/' + dname[i]], dtype='int16')
            np.save('./zmwm/' + dname[i], temp)
        else:
            temp = np.array(f['PulseData/BaseCalls/ZMWMetrics/' + dname[i]], dtype='uint8')
            np.save('./zmwm/' + dname[i], temp)

    # 数据集名
    dname1 = ['NumEvent',
              'HoleNumber',
              'HoleChipLook',
              'HoleXY',
              'HoleStatus']
    for i in range(0, 5):
        if i <= 0:
            temp = np.array(f['PulseData/BaseCalls/ZMW/' + dname1[i]], dtype='int32')
            np.save('./zmwm/' + dname1[i], temp)
        elif i <= 1:
            temp = np.array(f['PulseData/BaseCalls/ZMW/' + dname1[i]], dtype='uint32')
            np.save('./zmwm/' + dname1[i], temp)
        elif i <= 3:
            temp = np.array(f['PulseData/BaseCalls/ZMW/' + dname1[i]], dtype='int16')
            np.save('./zmwm/' + dname1[i], temp)
        else:
            temp = np.array(f['PulseData/BaseCalls/ZMW/' + dname1[i]], dtype='uint8')
            np.save('./zmwm/' + dname1[i], temp)


# Main launcher
if __name__ == "__main__":
    main()
