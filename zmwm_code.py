#!/usr/bin/python3
# -*- coding: utf-8 -*-


import h5py
import numpy as np
import struct
import math
import os
import py7zr


# 针对浮点数的游程编码，返回代表有效/无效数据的块标志，块长度，和全部的有效值
def run_len_float(val_dset, none_tag, run_len, run_val):
    run_tag = ''
    v0_len = 0
    v1_len = 0
    flag = 0
    # 循环遍历，对数据进行游程编码
    for i in range(0, len(val_dset)):
        # 当前数据为none_tag时，即无效数据
        if val_dset[i] == none_tag:
            # 有效数据长度大于0，说明之前是有效数据游程块，记录后归零
            if v1_len > 0:
                run_tag = run_tag + '1'
                run_len.append(v1_len)
                v1_len = 0
                v0_len = v0_len + 1
                flag = 0
            # 防止游程块长度超过1个字节
            elif flag == 1:
                run_tag = run_tag + '0'
                run_len.append(v0_len)
                v0_len = 0
                v0_len = v0_len + 1
                flag = 0
            # 否则说明之前是无效数据游程块
            else:
                v0_len = v0_len + 1
                if v0_len == 255:
                    flag = 1
        # 当前数据为有效数据时
        else:
            # 无效数据长度大于0，说明之前是无效数据游程块，记录后归零
            if v0_len > 0:
                run_tag = run_tag + '0'
                run_len.append(v0_len)
                v0_len = 0
                v1_len = v1_len + 1
                flag = 0
            # 防止游程块长度超过1个字节
            elif flag == 1:
                run_tag = run_tag + '1'
                run_len.append(v1_len)
                v1_len = 0
                v1_len = v1_len + 1
                flag = 0
            # 否则说明之前是有效数据的游程块
            else:
                v1_len = v1_len + 1
                if v1_len == 255:
                    flag = 1
    # 记录最后的游程块的标志和长度
    if v0_len > 0:
        run_tag = run_tag + '0'
        run_len.append(v0_len)
    else:
        run_tag = run_tag + '1'
        run_len.append(v1_len)

    # 循环遍历此行，记录有效数据
    for i in range(0, len(val_dset)):
        if val_dset[i] != none_tag:
            run_val.append(val_dset[i])

    # 将游程块标志01串填充到8的倍数，补0
    run_tag = run_tag.ljust((math.ceil(len(run_tag) / 8)) * 8, '0')
    return run_tag


# 针对整形数的游程编码，返回游程标志和游程长度
def run_len_int(dset, run_tag, run_len):
    length = len(dset)
    i = 0
    while i < length:
        run_tag.append(dset[i])
        for j in range(1, 256):
            if (i + j == length) or (dset[i + j] != dset[i]):
                run_len.append(j)
                i = i + j
                break
        # 防止游程块长度超过1个字节
        if (j == 255) and (i + j != length):
            run_len.append(j)
            i = i + j


# 针对hole数据集的游程编码，返回块标志，块长度，以及每个块的首位值
def run_len_hole(dset1, dset2, run_len, run_val):
    run_tag = ''
    v0_len = 0
    v1_len = 0
    flag = 0
    # 循环遍历，对数据进行游程编码
    for i in range(0, len(dset1)):
        # 当前数据为0时，即代表x坐标
        if dset1[i] == 0:
            # y坐标数据长度大于0，说明之前是y坐标数据块，记录后归零
            if v1_len > 0:
                run_tag = run_tag + '1'
                run_len.append(v1_len)
                v1_len = 0
                v0_len = v0_len + 1
                flag = 0
                run_val.append(dset2[i])
            # 防止游程块长度超过1个字节
            elif flag == 1:
                run_tag = run_tag + '0'
                run_len.append(v0_len)
                v0_len = 0
                v0_len = v0_len + 1
                flag = 0
            # 否则说明之前是x坐标数据游程块
            else:
                v0_len = v0_len + 1
                if v0_len == 255:
                    flag = 1
        # 当前数据代表y坐标时
        else:
            # x坐标数据长度大于0，说明之前是x坐标数据游程块，记录后归零
            if v0_len > 0:
                run_tag = run_tag + '0'
                run_len.append(v0_len)
                v0_len = 0
                v1_len = v1_len + 1
                flag = 0
            # 防止游程块长度超过1个字节
            elif flag == 1:
                run_tag = run_tag + '1'
                run_len.append(v1_len)
                v1_len = 0
                v1_len = v1_len + 1
                flag = 0
            # 否则说明之前是y坐标数据的游程块
            else:
                v1_len = v1_len + 1
                if v1_len == 255:
                    flag = 1
    # 记录最后的游程块的标志和长度
    if v0_len > 0:
        run_tag = run_tag + '0'
        run_len.append(v0_len)
    else:
        run_tag = run_tag + '1'
        run_len.append(v1_len)

    # 将游程块标志01串填充到8的倍数，补0
    run_tag = run_tag.ljust((math.ceil(len(run_tag) / 8)) * 8, '0')
    return run_tag


# 压缩函数
def zmw_compress(fp):
    # 数据集1（24个）, float32, 有效/无效数据的游程编码
    dname1 = ['BaseRate',
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
              'SpectralDiagRR']
    temp = np.array(fp['PulseData/BaseCalls/ZMWMetrics/BaseIpd'], dtype='float32')
    # 将无效数据-1.0等价为0.0
    temp[temp == -1.0] = 0.0
    temp = np.array([temp])
    dset = temp.T
    # 将数据集按列并排合并
    for i in range(0, len(dname1)):
        temp = np.array(fp['PulseData/BaseCalls/ZMWMetrics/' + dname1[i]], dtype='float32')
        temp[temp == -1.0] = 0.0
        # 处理1维数据集，将数据集转置变成2维
        if temp.ndim == 1:
            temp = np.array([temp])
            dset = np.concatenate((dset, temp.T), axis=1)
        # 处理3维数据集，将3维展平为多个2维合并
        elif temp.ndim == 3:
            temp1 = temp[:, :, 0]
            for j in range(1, temp.shape[2]):
                temp1 = np.concatenate((temp1, temp[:, :, j]), axis=1)
            dset = np.concatenate((dset, temp1), axis=1)
        # 2维数据集直接合并
        else:
            dset = np.concatenate((dset, temp), axis=1)
    val_tag1 = ''
    # 循环遍历合并后的数据集的每一行，当此行全为无效数据时设为0，反之设为1
    for i in range(0, dset.shape[0]):
        flag = 1
        for j in range(0, dset.shape[1]):
            if dset[i][j] != 0.0:
                flag = 0
                break
        if flag == 0:
            val_tag1 = val_tag1 + '1'
        else:
            val_tag1 = val_tag1 + '0'
    val_dset = []
    # 将所有有效行合并
    for i in range(0, len(val_tag1)):
        # 当为1时代表此行为有效行
        if val_tag1[i] == '1':
            for j in range(0, dset.shape[1]):
                val_dset.append(dset[i][j])
    run_len1 = []
    run_val1 = []
    run_tag1 = run_len_float(val_dset, 0.0, run_len1, run_val1)

    # 数据集2（2个）, int16, 有效/无效数据的游程编码
    dname2 = ['NumBaseVsT',
              'NumPauseVsT']
    dset = np.array(fp['PulseData/BaseCalls/ZMWMetrics/' + dname2[0]], dtype='int16')
    temp = np.array(fp['PulseData/BaseCalls/ZMWMetrics/' + dname2[1]], dtype='int16')
    val_tag2 = ''
    # 循环遍历每一行，当此行全为无效数据时设为0，反之设为1
    for i in range(0, dset.shape[0]):
        flag = 1
        for j in range(0, dset.shape[1]):
            if dset[i][j] != -1:
                flag = 0
                break
        if flag == 0:
            val_tag2 = val_tag2 + '1'
        else:
            val_tag2 = val_tag2 + '0'
    val_dset1 = []
    val_dset2 = []
    # 将所有有效行合并
    for i in range(0, len(val_tag2)):
        # 当为1时代表此行为有效行
        if val_tag2[i] == '1':
            for j in range(0, dset.shape[1]):
                val_dset1.append(dset[i][j])
                val_dset2.append(temp[i][j])
    run_len2 = []
    run_val2 = []
    run_tag2 = run_len_float(val_dset1, -1, run_len2, run_val2)
    # temp和dset是一样的数据集模版，只是值不同
    for i in range(0, len(val_dset2)):
        if val_dset2[i] != -1:
            run_val2.append(val_dset2[i])

    # 数据集3（3个）, uint8, 标志/长度的游程编码
    dname3 = ['HoleStatus',
              'Productivity',
              'ReadType']
    dset = np.array(fp['PulseData/BaseCalls/ZMW/' + dname3[0]], dtype='uint8')
    temp = np.array(fp['PulseData/BaseCalls/ZMWMetrics/' + dname3[1]], dtype='uint8')
    temp1 = np.array(fp['PulseData/BaseCalls/ZMWMetrics/' + dname3[2]], dtype='uint8')
    dset = np.concatenate((dset, temp, temp1), axis=0)
    run_tag3 = []
    run_len3 = []
    run_len_int(dset, run_tag3, run_len3)

    # 数据集4（2个）, int16, HoleChipLook对应HoleXY的块标志
    dname4 = ['HoleChipLook',
              'HoleXY']
    dset = np.array(fp['PulseData/BaseCalls/ZMW/' + dname4[0]], dtype='int16')
    temp = np.array(fp['PulseData/BaseCalls/ZMW/' + dname4[1]], dtype='int16')
    run_len4 = []
    run_val4 = []
    run_val4.append(temp[0][0])
    run_val4.append(temp[0][1])
    run_tag4 = run_len_hole(dset, temp[:, 1], run_len4, run_val4)

    # 文件写入部分
    os.mkdir('./zmw_compress')
    val_tag = val_tag1 + val_tag2
    # 将01串填充到8的倍数，补0
    val_tag = val_tag.ljust((math.ceil(len(val_tag) / 8)) * 8, '0')
    # 将代表某一行数据是否有效的01串写入文件1
    with open('./zmw_compress/value_tag.bin', 'wb')as fp1:
        # 每8bit打包写入二进制文件
        for x in range(0, len(val_tag), 8):
            b = struct.pack('B', int(val_tag[x:x + 8], 2))
            fp1.write(b)

    # 将数据集1的总的游程块标志、长度、有效数据分别写入三个文件
    with open('./zmw_compress/float_tag.bin', 'wb')as fp1, open('./zmw_compress/float_len.bin', 'wb')as fp2, \
            open('./zmw_compress/float_val.bin', 'wb')as fp3:
        for x in range(0, len(run_tag1), 8):
            b = struct.pack('B', int(run_tag1[x:x + 8], 2))
            fp1.write(b)

        for x in range(0, len(run_len1)):
            b = struct.pack('B', run_len1[x])
            fp2.write(b)

        for x in range(0, len(run_val1)):
            b = struct.pack('f', run_val1[x])
            fp3.write(b)

    # 将数据集2的总的游程块标志、长度、有效数据分别写入三个文件
    with open('./zmw_compress/int16_tag.bin', 'wb')as fp1, open('./zmw_compress/int16_len.bin', 'wb')as fp2, \
            open('./zmw_compress/int16_val.bin', 'wb')as fp3:
        for x in range(0, len(run_tag2), 8):
            b = struct.pack('B', int(run_tag2[x:x + 8], 2))
            fp1.write(b)

        for x in range(0, len(run_len2)):
            b = struct.pack('B', run_len2[x])
            fp2.write(b)

        for x in range(0, len(run_val2)):
            b = struct.pack('h', run_val2[x])
            fp3.write(b)

    # 将数据集3的总的游程块标志、长度分别写入两个文件
    with open('./zmw_compress/uint8_tag.bin', 'wb')as fp1, open('./zmw_compress/uint8_len.bin', 'wb')as fp2:
        for x in range(0, len(run_tag3)):
            b = struct.pack('B', run_tag3[x])
            fp1.write(b)

        for x in range(0, len(run_len3)):
            b = struct.pack('B', run_len3[x])
            fp2.write(b)

    # 将数据集4的总的游程块标志、长度、每个块的首位数据分别写入三个文件
    with open('./zmw_compress/hole_tag.bin', 'wb')as fp1, open('./zmw_compress/hole_len.bin', 'wb')as fp2, \
            open('./zmw_compress/hole_val.bin', 'wb')as fp3:
        for x in range(0, len(run_tag4), 8):
            b = struct.pack('B', int(run_tag4[x:x + 8], 2))
            fp1.write(b)

        for x in range(0, len(run_len4)):
            b = struct.pack('B', run_len4[x])
            fp2.write(b)

        for x in range(0, len(run_val4)):
            b = struct.pack('h', run_val4[x])
            fp3.write(b)

    # 数据集5, float + int, 无明显规模直接保存
    dname5 = ['BaseFraction',
              'RmBasQv',
              'RmDelQv',
              'RmInsQv',
              'RmSubQv',
              'NumEvent']
    for i in range(0, len(dname5)):
        if i == 5:
            temp = np.array(fp['PulseData/BaseCalls/ZMW/' + dname5[i]], dtype='int32')
            np.save('./zmw_compress/' + dname5[i], temp)
        else:
            temp = np.array(fp['PulseData/BaseCalls/ZMWMetrics/' + dname5[i]], dtype='float32')
            np.save('./zmw_compress/' + dname5[i], temp)

    # 记录数据集长度的数据集（uint32）
    temp = np.array(fp['PulseData/BaseCalls/ZMW/HoleNumber'], dtype='uint32')
    with open('./zmw_compress/HoleNumber.bin', 'wb')as fp1:
        b = struct.pack('I', temp.shape[0])
        fp1.write(b)


# 主函数，读取文件，调用压缩函数，打包所有压缩后的文件
def main():
    inputfile = 'm140928_104939_ethan_c100699582550000001823139903261541_s1_p0.1.bax.h5'
    fp = h5py.File(inputfile, 'r')
    # Perform file compression
    zmw_compress(fp)
    # 将所有文件打包成7z包
    # my_filters = [{"id": py7zr.FILTER_PPMD, 'level': 6, 'mem': 16}]
    with py7zr.SevenZipFile('zmw.7z', 'w') as z:
        z.writeall('./zmw_compress')
    fp.close()


# Main launcher
if __name__ == "__main__":
    main()


# # cmd运行版本
# def main(args):
#     # Handle command line arguments
#     if len(args) != 1:
#         sys.exit("Usage: python zmwm_code.py InputFile.h5 OutputFile.7z")
#     inputfile = args[0]
#     outputfile = args[1]
#     fp = h5py.File(inputfile, 'r')
#     # Perform file compression
#     zmw_compress(fp)
#     # 将所有文件打包成7z包
#     # my_filters = [{"id": py7zr.FILTER_PPMD, 'level': 6, 'mem': 16}]
#     with py7zr.SevenZipFile(outputfile, 'w') as z:
#         z.writeall('./zmw_compress')
#     fp.close()
#
#
# # Main launcher
# if __name__ == "__main__":
#     main(sys.argv[1:])
