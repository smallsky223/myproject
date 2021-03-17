#!/usr/bin/python3
# -*- coding: utf-8 -*-


import h5py
import numpy as np
import struct
import py7zr

# whole_len为共同的数据集长度
whole_len = 0


# 针对浮点数的游程编码的解码，输入为代表有效/无效数据的块标志，块长度，和全部的有效值，返回原数据列表
def run_len_float(none_tag, run_tag, run_len, run_val):
    temp = []
    k = 0
    for i in range(0, len(run_len)):
        if run_tag[i] == '0':
            for j in range(0, run_len[i]):
                temp.append(none_tag)
        if run_tag[i] == '1':
            for j in range(0, run_len[i]):
                temp.append(run_val[k])
                k = k + 1
    if none_tag == -1:
        del run_val[:k]
    return temp


# 针对整形数的游程编码的解码，输入为游程标志和游程长度
def run_len_int(run_tag, run_len):
    temp = []
    for i in range(0, len(run_tag)):
        for j in range(0, run_len[i]):
            temp.append(run_tag[i])
    return temp


# 针对hole数据集的游程编码的解码，输入为块标志，块长度，以及每个块的首位值，以及两个用来返回的原数据列表
def run_len_hole(dset1, dset2, run_tag, run_len, run_val):
    dset2_temp1 = run_val[0]
    dset2_temp2 = run_val[1]
    k = 1
    for i in range(0, len(run_len)):
        if i > 0 and run_tag[i] == '0' and run_tag[i - 1] == '1':
            dset2_temp1 = dset2_temp1 - 1
            k = k + 1
            dset2_temp2 = run_val[k]
        if i > 0 and run_tag[i] == '1' and run_tag[i - 1] == '0':
            dset2_temp2 = run_val[k]
        for j in range(0, run_len[i]):
            dset1.append(run_tag[i])
            dset2[0].append(dset2_temp1)
            dset2[1].append(dset2_temp2)
            dset2_temp2 = dset2_temp2 + 1


# 解压缩函数
def zmw_decompress(fp):
    g1 = fp.create_group('PulseData/BaseCalls/ZMW')
    g2 = fp.create_group('PulseData/BaseCalls/ZMWMetrics')

    # 获取记录数据集长度的数据集（uint32）
    with open('./zmw_compress/HoleNumber.bin', "rb") as fp1:
        b = fp1.read(4)
        temp, = struct.unpack('I', b)
        global whole_len
        whole_len = temp
        temp = list(range(0, whole_len))
        g1.create_dataset('HoleNumber', data=temp, dtype='uint32')

    # 获取代表某一行数据是否有效的01串
    value_tag = []
    with open('./zmw_compress/value_tag.bin', "rb") as fp1:
        while True:
            b = fp1.read(1)
            if not b:
                break
            temp, = struct.unpack('B', b)
            temp_str = bin(temp).replace('0b', '').rjust(8, '0')
            value_tag.extend(list(temp_str))
    # 去除因补位产生的无关数据
    value_tag = value_tag[:(2 * whole_len)]

    # 数据集1（24个）, float32, 有效/无效数据的游程编码
    dname1 = ['BaseIpd',
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
              'SpectralDiagRR']
    # 对应24个数据集各自的宽度
    dim1 = [1, 1, 720, 1, 4, 4, 4, 4, 1, 4, 5 * 4, 4 * 4, 1, 4, 4, 4, 4, 5 * 4, 4, 1, 1, 1, 1, 4]
    run_tag1 = []
    run_len1 = []
    run_val1 = []
    with open('./zmw_compress/float_tag.bin', 'rb')as fp1, open('./zmw_compress/float_len.bin', 'rb')as fp2, \
            open('./zmw_compress/float_val.bin', 'rb')as fp3:
        while True:
            b = fp1.read(1)
            if not b:
                break
            temp, = struct.unpack('B', b)
            temp_str = bin(temp).replace('0b', '').rjust(8, '0')
            run_tag1.extend(list(temp_str))

        while True:
            b = fp2.read(1)
            if not b:
                break
            temp, = struct.unpack('B', b)
            run_len1.append(temp)

        while True:
            b = fp3.read(4)
            if not b:
                break
            temp, = struct.unpack('f', b)
            run_val1.append(temp)
    temp = run_len_float(0.0, run_tag1, run_len1, run_val1)
    # 将无效行和有效行按原排列恢复
    temp2 = np.zeros((whole_len, 829), dtype='float32')
    k = 0
    for i in range(0, whole_len):
        if value_tag[i] == '0':
            for j in range(0, 829):
                temp2[i][j] = 0.0
        if value_tag[i] == '1':
            for j in range(0, 829):
                temp2[i][j] = temp[k]
                k = k + 1
    # 将合并的总数据集按原数据集维度拆分为初始的多个数据集
    start_i = 0
    for i in range(0, 24):
        end_i = start_i + dim1[i]
        temp_data = temp2[:, start_i: end_i]
        # 恢复无效值-1.0
        if i < 4 or i == 8 or i == 20:
            temp_data[temp_data == 0.0] = -1.0
        # 1维
        if temp_data.shape[1] == 1:
            temp_data1 = temp_data.flatten()
            g2.create_dataset(dname1[i], data=temp_data1, dtype='float32')
        # 3维
        elif i == 10 or i == 11 or i == 17:
            width = int(dim1[i] / 4)
            temp3 = np.ones((4, whole_len, width), dtype='float32')
            m_start = 0
            for j in range(0, 4):
                m_end = m_start + width
                temp3[j, :, :] = temp_data[:, m_start: m_end]
                m_start = m_end
            temp_data3 = np.ones((whole_len, width, 4), dtype='float32')
            for k in range(0, whole_len):
                for m in range(0, width):
                    for n in range(0, 4):
                        temp_data3[k][m][n] = temp3[n][k][m]
            g2.create_dataset(dname1[i], data=temp_data3, dtype='float32', compression="gzip", compression_opts=3)
        # 2维
        else:
            g2.create_dataset(dname1[i], data=temp_data, dtype='float32')
        start_i = end_i

    # 数据集2（2个）, int16, 有效/无效数据的游程编码
    dname2 = ['NumBaseVsT',
              'NumPauseVsT']
    run_tag2 = []
    run_len2 = []
    run_val2 = []
    with open('./zmw_compress/int16_tag.bin', 'rb')as fp1, open('./zmw_compress/int16_len.bin', 'rb')as fp2, \
            open('./zmw_compress/int16_val.bin', 'rb')as fp3:
        while True:
            b = fp1.read(1)
            if not b:
                break
            temp, = struct.unpack('B', b)
            temp_str = bin(temp).replace('0b', '').rjust(8, '0')
            run_tag2.extend(list(temp_str))

        while True:
            b = fp2.read(1)
            if not b:
                break
            temp, = struct.unpack('B', b)
            run_len2.append(temp)

        while True:
            b = fp3.read(2)
            if not b:
                break
            temp, = struct.unpack('h', b)
            run_val2.append(temp)
    temp = run_len_float(-1, run_tag2, run_len2, run_val2)
    # 将无效行和有效行按原排列恢复
    temp2 = np.zeros((whole_len, 720), dtype='int16')
    k = 0
    for i in range(0, whole_len):
        if value_tag[whole_len + i] == '0':
            for j in range(0, 720):
                temp2[i][j] = -1
        if value_tag[whole_len + i] == '1':
            for j in range(0, 720):
                temp2[i][j] = temp[k]
                k = k + 1
    # d2[1]和d2[0]相同模板，只是有效值不一样
    temp3 = temp2
    k = 0
    for i in range(0, whole_len):
        for j in range(0, 720):
            if temp3[i][j] != -1:
                temp3[i][j] = run_val2[k]
                k = k + 1
    g2.create_dataset(dname2[0], data=temp2, dtype='int16')
    g2.create_dataset(dname2[1], data=temp3, dtype='int16')

    # 数据集3（3个）, uint8, 标志/长度的游程编码
    dname3 = ['HoleStatus',
              'Productivity',
              'ReadType']
    run_tag3 = []
    run_len3 = []
    with open('./zmw_compress/uint8_tag.bin', 'rb')as fp1, open('./zmw_compress/uint8_len.bin', 'rb')as fp2:
        while True:
            b = fp1.read(1)
            if not b:
                break
            temp, = struct.unpack('B', b)
            run_tag3.append(temp)

        while True:
            b = fp2.read(1)
            if not b:
                break
            temp, = struct.unpack('B', b)
            run_len3.append(temp)
    temp = run_len_int(run_tag3, run_len3)
    g1.create_dataset(dname3[0], data=temp[:whole_len], dtype='uint8')
    g2.create_dataset(dname3[1], data=temp[whole_len:(whole_len * 2)], dtype='uint8')
    g2.create_dataset(dname3[2], data=temp[(whole_len * 2):], dtype='uint8')

    # 数据集4（2个）, int16, HoleChipLook对应HoleXY的块标志
    dname4 = ['HoleChipLook',
              'HoleXY']
    run_tag4 = []
    run_len4 = []
    run_val4 = []
    with open('./zmw_compress/hole_tag.bin', 'rb')as fp1, open('./zmw_compress/hole_len.bin', 'rb')as fp2, \
            open('./zmw_compress/hole_val.bin', 'rb')as fp3:
        while True:
            b = fp1.read(1)
            if not b:
                break
            temp, = struct.unpack('B', b)
            temp_str = bin(temp).replace('0b', '').rjust(8, '0')
            run_tag4.extend(list(temp_str))

        while True:
            b = fp2.read(1)
            if not b:
                break
            temp, = struct.unpack('B', b)
            run_len4.append(temp)

        while True:
            b = fp3.read(2)
            if not b:
                break
            temp, = struct.unpack('h', b)
            run_val4.append(temp)
    temp1 = []
    temp2 = [[] for i in range(2)]
    run_len_hole(temp1, temp2, run_tag4, run_len4, run_val4)
    temp2 = np.array(temp2)
    g1.create_dataset(dname4[0], data=temp1, dtype='int16')
    g1.create_dataset(dname4[1], data=temp2.T, dtype='int16')

    # 数据集5, float + int, 无明显规模直接保存
    dname5 = ['BaseFraction',
              'RmBasQv',
              'RmDelQv',
              'RmInsQv',
              'RmSubQv',
              'NumEvent']
    for i in range(0, len(dname5)):
        if i == 5:
            temp = np.load('./zmw_compress/' + dname5[i] + '.npy')
            g1.create_dataset(dname5[i], data=temp, dtype='int32')
        else:
            temp = np.load('./zmw_compress/' + dname5[i] + '.npy')
            g2.create_dataset(dname5[i], data=temp, dtype='float32')


# 主函数，读取文件，调用解压函数
def main():
    # 将压缩打包的7z文件解压
    archive = py7zr.SevenZipFile('zmw.7z', mode='r')
    archive.extractall()
    archive.close()

    outputfile = 'm140928_41_s1_p0.1.bax.h5'
    fp = h5py.File(outputfile, 'w')
    # Perform file decompression
    zmw_decompress(fp)
    fp.close()


# Main launcher
if __name__ == "__main__":
    main()


# # cmd运行版本
# def main(args):
#     # Handle command line arguments
#     if len(args) != 1:
#         sys.exit("Usage: python zmwm_decode.py InputFile.7z OutputFile.h5")
#     inputfile = args[0]
#     outputfile = args[1]
#     # 将压缩打包的7z文件解压
#     archive = py7zr.SevenZipFile(inputfile, mode='r')
#     archive.extractall()
#     archive.close()
#     fp = h5py.File(outputfile, 'w')
#     # Perform file decompression
#     zmw_decompress(fp)
#     fp.close()
#
#
# # Main launcher
# if __name__ == "__main__":
#     main(sys.argv[1:])