#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov  8 14:18:26 2021

@author: marcos
"""

import pandas as pd
import time
import math
import os
import csv

def cpt(v, i, cycles=1024):

    class MAF:
        index = 0
        full = 0

        def __init__(self):
            self.moving = [0] * cycles

        def feed(self, n):
            self.full = self.full - self.moving[self.index]
            self.full = self.full + n
            self.moving[self.index] = n

            self.index = self.index + 1
            if self.index >= cycles:
                self.index = self.index - cycles
            return self

        def get(self):
            return self.full / cycles

    class UnbiasedIntegral(MAF):
        integral = 0

        def feed(self, n):
            self.integral = self.integral + n
            super().feed(self.integral)
            return self

        def get(self):
            return (self.integral - super().get()) * 2 * math.pi / cycles

    Pa = MAF()  # Potência ativa média

    v_c = UnbiasedIntegral()  # Integral imparcial da tensão
    W = MAF()  # Energia reativa média

    V = [0] * len(v)
    U = MAF()  # Valor eficaz da tensão

    U_C = MAF()  # Valor eficaz da integral imparcial da tensão

    I = [0] * len(v)
    _I = MAF()  # Valor eficaz da corrente

    i_a = MAF()  # Corrente ativa

    Ia = [0] * len(v)

    i_v = MAF()  # Corrente residual

    Iv = [0] * len(v)
    Ir = [0] * len(v)

    P = [0] * len(v)  # Potência ativa média

    Q = [0] * len(v)  # Potência reativa média

    D = [0] * len(v)  # Potência residual média

    fp = [0] * len(v)  # Fator de potência

    fl = [0] * len(v)  # Fator de não lineridade

    fr = [0] * len(v)  # Fator de reatividade

    for index in range(len(v)):
        # Calcula a potência ativa média
        _P = Pa.feed(v[index]*i[index]).get()

        # Salva a potência ativa
        P[index] = _P

        # Calcula a integral parcial da tensão
        _v_c = v_c.feed(v[index]).get()

        # Calcula a energia reativa
        _W = W.feed(_v_c * i[index]).get()

        # Calcula a tensão eficaz ao quadrado
        _U = U.feed(v[index] ** 2).get()
        # Calcula a tensão eficaz
        V[index] = math.sqrt(_U)

        # Calcula a corrente eficaz
        I[index] = math.sqrt(_I.feed(i[index] ** 2).get())

        _ia = 0
        if _U != 0:
            # Calcula a corrente ativa instantânea
            _ia = _P * v[index] / _U

        # Calcula a corrente ativa eficaz
        Ia[index] = math.sqrt(i_a.feed(_ia ** 2).get())

        # Calcula a integral parcial da tensão eficaz ao quadrado
        _U_C = U_C.feed(_v_c ** 2).get()

        _ir = 0
        Ir[index] = 0
        if _U_C != 0:
            # Calcula a corrente reativa instantânea
            _ir = _W * _v_c / _U_C
            # Calcula a corrente reativa eficaz
            Ir[index] = _W / math.sqrt(_U_C)

        # Calcula a potência reativa
        Q[index] = V[index] * Ir[index]

        # Calcula a corrente residual instantânea
        _iv = i[index] - _ia - _ir

        # Calcula a corrente residual eficaz
        Iv[index] = math.sqrt(i_v.feed(_iv ** 2).get())

        # Calcula a potência residual
        D[index] = V[index] * Iv[index]

        if I[index] != 0:
            # Calcula o fator de potência
            fp[index] = Ia[index] / I[index]

            # Calcula o fator de linearidade (versão Wesley)
            fl[index] = 1 - Iv[index] / I[index]

            # Calcula o fator de reatividade (versão Wesley)
            fr[index] = 1 - Ir[index] / I[index]
        else:
            fp[index] = 0
            fl[index] = 0
            fr[index] = 0
    return \
{'V': V, 'I': I, 'Ia': Ia, 'Ir': Ir, 'Iv': Iv,
 'P': P, 'Q': Q, 'D': D, 'fp': fp, 'fl': fl, 'fr': fr}

def convert(arquivo):
    start = time.time()
    col_names = ['date', 'VA', 'VB', 'VC', 'VN', 'IA', 'IB', 'IC', 'IN']
    data = pd.read_csv(arquivo, header=None, names=col_names, sep='\t')

    res = cpt(v=data['VA'], i=data['IA'])

    with open(os.path.splitext(arquivo)[0] + "_d.csv", "w") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(res.keys())
        writer.writerows(zip(*res.values()))

    end = time.time()
    print("A execução de", arquivo, "levou :", end-start, "segundos")

def multi_convert():
    from multiprocessing import Pool, cpu_count

    # Calcula aqui a decomposição CPT das medições
    with Pool(cpu_count() ) as p:
        p.map(convert, [
            "esmeril1.csv",
            "ferro_de_solda1.csv",
            "ferro_de_solda2.csv",
            "ferro_de_solda3.csv",
            "fonte1.csv",
            "laptop1.csv",
            "laptop2.csv",
            "luminaria1.csv",
            "luminaria2.csv",
            "luminaria3.csv",
            "luminaria4.csv",
            "mandril1.csv",
            "microondas1.csv",
            "monitor1.csv",
            "monitor2.csv",
            "osciloscópio.csv",
            "qualímetro_pq13.csv",
            "refletor_led1.csv",
            "refletor_led2.csv",
            "soprador_térmico1.csv",
            "ventilador1.csv",
            "ventilador2.csv",
            "ventilador3.csv",
            "ventilador4.csv"
        ])

    p.close()
    p.join()

if __name__ == '__main__':
    multi_convert()
