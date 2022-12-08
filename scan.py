#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 08:34:12 2022

@author: marcos
"""

import pandas as pd
import matplotlib.pyplot as plt

def scan(path):
    import os
    import numpy as np
    import scipy.stats

    # Variação mínima de potência a considerar como arranque
    ΔP = 2.5
    # Quantidade de amostras por ciclo
    amostra_por_ciclo = 1024
    # Frequencia da rede
    frequencia = 60
    # Tempo para considerar o sinal estável, isso remove o "spike" inicial de algumas cargas
    tempo_estabilização = amostra_por_ciclo*2
    # Ciclos a capturar antes do inicio
    n_before = amostra_por_ciclo * 2
    # Ciclos a capturar depois do inicio
    n_after = 5 * frequencia * amostra_por_ciclo
    # Porcentagem da potência a manter para considerar que o circuito ativou
    p_min = 0.9
    # Ciclos a ignorar no início por causa do calcula da CPT
    ign = amostra_por_ciclo

    nomes = [ 'P', 'Q', 'D', 'V', 'I', 'Ia', 'Ir', 'Iv' ]
    filename = os.path.splitext(os.path.basename(path) )[0]
    filename = filename[:len(filename)-2]

    print("Analisando " + filename + "...")

    passo = 0;
    for chunk in pd.read_csv(path, \
                             usecols=nomes, \
                             chunksize=300*1024*1024):
        for n, amostra in chunk.iterrows():
            if passo == 0: # Espera pela estabilização do calculo dos dados da CPT
                if n >= ign:
                    # Guarda o patamar inicial                    
                    ac = amostra
                    passo = 1
            elif passo == 1: # Verifica se a variação é maior que o threshold ΔP
                d = abs(amostra['P'] - ac['P'])
                if d >= ΔP:
                    n0 = n
                    passo = 2
            elif passo == 2: # Espera o delta se manter pelo tempo
                d = abs(amostra['P'] - ac['P'])
                if d >= ΔP:
                    if (n - n0) >= tempo_estabilização:
                        P0 = d * p_min
                        P1 = amostra['P']
                        n0 = n
                        # Salva o início do evento
                        inicio_arranque = n - tempo_estabilização
                        passo = 3
                else:
                    passo = 1 # Volta para o passo 1 se baixou
            elif passo == 3: # Verifica se a medição caiu novamente
                d = abs(amostra['P'] - P1)
                if P0 > d:
                    if (n - n0) >= tempo_estabilização:
                        # Verifica se a carga entrou ou saiu
                        d = amostra['I'] - ac['I']
                        if d >= 0:
                            ac['ini'] = inicio_arranque;

                            # Cria aqui o degrau para comparar
                            cmp = np.full(n_before, ac['P']).tolist()
                            cmp = cmp + np.full(n_after, amostra['P']).tolist()
                            cut = chunk.P[inicio_arranque-n_before: inicio_arranque+n_after]
                            # Corta o tamanho extra caso a amostra seja pequena demais
                            max = min(len(cmp), len(cut) )
                            cut = cut.iloc[0:max]
                            cmp = cmp[0:max]
                            # Calcula a correlação de Pearson com o degrau
                            ac['pearson'] = scipy.stats.pearsonr(cmp, cut).statistic

#                            plt.figure()
#                            plt.title(filename + ", pearson: " + str(ac['pearson']) )
#                            plt.plot(cmp)
#                            plt.plot(cut.tolist() )

                            ac['P'] = amostra['P']# - ac['P']
                            ac['Q'] = amostra['Q']# - ac['Q']
                            ac['D'] = amostra['D']# - ac['D']
                            ac['I'] = amostra['I']# - ac['I']
                            ac['Ia'] = amostra['Ia']# - ac['Ia']
                            ac['Ir'] = amostra['Ir']# - ac['Ir']
                            ac['Iv'] = amostra['Iv']# - ac['Iv']
                            ac['V'] = amostra['V']
                            # Calcula o fator de potência
                            ac['fp'] = ac['Ia'] / ac['I']
                            # Calcula o fator de linearidade (versão Wesley)
                            ac['fl'] = 1 - ac['Iv'] / ac['I']
                            # Calcula o fator de reatividade (versão Wesley)
                            ac['fr'] = 1 - ac['Ir'] / ac['I']                                                        
                            ac['nome'] = filename
                            return ac
                else:
                    # Volta se a entrada se desestabiizou
                    passo = 2
            else:
                print('default')
    raise Exception("Dados inválidos em " + path)

def save_data(path, cargas):
    # Salva o resultado em um arquivo csv
    outputs = [ 'nome', 'P', 'Q', 'D', 'V', 'I', 'Ia', 'Ir', 'Iv', 'fp', 'fl', 'fr', 'pearson', 'ini' ]
    table = pd.DataFrame(columns=outputs)
    for l in cargas:
        d = {
            'nome': l['nome'],
            'P': l['P'],
            'Q': l['Q'],
            'D': l['D'],
            'V': l['V'],
            'I': l['I'],
            'Ia': l['Ia'],
            'Ir': l['Ir'],
            'Iv': l['Iv'],
            'fp': l['fp'],
            'fl': l['fl'], 
            'fr': l['fr'], 
            'pearson': l['pearson'],
            'ini': l['ini']
        }
        table = pd.concat([table, pd.DataFrame([d])], ignore_index=True)    
    table.to_csv(path, index = False)

def multi_save():
    from multiprocessing import Pool, cpu_count

    # Encontra aqui os dados para agrupar
    with Pool(cpu_count() ) as p:
        res = p.map(scan, [
            "esmeril1_d.csv",
            "ferro_de_solda1_d.csv",
            "ferro_de_solda2_d.csv",
            "ferro_de_solda3_d.csv",
            "fonte1_d.csv",
            "laptop1_d.csv",
            "laptop2_d.csv",
            "luminaria1_d.csv",
            "luminaria2_d.csv",
            "luminaria3_d.csv",
            "luminaria4_d.csv",
            "mandril1_d.csv",
            "microondas1_d.csv",
            "monitor1_d.csv",
            "monitor2_d.csv",
            "osciloscópio_d.csv",
            "qualímetro_pq13_d.csv",
            "refletor_led1_d.csv",
            "refletor_led2_d.csv",
            "soprador_térmico1_d.csv",
            "ventilador1_d.csv",
            "ventilador2_d.csv",
            "ventilador3_d.csv",
            "ventilador4_d.csv"
        ])

    cargas = []
    for i in res:
        cargas.append(i)

    save_data("cargas.csv", cargas)

    print("Terminado.")

    return cargas

if __name__ == '__main__':
    multi_save()
 