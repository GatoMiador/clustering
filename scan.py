#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 08:34:12 2022

@author: marcos
"""

import pandas as pd

class Carga:
    nome = 'nada'
    P = 0 
    Q = 0 
    D = 0 
    V = 0 
    I = 0 
    Ia = 0
    Ir = 0
    Iv = 0
    fp = 0 
    fl = 0
    fr = 0
    pearson = 0

    # Calcula os fatores do CPT
    def calc_factors(self):
        # Calcula o fator de potência
        self.fp = self.Ia / self.I
        # Calcula o fator de linearidade (versão Wesley)
        self.fl = 1 - self.Iv / self.I
        # Calcula o fator de reatividade (versão Wesley)
        self.fr = 1 - self.Ir / self.I

def scan(path):
    import os
    import numpy as np
    import scipy.stats

    # Potência a se manter para detectar a carga
    P0 = 0
    # Variação mínima de potência a considerar
    delta = 2.5
    # Passo do detector
    step = 0;
    # Indice da amostra analisada
    n = 0
    # Quantidade de amostras por ciclo
    cycles = 1024
    # Frequencia da rede
    freq = 60

    # Auxiliar do indice
    n0 = 0
    # Indice do início do evento
    start_event = 0

    # Tempo para considerar o sinal estável, isso remove o "spike" inicial de algumas cargas
    velocidade = cycles*2

    # Ciclos a capturar antes do evento
    n_before = cycles * 2
    # Ciclos a capturar depois do evento
    n_after = 5 * freq * cycles

    # Porcentagem da potência a manter para considerar que o circuito ativou
    p_min = 0.9

    # Ciclos a ignorar no início
    ign = (cycles*2)-1

    ac = Carga()

    nomes = [ 'P', 'Q', 'D', 'V', 'I', 'Ia', 'Ir', 'Iv' ]
    filename = os.path.splitext(os.path.basename(path) )[0]
    filename = filename[:len(filename)-2]

    print("Analisando " + filename + "...")

    for chunk in pd.read_csv(path, \
                             usecols=nomes, \
                             chunksize=300*1024*1024):
        for n, row in chunk.iterrows():
            if step == 0: # Espera pela estabilização dos dados
                if n >= ign:
                    # Seta o patamar inicial                    
                    ac.P = row['P']
                    ac.Q = row['Q']
                    ac.D = row['D']
                    ac.V = row['V']
                    ac.I = row['I']
                    ac.Ia = row['Ia']
                    ac.Ir = row['Ir']
                    ac.Iv = row['Iv']
                    step = 1
            elif step == 1: # Verifica se a variação é maior que o threshold
                d = abs(row['P'] - ac.P)
                if d >= delta:
                    n0 = n
                    step = 2
            elif step == 2: # Espera o delta se manter pelo tempo
                # Só classifica se a corrente se mantém acima do delta pelo tempo
                d = abs(row['P'] - ac.P)
                if d >= delta:
                    if (n - n0) >= velocidade:
                        P0 = d * p_min
                        P1 = row['P']                        
                        n0 = n
                        start_event = n - velocidade
                        step = 3
                else:
                    step = 1 # Volta para o passo 1 se baixou
            elif step == 3: # Espera estabilizar
                # Verifica se estabilizou
                d = abs(row['P'] - P1)
                if P0 > d:
                    if (n - n0) >= velocidade:
                        # Verifica se a carga entrou ou saiu
                        d = row['I'] - ac.I
                        if d >= 0:
                            # Faz aqui se a carga entrou                            
                            l = Carga()
                            l.ini = start_event;
                            l.P = row['P']# - ac.P
                            l.Q = row['Q']# - ac.Q
                            l.D = row['D']# - ac.D
                            l.I = row['I']# - ac.I
                            l.Ia = row['Ia']# - ac.Ia
                            l.Ir = row['Ir']# - ac.Ir
                            l.Iv = row['Iv']# - ac.Iv
                            l.V = row['V']
                            l.calc_factors()
                            l.nome = filename
                            # Cria aqui o degrau para comparar
                            # TODO - O degrau pode precisar de uma rampa de 1 ciclo, adicionar se necessário
                            cmp = np.full(n_before, ac.P).tolist()
                            cmp = cmp + np.full(n_after, row['P']).tolist()
                            cut = chunk.P[start_event-n_before: start_event+n_after]
                            # Corta o tamanho extra caso a amostra seja pequena demais
                            max = min(len(cmp), len(cut) )
                            cut = cut.iloc[0:max]
                            cmp = cmp[0:max]
                            # Calcula a correlação de Pearson com o degrau
                            l.pearson = scipy.stats.pearsonr(cmp, cut).statistic
                            return l
                else:
                    # Volta se a entrada se desestabiizou
                    step = 2
            else:
                print('default')
    raise Exception("Dados inválidos em " + path)

def save_data(path, cargas):
    # Salva o resultado em um arquivo csv
    outputs = [ 'nome', 'P', 'Q', 'D', 'V', 'I', 'Ia', 'Ir', 'Iv', 'fp', 'fl', 'fr', 'pearson', 'ini' ]
    table = pd.DataFrame(columns=outputs)
    for l in cargas:
        d = {
            'nome': l.nome, 
            'P': l.P,
            'Q': l.Q,
            'D': l.D,
            'V': l.V,
            'I': l.I,
            'Ia': l.Ia,
            'Ir': l.Ir,
            'Iv': l.Iv,
            'fp': l.fp,
            'fl': l.fl, 
            'fr': l.fr, 
            'pearson': l.pearson,
            'ini': l.ini
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

    p.close()
    p.join()

    cargas = []
    for i in res:
        cargas.append(i)

    save_data("cargas.csv", cargas)

    print("Terminado.")

    return cargas

if __name__ == '__main__':
    multi_save()
 