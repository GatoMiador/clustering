#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  2 11:27:28 2022

@author: marcos
"""

import pandas as pd
from sklearn.cluster import KMeans
from sklearn.cluster import DBSCAN

table = pd.read_csv("cargas.csv")
data = table[['fp', 'fl', 'pearson']];

# Processa com o Kmeans aqui com detecção automática de grupo
k_max = 10
lst_sse = []

# Encontra os SSEs
for k in range(1, k_max+1):   # Ordered Muliple Runs
    lst_sse.append(KMeans(n_clusters=k).fit(data).inertia_)

def processa_joelho(s):
    res = [ 0 ] # O zero aqui é porque o algoritmo não processa o primeiro dado
    for k in range(1, len(s)-1):
        res.append(abs((s[k-1] - s[k])/(s[k]-s[k+1]))) 
    return res

# Processa o "joelho"
lst_sse_peak = processa_joelho(lst_sse)

# Encontra o grupo ótimo
best_k=lst_sse_peak.index(max(lst_sse_peak))+1

table['K-Means'] = KMeans(n_clusters=best_k).fit_predict(data)

# Processa com o DBSCAN aqui
minPts = len(data.columns) + 1
clustering = DBSCAN(eps=0.33, min_samples=len(data.columns) + 1).fit(data)

table['DBSCAN'] = clustering.labels_
