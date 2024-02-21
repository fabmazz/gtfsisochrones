# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import numpy as np
import numba as nb
import rustworkx as rx
import polars as pl
import pandas as pd

## build the graph using rustworkx
def build_graph_rx(nodes_df,edgelist):
    nrx = rx.PyGraph()

    nodes_i = {}
    for n in nodes_df["i"].values:
        #n=r.i
        ii=nrx.add_node(n)
        nodes_i[n]=ii
    
    nn=nodes_df.set_index("i")
    nn["id_g"] = pd.Series(nodes_i)
    nodes_df = nn.reset_index()

    for i,j,d in zip(edgelist.i,edgelist.j, edgelist.dist):
        ii =nodes_i[i]
        jj=nodes_i[j]
        nrx.add_edge(ii,jj,{"d":d})
    
    return nrx, nodes_df, nodes_i

def add_stops_nodes_rx(nrx: rx.PyGraph, stops_ids, dists, inn, maxdist=1000):
    stop_netid=dict()
    remove_stop_ids = []
    nnodes_=0
    nedges_=0
    for i, sid in enumerate(stops_ids):
        if np.all(dists[i]>maxdist):
            remove_stop_ids.append(sid)
            continue
        
        ist = nrx.add_node(f"s{sid}")
        nnodes_+=1
        stop_netid[sid]=ist
        for di, k in zip(dists[i],inn[i]):
            nrx.add_edge(ist,k,{"d":di})
            nedges_+=1

    return stop_netid, remove_stop_ids, nnodes_, nedges_

def adjust_stops_graph(stops, stop_netid, remove_stop_ids):
    ss=stops.set_index("stop_id")
    ss=ss.drop(remove_stop_ids,)
    ss["graph_i"] = pd.Series(stop_netid, dtype=np.int_)
    return ss.reset_index()

def cost(x):
    return x["d"]
## unused
def find_distance_point_all(nrx, i_net,nnodes):
    dist_rr = rx.dijkstra_shortest_path_lengths(nrx,i_net,cost)
    dists=np.fromiter((dist_rr[k] if k in dist_rr else 0.0 for k in range(nnodes)),np.float_)
    assert dists[i_net] == 0
    return dists#*3.6/speed_foot

@nb.jit(nopython=False)
def _check_dist(timedist, dist_rr, start_time, change,nnodes, speed_foot):
    for i in range(nnodes):
        d=dist_rr[i] if i in dist_rr else 0.0
        ntime = start_time + d*3.6/speed_foot
        if ntime < timedist[i]:
            timedist[i] = ntime
            change[0]+=1


def update_timedist_all(timedist, nrx, i_net,start_time_secs,nnodes,speed_foot_kmh, cache=None):

    hit = False
    if cache is not None:
        if i_net in cache:
            dist_rr = cache[i_net]
            hit = True
    if not hit:
        dist_rr = rx.dijkstra_shortest_path_lengths(nrx,i_net,cost)

    change = np.zeros(nnodes,dtype=np.bool_)#[0]
    #_check_dist(timedist, dist_rr, start_time, change, nnodes,speed_foot)
    for i in range(nnodes):
        d=dist_rr[i] if i in dist_rr else 0.0
        ntime = start_time_secs + d*3.6/speed_foot_kmh
        if ntime < timedist[i]:
            timedist[i] = ntime
            change[i] = True
    
    return change #dists*3.6/speed_foot

@nb.njit()
def update_timediff_with_dist(timedist, dist_vec,start_time_secs,nnodes,speed_foot_kmh):

    change = np.zeros(nnodes,dtype=np.bool_)#[0]
    #_check_dist(timedist, dist_rr, start_time, change, nnodes,speed_foot)
    for i in range(nnodes):
        d=dist_vec[i] #if i in dist_rr else 0.0
        ntime = start_time_secs + d*3.6/speed_foot_kmh
        if ntime < timedist[i]:
            timedist[i] = ntime
            change[i] = True
    
    return change #dists*3.6/speed_foot


def trips_from_stop_time(stoptimes, tstart_stop, final_time, stopid):
    ## stoptimes has already been sorted by departure time
    return stoptimes.lazy().filter((pl.col("departure_time")> str(tstart_stop)) &(pl.col("departure_time")<str(final_time)) & (pl.col("stop_id")==stopid)
                       ).group_by("trip_id").first().select(["trip_id","stop_id","departure_time","stop_sequence"]).sort("trip_id").collect()