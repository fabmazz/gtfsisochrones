# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

from shapely import MultiPolygon,Point,Polygon
from shapely.ops import transform
import numpy as np
import geopandas as gpd
import polars as pl
import pyproj

from concave_hull import concave_hull, concave_hull_indexes
from . import graph
from .gtfstime import add_time, GtfsTime
from .priority_queue import PriorityQueue

WEB_MERCATOR="EPSG:3857"
LATLNG="EPSG:4326"

crs_conv = pyproj.Transformer.from_crs(LATLNG,WEB_MERCATOR,always_xy=True)
crs_back_conv = pyproj.Transformer.from_crs(WEB_MERCATOR, LATLNG,always_xy=True)

def make_layers_isochrone(timetaken,nodes_all, minutes_thr,multipolygon=False, concavity=2):
    mask=timetaken<=(60*minutes_thr)
    l=nodes_all[(mask)&(~nodes_all.is_stop)]

    pp=gpd.GeoDataFrame(l,geometry=gpd.points_from_xy(l.x,l.y),crs=LATLNG)

    ps=pp.to_crs(WEB_MERCATOR)#.buffer(3).to_crs(LATLNG)
    points_mk= np.array([(p.x,p.y) for p in ps["geometry"]])


    idcs=concave_hull_indexes(points_mk,concavity=3.0)
    hpoints=concave_hull(points_mk, length_threshold=0,concavity=concavity)
    pps=MultiPolygon([Polygon(hpoints)]) if multipolygon else Polygon(hpoints)

    boundary_cc=gpd.GeoDataFrame(dict(mins=[minutes_thr]),geometry=[pps],crs=WEB_MERCATOR).to_crs(LATLNG)
    return boundary_cc

def find_stops_reachable_distance(timetaken, max_time_secs, stop_id_gid):
    nodes_reach=np.where(timetaken<max_time_secs)[0]
    m=np.isin(stop_id_gid[1],nodes_reach)
    gr_ids = stop_id_gid[1][m]
    ## distances of the stops
    dists = timetaken[gr_ids]
    return stop_id_gid[0][m], dists


def run_isochrone_algo(START_POINT, START_TIME, MAX_TIME_dt, nrx, kdtree, stoptimes_today, data, verbose=False):

    SPEED_FOOT_KMH = data["speed_foot_kmh"]
    DELAY_STOPS = data["delay_stop"]
    DELAY_METRO = data["delay_metro_stop"]
    NNODES = data["num_nodes_graph"]
    stop_id_gid = data["stop_id_gid"]
    stops_metro =  data["stops_metro"]
    gid_for_stop = dict(zip(*stop_id_gid))
    distances_graph_sid = data["distances_g"]
    dist_idx_sid = data["distances_idx_sid"]
    max_trips = data["max_num_trips"] if "max_num_trips" in data else 10
    #distidx_by_sid = 
    get_dist_stop = lambda x: distances_graph_sid[dist_idx_sid[x]]


    MAX_TIME_s=MAX_TIME_dt.total_seconds()
    p=crs_conv.transform(START_POINT[1],START_POINT[0])

    d,istart=kdtree.query(np.array(p))


    tottimetaken = np.full(NNODES,np.inf)
    tottimetaken[istart] = 0.
    totvehstaken = np.zeros(NNODES,dtype=np.int32)

    updated=graph.update_timedist_all(tottimetaken, nrx, istart, 0, NNODES, SPEED_FOOT_KMH,)
    stops_dists=find_stops_reachable_distance(tottimetaken, MAX_TIME_s, stop_id_gid )

    queue=PriorityQueue(*stops_dists)

    final_t = START_TIME+MAX_TIME_dt
    #diff10sec = timedelta(seconds=10)
    i=0
    limited_stops=[]

    #for stid,nid in zip(*stops_reachable):
    """
    The possibility of finding the same stop twice in the queue is handled by the PriorityQueue implementation
    """
    while len(queue)>0:
        ## queue.pop() can return None
        print(f"queue size: {len(queue):04d}",end="\r")
        sid = queue.pop()
        _= print("stopid is None") if verbose and sid is None else None
        if sid is None:
            continue
        gid = gid_for_stop[sid]
        deltat_s=tottimetaken[gid]
        nvehs = totvehstaken[gid]
        
        if deltat_s > MAX_TIME_s:
            continue

        if nvehs >= max_trips:
            limited_stops.append((sid,nvehs,"hightrips"))
            continue
        delay_s = DELAY_METRO if stops_metro[sid] else DELAY_STOPS
        
        #stopids.append(stid)
        ## routes_passing 
        tstop=add_time(START_TIME, deltat_s+delay_s)
        if stops_metro[sid] and verbose:
            print(f"METRO: {sid} {tstop}")
        
        #find trips from this stop
        tripsdf = graph.trips_from_stop_time(stoptimes_today, tstop,final_t, sid)

        dfss = []
        for trid, seq in zip(tripsdf["trip_id"],tripsdf["stop_sequence"]):
            ## find arrival times at other stops
            df1=stoptimes_today.lazy().filter((pl.col("trip_id")==trid) & (pl.col("stop_sequence")> seq)).select(["arrival_time","stop_id"]).collect()
            #ss= dict(zip(df1.stop_id,df1.arrival_time)
            dfss.append(df1)
        if len(dfss) == 0:
            ## no trips
            continue
        dfss=pl.concat(dfss)
        stops_arr=dfss.lazy().group_by("stop_id").agg(pl.col("arrival_time").min()).sort("arrival_time").collect()
        #print(f"Starting at time {str(tstop)}, stop {sid} {deltat_s}")

        for sid_new, arr_time in stops_arr.iter_rows():
            ## now have new stop -> added another trip
            gid_new = gid_for_stop[sid_new]
            # put there that we have gotten out of the bus
            #totvehstaken[gid_new] = nvehs+1
            new_nvehs = nvehs+1
            
            diff_ts = GtfsTime.from_string(arr_time).diff_second(START_TIME)
            delay_n = DELAY_METRO if stops_metro[sid_new] else DELAY_STOPS
            diff_ts += delay_n
            if diff_ts > MAX_TIME_s:
                continue
            #print(sid_new, gid_new, mti.GtfsTime.from_seconds(diff_ts), diff_ts)
            dist_from_stop=get_dist_stop(sid_new)
            updated_td, nupdated = graph.update_timediff_with_dist(tottimetaken,totvehstaken, dist_from_stop,diff_ts, new_nvehs, NNODES, SPEED_FOOT_KMH)
            if nupdated == 0:
                continue
            ## we have definetely made the trip
            totvehstaken[gid_new] = new_nvehs
            ## find idcs
            idcs_up= np.where(updated_td)[0]
            ## check which stops ids have been updated
            m=np.isin(stop_id_gid[1],idcs_up)
            ustop_ids = stop_id_gid[0][m]
            stops_tdiff =tottimetaken[ stop_id_gid[1][m] ]
            
            for sid,dist in zip(ustop_ids, stops_tdiff):
                queue.push(sid,dist)
            
            #print(f"\t -> at stop {sid_new}, updated {sum(updated_td)} nnodes, queue size: {len(queue)}")
        i+=1
    
    print("")
    print("DONE")

    return tottimetaken, totvehstaken, limited_stops