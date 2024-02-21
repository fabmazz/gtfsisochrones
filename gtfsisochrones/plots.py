# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import matplotlib.tri as tri
import numpy as np

def interpolate_grid(xsel,ysel, ngrid):
    xi = np.linspace(min(xsel), max(xsel), ngrid)
    yi = np.linspace(min(ysel), max(ysel), ngrid)
    triang = tri.Triangulation(xsel, ysel)
    Xi, Yi = np.meshgrid(xi, yi)
    return xi, yi, {"triang":triang, "xi":xi, "yi":yi,"xmesh":Xi,"ymesh":Yi}


def contourf_interp(xi, yi, z, ax1,fig,interpol_data, colorbar=True,**kwargs):

    triang = interpol_data["triang"]
    
    # Linearly interpolate the data (x, y) on a grid defined by (xi, yi).
    #xi = interpol_data["xi"]
    #yi = interpol_data["yi"]
    interpolator = tri.LinearTriInterpolator(triang, z)
    zi = interpolator(interpol_data["xmesh"], interpol_data["ymesh"])
    # Note that scipy.interpolate provides means to interpolate data on a grid
    # as well. The following would be an alternative to the four lines above:
    # from scipy.interpolate import griddata
    # zi = griddata((x, y), z, (xi[None, :], yi[:, None]), method='linear')
    if "vmax" in kwargs:
        z2= np.ma.masked_greater(zi,kwargs["vmax"])
    else:
        z2 = zi
    #ax1.contour(xi, yi, z2, levels=14, linewidths=0.5, colors='k',**kwargs)
    cntr1 = ax1.contourf(xi, yi, z2,**kwargs)
    
    if colorbar:
        fig.colorbar(cntr1, ax=ax1)
    #ax1.plot(x, y, 'ko', ms=3)
    

def find_lims_time(xweb, yweb, timetaken,max_time):
    #all_dist_min = {k: d/60 for k,d in timedists.items()}
    
    #r=np.stack([d<max_time for d in all_dist_min.values()])
    
    #select= np.any(r,axis=0)
    select = (timetaken<=max_time)
    
    xsel = xweb[select]
    ysel = yweb[select]

    
    m = (xweb >= xsel.min()) & (xweb <=xsel.max()) & (yweb >= ysel.min()) & (yweb <=ysel.max())
    print(sum(m))
    return m