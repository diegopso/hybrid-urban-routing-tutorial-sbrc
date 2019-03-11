import time, math
import numpy as np
import pandas as pd
import pylab as pl
import scipy

def get_region(df, angle_step=5, **kwargs):
    df = df.copy()

    min_lat = df['lat'].min()
    max_lat = df['lat'].max()
    min_lon = df['lon'].min()
    max_lon = df['lon'].max()

    origin = ((max_lat - min_lat) / 2 + min_lat, (max_lon - min_lon) / 2 + min_lon)

    df['teta'] =  np.arctan2((df['lon'] - origin[1]), (df['lat'] - origin[0]))
    df['r'] = (df['lat'] - origin[0]) * np.cos(df['teta'])
    df['teta'] = np.round(df['teta'] * 180 / math.pi / angle_step) * angle_step

    df = df[df['r'] == df.groupby('teta')['r'].transform(max)]
    df = df.groupby('teta').max().reset_index()

    if pd.__version__ >= '0.17.0':
        return df[['lat', 'lon', 'teta']].sort_values(by='teta')
    else:
        return df[['lat', 'lon', 'teta']].sort('teta')

def _angle_to_point(point, centre):
    '''calculate angle in 2-D between points and x axis'''
    delta = point - centre
    res = np.arctan(delta[1] / delta[0])
    if delta[0] < 0:
        res += np.pi
    return res

def _draw_triangle(p1, p2, p3, **kwargs):
    tmp = np.vstack((p1,p2,p3))
    x,y = [x[0] for x in zip(tmp.transpose())]
    pl.fill(x,y, **kwargs)

def area_of_triangle(p1, p2, p3):
    '''calculate area of any triangle given co-ordinates of the corners'''
    return np.linalg.norm(np.cross((p2 - p1), (p3 - p1)))/2.


def convex_hull(points, graphic=False, smidgen=0.0075):
    '''
    Calculate subset of points that make a convex hull around points
    Recursively eliminates points that lie inside two neighbouring points until only convex hull is remaining.

    :Parameters:
    points : ndarray (2 x m)
    array of points for which to find hull
    graphic : bool
    use pylab to show progress?
    smidgen : float
    offset for graphic number labels - useful values depend on your data range

    :Returns:
    hull_points : ndarray (2 x n)
    convex hull surrounding points
    '''

    points = points[['lat', 'lon']].as_matrix().T

    if graphic:
        pl.clf()
        pl.plot(points[0], points[1], 'ro')
    n_pts = points.shape[1]
    assert(n_pts > 5)
    centre = points.mean(1)
    if graphic: pl.plot((centre[0],),(centre[1],),'bo')
    angles = np.apply_along_axis(_angle_to_point, 0, points, centre)
    pts_ord = points[:,angles.argsort()]
    if graphic:
        for i in xrange(n_pts):
            pl.text(pts_ord[0,i] + smidgen, pts_ord[1,i] + smidgen, \
                   '%d' % i)
    pts = [x[0] for x in zip(pts_ord.transpose())]
    prev_pts = len(pts) + 1
    k = 0
    while prev_pts > n_pts:
        prev_pts = n_pts
        n_pts = len(pts)
        if graphic: pl.gca().patches = []
        i = -2
        while i < (n_pts - 2):
            Aij = area_of_triangle(centre, pts[i],     pts[(i + 1) % n_pts])
            Ajk = area_of_triangle(centre, pts[(i + 1) % n_pts], \
                                   pts[(i + 2) % n_pts])
            Aik = area_of_triangle(centre, pts[i],     pts[(i + 2) % n_pts])
            if graphic:
                _draw_triangle(centre, pts[i], pts[(i + 1) % n_pts], \
                               facecolor='blue', alpha = 0.2)
                _draw_triangle(centre, pts[(i + 1) % n_pts], \
                               pts[(i + 2) % n_pts], \
                               facecolor='green', alpha = 0.2)
                _draw_triangle(centre, pts[i], pts[(i + 2) % n_pts], \
                               facecolor='red', alpha = 0.2)
            if Aij + Ajk < Aik:
                if graphic: pl.plot((pts[i + 1][0],),(pts[i + 1][1],),'go')
                del pts[i+1]
            i += 1
            n_pts = len(pts)
        k += 1

    df = pd.DataFrame(np.asarray(pts))
    df.columns = ['lat', 'lon']
    return df

def smoother_region(points):
    x, y = np.array(points['lat'].tolist()), np.array(points['lon'].tolist())

    nt = np.linspace(0, 1, 100)
    t = np.zeros(x.shape)
    t[1:] = np.sqrt((x[1:] - x[:-1])**2 + (y[1:] - y[:-1])**2)
    t = np.cumsum(t)
    t /= t[-1]
    x2 = scipy.interpolate.spline(t, x, nt)
    y2 = scipy.interpolate.spline(t, y, nt)

    return pd.DataFrame({'lat': x2, 'lon': y2})