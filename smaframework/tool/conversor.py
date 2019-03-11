import numpy as np
import smaframework.tool.constants as Constants

def kmph2mps(speed):
    '''
     * Coverts a speed from Km/h to m/s.
     *
     * @param   speed    the speed to convert.
     * @return  float       the converted speed.
    '''
    return speed / 3.6

def deg2rad(degree):
    '''
     * Coverts an angle from Deg to Rad.
     *
     * @param   degree  the angle to convert.
     * @return  float   the converted angle.
    '''
    rad = degree * 2 * np.pi / 360
    return rad

def meters2geodist(x, merge=True, lat=0):
    '''
     * Coverts an distance in meters to geo-coordinates.
     *
     * @param   x       the distance to convert.
     * @param   merge   whether lat and long distances are meant to be merged (as the maximum) or return separately.
     * @param   lat     the assumed reference latitude in degrees (default 0 deg).
     * @return  float   the converted distance.
    '''
    R = Constants.earth_radius
    
    dLat = x / R * 180 / np.pi
    dLon = x / (R * np.cos(np.pi * lat / 180)) * 180 / np.pi

    # print(dLat, dLon)

    if merge:
        return max([dLat, dLon])
    return (dLat, dLon)