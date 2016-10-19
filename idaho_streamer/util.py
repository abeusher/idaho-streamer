from twisted.internet import reactor, defer
import pvl
import geoio.constants as constants
import requests
import numpy as np
import ephem
from bson.json_util import loads as json_loads


def sleep(seconds):
    d = defer.Deferred()
    reactor.callLater(seconds, d.callback, seconds)
    return d


def extract_idaho_metadata(imd, til):
    meta = {}

    til_md = dict(dict(pvl.loads(til))['TILE_1'][0])
    md = dict(pvl.loads(imd))
    image_md = dict(md['IMAGE_1'][0])

    meta['bbox'] = [til_md['LLLon'][0], til_md['LLLat'][0], til_md['URLon'][0], til_md['URLat'][0]]
    #print image_md
    meta['satid'] = str(image_md['satId'][0])
    meta['bandid'] = str(md['bandId'][0])
    meta['catid'] = str(image_md['CatId'][0])
    meta['cloud_cover'] = image_md['cloudCover'][0]
    meta['off_nadir'] = image_md['meanOffNadirViewAngle'][0]
    meta['product_level'] = str(md['productLevel'][0])
    meta['img_datetime_obj_utc'] = image_md['firstLineTime'][0]
    meta['mean_sun_el'] = image_md['meanSunEl'][0]

    abscalfactor = []
    effbandwidth = []
    tdilevel = []
    lat = []
    lon = []
    hae = []

    for x in md.keys():
        if 'BAND_' in x:
            band = dict(md[x][0])
            abscalfactor.append(float(band['absCalFactor'][0]))
            effbandwidth.append(float(band['effectiveBandwidth'][0]))
            tdilevel.append(float(band['TDILevel'][0]))
            lat.append(float(band['ULLat'][0]))
            lon.append(float(band['ULLon'][0]))
            hae.append(float(band['ULHAE'][0]))

        meta['abscalfactor'] = abscalfactor
        meta['effbandwidth'] = effbandwidth
        meta['tdilevel'] = tdilevel

    sat_index = meta['satid'].upper() + "_" + \
                meta['bandid'].upper()
    meta['band_names'] = constants.DG_BAND_NAMES[sat_index]
    meta['band_centers'] = constants.DG_WEIGHTED_BAND_CENTERS[sat_index]
    meta['latlonhae'] = (lat[0], lon[0], hae[0])


    return meta


def calc_toa_gain_offset(meta):
    """
    Compute (gain, offset) tuples for each band of the specified image metadata
    """

    # Set satellite index to look up cal factors
    sat_index = meta['satid'].upper() + "_" + \
                meta['bandid'].upper()

    # Set scale for at sensor radiance
    # Eq is:
    # L = GAIN * DN * (ACF/EBW) + Offset
    # ACF abscal factor from meta data
    # EBW effectiveBandwidth from meta data
    # Gain provided by abscal from const
    # Offset provided by abscal from const
    acf = np.asarray(meta['abscalfactor']) # Should be nbands length
    ebw = np.asarray(meta['effbandwidth'])  # Should be nbands length
    gain = np.asarray(constants.DG_ABSCAL_GAIN[sat_index])
    scale = (acf/ebw)*(gain)
    offset = np.asarray(constants.DG_ABSCAL_OFFSET[sat_index])

    e_sun_index = meta['satid'].upper() + "_" + \
                  meta['bandid'].upper()
    e_sun = np.asarray(constants.DG_ESUN[e_sun_index])
    sun = ephem.Sun()
    img_obs = ephem.Observer()
    img_obs.lon = meta['latlonhae'][1]
    img_obs.lat = meta['latlonhae'][0]
    img_obs.elevation = meta['latlonhae'][2]
    img_obs.date = meta['img_datetime_obj_utc']
    sun.compute(img_obs)
    d_es = sun.earth_distance

    ## Pull sun elevation from the image metadata
    #theta_s can be zenith or elevation - the calc below will us either
    # a cos or s in respectively
    #theta_s = float(self.meta_dg.IMD.IMAGE.MEANSUNEL)
    theta_s = 90-float(meta['mean_sun_el'])
    scale2 = (d_es ** 2 * np.pi) / (e_sun * np.cos(np.deg2rad(theta_s)))

    offset = offset * scale2

    # Return scaled data
    # Radiance = Scale * Image + offset, Reflectance = Radiance * Scale2
    return zip(scale, scale2, offset)
