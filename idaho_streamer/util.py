from twisted.internet import reactor, defer
import pvl
import geoio.constants as constants

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
