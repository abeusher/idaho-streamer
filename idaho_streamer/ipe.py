import uuid
import json

from idaho_streamer.util import calc_toa_gain_offset

VIRTUAL_IPE_URL = "http://virtualidaho-env.us-east-1.elasticbeanstalk.com/v1"

def generate_ipe_graph(idaho_id, meta, bucket="idaho-images"):
    gains_offsets = calc_toa_gain_offset(meta)
    radiance_scales = [e[0] for e in gains_offsets]
    reflectance_scales = [e[1] for e in gains_offsets]
    radiance_offsets = [e[2] for e in gains_offsets]
    return {
      "id": str(uuid.uuid4()),
      "edges": [
        {
          "id": str(uuid.uuid4()),
          "index": 1,
          "source": "MsSourceImage",
          "destination": "MsFloatImage"
        },
        {
            "id": str(uuid.uuid4()),
            "index": 1,
            "source": "MsFloatImage",
            "destination": "RadianceGain"
        },
        {
          "id": str(uuid.uuid4()),
          "index": 1,
          "source": "RadianceGain",
          "destination": "TOARadiance"
        },
        {
          "id": str(uuid.uuid4()),
          "index": 1,
          "source": "TOAReflectance",
          "destination": "TOAReflectance"
        }
      ],
      "nodes": [
        {
          "id": "MsSourceImage",
          "operator": "IdahoRead",
          "parameters": {
            "bucketName": bucket,
            "imageId": idaho_id,
            "objectStore": "S3"
          }
        },
        {
            "id": "MsFloatImage",
            "operator": "Format",
            "parameters": {
                "dataType": "TYPE_FLOAT"
            }
        },
        {
            "id": "RadianceGain",
            "operator": "MultiplyConst",
            "parameters": {
                "constants": json.dumps(radiance_scales)
            }
        },
        {
            "id": "TOARadiance",
            "operator": "AddConst",
            "parameters": {
                "constants": json.dumps(radiance_offsets)
            }
        },
        {
            "id": "TOAReflectance",
            "operator": "MultiplyConst",
            "parameters": {
                "constants": json.dumps(reflectance_scales)
            }
        }
      ]
    }
