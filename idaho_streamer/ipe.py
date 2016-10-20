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
          "destination": "RadianceGain"
        },
        {
          "id": str(uuid.uuid4()),
          "index": 1,
          "source": "RadianceGain",
          "destination": "RadianceOffset"
        },
        {
          "id": str(uuid.uuid4()),
          "index": 1,
          "source": "RadianceOffset",
          "destination": "ReflectanceScale"
        },
        {
          "id":  str(uuid.uuid4()),
          "index": 1,
          "source": "RadianceOffset",
          "destination": "TOARadiance"
        },
        {
          "id":  str(uuid.uuid4()),
          "index": 1,
          "source": "ReflectanceScale",
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
            "id": "RadianceGain",
            "operator": "MultiplyConst",
            "parameters": {
                "constants": json.dumps(radiance_scales)
            }
        },
        {
            "id": "RadianceOffset",
            "operator": "AddConst",
            "parameters": {
                "constants": json.dumps(radiance_offsets)
            }
        },
        {
            "id": "ReflectanceScale",
            "operator": "MultiplyConst",
            "parameters": {
                "constants": json.dumps(reflectance_scales)
            }
        },
        {
            "id": "TOARadiance",
            "operator": "Format",
            "parameters": {
                "dataType": "TYPE_FLOAT"
            }
        },
        {
            "id": "TOAReflectance",
            "operator": "Format",
            "parameters": {
                "dataType": "TYPE_FLOAT"
            }
        },

      ]
    }
