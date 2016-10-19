import uuid

from idaho_streamer.util import calc_toa_gain_offset

def generate_ipe_graph(idaho_id, meta, bucket="idaho-images"):
    gains_offsets = calc_toa_gain_offsets(meta)
    radiance_scales = [e[0] for e in gain_offsets]
    reflectance_scales = [e[1] for e in gain_offsets]
    radiance_offsets = [e[2] for e in gain_offsets]
    return {
      "id": uuid.uuid4(),
      "edges": [
        {
          "id": uuid.uuid4(),
          "index": 1,
          "source": "MsSourceImage",
          "destination": "MsOrtho"
        },
        {
          "id": uuid.uuid4(),
          "index": 1,
          "source": "MsOrtho",
          "destination": "RadianceGain"
        },
        {
          "id": uuid.uuid4(),
          "index": 1,
          "source": "RadianceGain",
          "destination": "RadianceOffset"
        },
        {
          "id": uuid.uuid4(),
          "index": 1,
          "source": "RadianceOffset",
          "destination": "ReflectanceScale"
        },
        {
          "id": uuid.uuid4(),
          "index": 1,
          "source": "RadianceOffset",
          "destination": "Radiance"
        },
        {
          "id": uuid.uuid4(),
          "index": 1,
          "source": "ReflectanceScale",
          "destination": "Reflectance"
        }
      ],
      "nodes": [
        {
          "id": "MsSourceImage",
          "operator": "IdahoRead",
          "parameters": {
            "bucketName": bucket,
            "imageId": image_id,
            "objectStore": "S3"
          }
        },
        {
          "id": "MsOrtho",
          "operator": "GridOrthorectify",
          "parameters": {}
        },
        {
            "id": "RadianceGain",
            "operator": "MultiplyConst",
            "parameters": {
                "constants": radiance_scales
            }
        },
        {
            "id": "RadianceOffset",
            "operator": "AddConst",
            "parameters": {
                "constants": radiance_offsets
            }
        },
        {
            "id": "ReflectanceScale",
            "operator": "MultiplyConst",
            "parameters": {
                "constants": reflectance_scales
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
