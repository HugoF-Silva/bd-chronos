import boto3

TABLE_NAME = "units"
REGION_NAME = "us-east-1"
from decimal import Decimal

dynamodb = boto3.resource("dynamodb", region_name=REGION_NAME)
table = dynamodb.Table(TABLE_NAME)

backup_items = [
    {
        "unit": {"S": "CAIS Cândida de Morais"},
        "lat": {"N": "-16.6514931"},
        "lng": {"N": "-49.3280203"},
        "postal_code": {"S": "74463-330"}
    },
    {
        "unit": {"S": "UPA Região Noroeste"},
        "lat": {"N": "-16.6121551"},
        "lng": {"N": "-49.3427299"},
        "postal_code": {"S": "74480-650"}
    },
    {
        "unit": {"S": "CIAMS Urias Magalhães"},
        "lat": {"N": "-16.635675"},
        "lng": {"N": "-49.2749015"},
        "postal_code": {"S": "74565-610"}
    },
    {
        "unit": {"S": "Cais Finsocial"},
        "lat": {"N": "-16.6179383"},
        "lng": {"N": "-49.3535845"},
        "postal_code": {"S": "74480-110"}
    },
    {
        "lat": {"S": "-16.667448"},
        "unit": {"S": "UPA Campinas"},
        "lng": {"S": "-49.277836"}
    }
]

for item in backup_items:
    unit = item['unit']['S']

    # Pull out the raw strings (either under 'N' or 'S')
    lat_str = item.get('lat', {}).get('N') or item.get('lat', {}).get('S')
    lng_str = item.get('lng', {}).get('N') or item.get('lng', {}).get('S')

    # Wrap in Decimal instead of float()
    lat = Decimal(lat_str) if lat_str is not None else None
    lng = Decimal(lng_str) if lng_str is not None else None

    postal_code = item.get('postal_code', {}).get('S')
    address     = item.get('address', {}).get('S')

    item_to_put = {
        "unit": unit,
        "lat":  lat,
        "lng":  lng
    }
    if postal_code:
        item_to_put["postal_code"] = postal_code
    if address:
        item_to_put["address"] = address

    table.put_item(Item=item_to_put)

print("All items inserted.")
