from ksql import KSQLAPI
import pandas as pd
import json


client = KSQLAPI('http://localhost:8088')
client.ksql('show streams')

query = client.query('select * from passenger2 limit 10', stream_properties={"auto.offset.reset": "earliest"})


for item in query:
   print(item)



records = [json.loads(r) for r in query]
data = [r['row']['columns'][2:] for r in records[:-1]]
#data = r['row']['columns'][2] for r in records
df = pd.DataFrame(data=data)
df.head(5)
