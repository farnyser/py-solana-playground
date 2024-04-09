import os

from solana.rpc.api import Client, Pubkey
from solders.signature import Signature
import pandas as pd

# List of tx
df = pd.read_csv('/home/scawf/Downloads/Logs-data-2024-04-09 09 20 05.csv',header=0)
df['tx'] = df.apply(lambda x: x['message'].split('Ok(')[1].split(')')[0], axis=1)
txs = list(df['tx'])

def extract_ix_cu(log):
    ix = None
    for x in log:
        if 'Program log: Instruction: ' in x:
            ix = x.split('Instruction: ')[1].split(' ')[0]
        elif 'consumed ' in x:
            cu = x.split('consumed ')[1].split(' ')[0]
            yield (ix, cu)


solana_client = Client(os.environ['RPC_URL'])

res = {'ix': [], 'cu': []}
for tx in txs:
    sig = Signature.from_string(tx)
    try:
        tx = solana_client.get_transaction(sig,'jsonParsed', max_supported_transaction_version=0)
        log = tx.value.transaction.meta.log_messages

        for x in extract_ix_cu(log):
            (ix, cu) = x
            res['ix'] += [ix]
            res['cu'] += [cu]
    except Exception as e:
        print("Error: ", e)

resf = pd.DataFrame.from_dict(res)