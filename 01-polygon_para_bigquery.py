from polygon import RESTClient
from datetime import datetime, timedelta
import pandas as pd
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from google.cloud import bigquery

# Configurar a autenticação do Google Cloud
SCOPES = ['https://www.googleapis.com/auth/cloud-platform']

credentials_info = {
  "type": "service_account",
  "project_id": "swift-climate-411900",
  "private_key_id": "06c41e4952cc58828bdfeb84ee0697537141472d",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC7UEoEvNBn8IsJ\ni9ykIJDb8MyyO9n77jDgCH6EcmL374qDEYARcCS/gh9hi6SMrqilD5c0Z9q/ayDM\nS5VJZFAoxSxKL1ErH97cMmAtEMpygcaYoiBn/6zzz4mQaQUsiJX7dqIbbjgR/RHp\nm1/xkh51eFGN+BdDUmK13js7DKISmnmGpueCi3Qf6swaCM8O1m+E2wzrCpX8HHA1\ngXoKtpZwUXtGfnHkaFSBG2BxzHxJP07igQVO22fISLtIar9tKCp3HYSHUiOrYauA\nu5b5uOvsdHkBqt5DoKkk4b1svTL2HywQRDWzFkv+u/E5T9z3r61pO20sqxw6Nj+a\nrlm3V617AgMBAAECggEAKqwF0MJ679LaudgE8dcBaTmYSFpeNIh01fTZba7pYPZP\nJcDM3iwgEwi/wWcgm3QGs2Oz3Jp0OPtcw23MmqWhpUgV6OiSozJlgOBxDJMwq5lo\n9siofUi/y+NRwXZLWXPcRyclkv4vA9oVRJTC7LOPAM/iNGd6VTnlhu0TrUYZfgcT\nNahkbdfb6EfHM/MUHknmEwGuhAq1Y8Nhxp1isPrOlxx0vl3Wyk6wOujfNFT/7OfU\nKgise7iiNKK28FYCDWeBaaXFlPj28WqyJLqq15xpOHA6KzN+E+QzvxT66BU0wE1/\n9Wt763cJKO7mOVf+A0FQPaRtYqKfh6euFEo8FVlLwQKBgQDconH/iS2twjhC7OlT\nPhk7STVN6KBgmRYXbEoyf+gZsB3joxGv1f6bHTKuvqpCYL2Q6fSkGOFj+Vlc9PQ2\nbegQWDHapnqSn6tBigrscEqhzddd2oigdLnkUgO+FV0mcZMbVhLJJXIw+wp0WgYu\nUHMsAu5/wJNC7gM3WzdWcze1sQKBgQDZVowuaJvrMddxwjEBo5MNH38snaPUxjjn\nNNfIyaQBVJItCl0Q7bS/tQc2maZ0/1DsnLsLTJdR32FMX2GBs+GTU/yDzMVkffv3\n6etH54KlQb1U2Bflr0/8syrq6yS1Am8MdYwbZh5ijQmIhx2pHFNRtyceVzVWmdGb\n+/hcZtck6wKBgEeHhsvgrmV55QGViyOIq2d0GYrzkyMeHnJjkj6DBz1kwpvtXyuR\nhiTFt4u9lrdEY9DaeIzG4DOoQFeJtq76vNSnsyn+9RgaGcx6s4Xp9dg1QtBTrB3R\nGf8ys7HpfTScd6PSKO77a+UDTmVgVkanoF8xaB8U0OlO/s3wjaVCX1pRAoGAfyVG\ntQ+1x45M/9wfV1oeeRroB23hnmSofXpIksqEC9MyAO+DKpglmdGJ+sNAwklrSkEW\nm7GfBOKtxUQ3gu19FfeYTnLJN9UMRyit4E7r+0nOPYh90n0RSkB25x/RRaO624sZ\nAB5pwDXKUfjZvUk45SFE3VcfeR5bpelujoALdSkCgYAO5sALTFIYOlD/Gl3j3Gml\nU2qTteVMcFVPItv2k8Gw4txeZPIyNTR44PKPKj7hWapJNKiwsZvSkXVktwR+VmyF\nRgUdTgmAUFtJ+q1tZif+YVNh7zsNZLPHUCojqxVp9rrj57sGT/FzK5FQrQiBScbQkzpu2zfBqkUgW9Ukojug9g==\n-----END PRIVATE KEY-----\n",
  "client_email": "koru-466@swift-climate-411900.iam.gserviceaccount.com",
  "client_id": "115052303438197562319",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/koru-466%40swift-climate-411900.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

# Criar credenciais usando o google-auth library
credentials = service_account.Credentials.from_service_account_info(
    credentials_info, scopes=SCOPES
)

# Atualiza as credenciais se expirar
try:
    credentials.refresh(Request())
except RefreshError as e:
    print(f"Error refreshing credentials: {e}")

# Dados do Dataset da Koru
PROJECT_ID = 'swift-climate-411900'
DATASET_ID = 'indices'
TABLE_ID = 'euro'

# Usando a API da Polygon
client = RESTClient('4HntDYSlAIfaSCgx_EAYVBb24ijvMpdu')

today = datetime.today()
start_date = today - timedelta(days=10)

today_str = today.strftime('%Y-%m-%d')
start_date_str = start_date.strftime('%Y-%m-%d')

aggs_with_date = []
for agg in client.list_aggs(
    "C:EURUSD",
    1,
    "day",
    start_date_str,
    today_str,
    limit=50000,
):
    date_object = datetime.utcfromtimestamp(agg.timestamp/1000)
    formatted_date = date_object.strftime('%d/%m/%Y')

    agg_with_date = {
        "date": formatted_date,
        "open": agg.open,
        "high": agg.high,
        "low": agg.low,
        "close": agg.close,
        "volume": agg.volume
    }
    aggs_with_date.append(agg_with_date)

if aggs_with_date:
    df = pd.DataFrame(aggs_with_date)

    # Selecionando apenas a última linha
    last_row = df.iloc[-1:]

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Enviar para o BigQuery
    last_row.to_gbq(destination_table=table_id, project_id=PROJECT_ID, if_exists='append', credentials=credentials)
    print("Dados enviados para o BigQuery com sucesso.")
else:
    print("Não há dados disponíveis para exibir.")
 