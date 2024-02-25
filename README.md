### Introdução <br>
Conforme solicitado, nesse trabalho criamos uma aplicação que se comunica com a api da Polygon.io trazendo informações de cotações/índices, enviando os dados coletados a um banco de dados do Google BigQuery.<br>

Para que esses dados fossem inseridos na base da Big Query criada, usamos a aplicação Apache Airflow, que executa o arquivo produzido em python, e a cada 20 minutos envia a uma tabela desse dataset.<br>

### Acesso ao Airflow
Usuário:
```
from polygon import RESTClient
```
Senha:
```
koru135
```


### API Polygon.io
Para utilização da API precisamos de um import:
```
from polygon import RESTClient
```
E também chamar a id da api:
```
client = RESTClient('4HntDYSlAIfaSCgx_EAYVBb24i********')
```
### Conexão com o BigQuery
Para se conectar ao BigQuery usamos a documentação da Google, onde criamos uma credencial para acesso:
```
# Configurar a autenticação do Google Cloud
SCOPES = ['https://www.googleapis.com/auth/cloud-platform']

credentials_info = {
  "type": "service_account",
  "project_id": "swift-climate-411900",
  "private_key_id": "06c41e4952cc58828bdfeb84ee0697537141472d",
  "private_key": "-----BEGIN PRIVATE KEY-----\******************iAgEAAoIBAQC7UEoEvNBn8IsJ\ni9ykIJDb8MyyO9n77jDgCH6EcmL374qDEYARcCS/gh9hi6SMrqilD5c0Z9q/ayDM\nS5VJZFAoxSxKL1ErH97cMmAtEMpygcaYoiBn/6zzz4mQaQUsiJX7dqIbbjgR/RHp\nm1/xkh51eFGN+BdDUmK13js7DKISmnmGpueCi3Qf6swaCM8O1m+E2wzrCpX8HHA1\ngXoKtpZwUXtGfnHkaFSBG2BxzHxJP07igQVO22fISLtIar9tKCp3HYSHUiOrYauA\nu5b5uOvsdHkBqt5DoKkk4b1svTL2HywQRDWzFkv+u/E5T9z3r61pO20sqxw6Nj+a\nrlm3V617AgMBAAECggEAKqwF0MJ679LaudgE8dcBaTmYSFpeNIh01fTZba7pYPZP\nJcDM3iwgEwi/wWcgm3QGs2Oz3Jp0OPtcw23MmqWhpUgV6OiSozJlgOBxDJMwq5lo\n9siofUi/y+NRwXZLWXPcRyclkv4vA9oVRJTC7LOPAM/iNGd6VTnlhu0TrUYZfgcT\nNahkbdfb6EfHM/MUHknmEwGuhAq1Y8Nhxp1isPrOlxx0vl3Wyk6wOujfNFT/7OfU\nKgise7iiNKK28FYCDWeBaaXFlPj28WqyJLqq15xpOHA6KzN+E+QzvxT66BU0wE1/\n9Wt763cJKO7mOVf+A0FQPaRtYqKfh6euFEo8FVlLwQKBgQDconH/iS2twjhC7OlT\nPhk7STVN6KBgmRYXbEoyf+gZsB3joxGv1f6bHTKuvqpCYL2Q6fSkGOFj+Vlc9PQ2\nbegQWDHapnqSn6tBigrscEqhzddd2oigdLnkUgO+FV0mcZMbVhLJJXIw+wp0WgYu\nUHMsAu5/wJNC7gM3WzdWcze1sQKBgQDZVowuaJv**********************nNNfIyaQBVJItCl0Q7bS/tQc2maZ0/1DsnLsLTJdR32FMX2GBs+GTU/yDzMVkffv3\n6etH54KlQb1U2Bflr0/8syrq6yS1Am8MdYwbZh5ijQmIhx2pHFNRtyceVzVWmdGb\n+/hcZtck6wKBgEeHhsvgrmV55QGViyOIq2d0GYrzkyMeHnJjkj6DBz1kwpvtXyuR\nhiTFt4u9lrdEY9DaeIzG4DOoQFeJtq76vNSnsyn+9RgaGcx6s4Xp9dg1QtBTrB3R\nGf8ys7HpfTScd6PSKO77a+UDTmVgVkanoF8xaB8U0OlO/s3wjaVCX1pRAoGAfyVG\ntQ+1x45M/9wfV1oeeRroB23hnmSofXpIksqEC9MyAO+DKpglmdGJ+sNAwklrSkEW\nm7GfBOKtxUQ3gu19FfeYTnLJN9UMRyit4E7r+0nOPYh90n0RSkB25x/RRaO624sZ\nAB5pwDXKUfjZvUk45SFE3VcfeR5bpelujoALdSkCgYAO5sALTFIYOlD/Gl3j3Gml\nU2qTteVMcFVPItv2k8Gw4txeZPIyNTR44PKPKj7hWapJNKiwsZvSkXVktwR+VmyF\nRgUdTgmAUFtJ+q1tZif+YVNh7zsNZLPHUCojqxVp9rrj57sGT/FzK5FQrQiBScbQkzpu2zfBqkUgW9Ukojug9g==\n-----END PRIVATE KEY-----\n",
  "client_email": "koru-466@swift-climate-411900.iam.gserviceaccount.com",
  "client_id": "*****2303438197562319",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/koru-466%40swift-climate-411900.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}
```

### Coletagem, Lógica e Ingestão de dados
Criamos em um único arquivo nomeado:
```
01-polygon_para_bigquery
```
Presente aqui no Github para avaliação.

Nesse arquivo ele pega as informações da cotação do EUR para Dolar, e sempre traz a última linha encontrada. Após coletagem envia esse dado a uma tabela do Banco de Dados criado na Google BigQuery

### Arquivo DAG
Criamos um único arquivo DAG com aciona o Python e executa o arquivo 01-polygon_para_bigquery a cada 20 minutos:
```
02-dag_koru_py
```
Presente aqui no Github para avaliação
### Criação de uma VM na Google Cloud
Foi criada uma VM com 2 núcleos de processamento e 16GB de Memória RAM, usando como sistema operacional o DEBIAN 11.9
Nela liberamos publicamente o IP e a porta 8080 para acesso externo do AIRFLOW
<img src="googlecloud.png">
### Instalação do Airflow
```
sudo apt-get update
sudo apt-get install python3-pip
pip3 --version
sudo pip3 install virtualenv
virtualenv -p python venv
source venv/bin/activate
pip3 install "apache-airflow==2.5.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-3.8.txt"
mkdir airflow_project
cd airflow_project
export AIRFLOW_HOME=/home/edsonpegorarotv/airflow_project
airflow db init
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email edsonpegoraro@gmail.com
airflow users list
airflow scheduler

-----------Segundo Terminal ----------
source venv/bin/activate

export AIRFLOW_HOME=/home/edsonpegorarotv/airflow_project

airflow webserver --port 8080
```
### Instalação do Postgresql

Para PRODUÇÃO a Apache recomenda a instalação do banco Postgresql para ser usado pelo AIRFLOW

```
sudo -u postgres psql 

CREATE DATABASE airflow;
CREATE USER airflow WITH ENCRYPTED PASSWORD 'sua_senha';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow_user;
```

No arquivo airflow.cfg alteramos o caminho do  sql_alchemy_conn
```
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost/airflow 
```

### Google BigQuery

Criamos um novo projeto no BigQuery nomeado <b>Koru</b>. Nele criamos uma base chamada <b>Indices</b>. Onde a aplicação enviará para uma tabela chamada <b>Euro</b> os dados da cotação.

<img src="bigquery.png">

### Dificuldades
A maioria dos módulos da Polygon.io são pagas, como todas os módulos Real Time. Escolhemos o Euro para Dólar, por ser free, porém é um dado diário e não minuto a minuto. Configuramos ele como se fosse realtime com intuito acadêmico apenas. Onde o arquivo DAG chama a ingestão a cada 20 minutos.

O AirFlow em Produção é bem diferente do que instalado em uma cloud. Exige troca de banco de dados e até um executor, com pouco material explicativo nas redes. Para nós iniciantes foi complicado. Conseguimos trocar o banco de dados para PostGreSQL mas o executor Celery recomendado não conseguimos encontrar material. Com isso nosso executor do Airflow para de executar após 15 minutos de ação.