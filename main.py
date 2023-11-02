import asyncio
import httpx
import json
import base64
from datetime import datetime, timedelta

# URL da API de autenticação
bearer_api_url = "https://gateway.apiserpro.serpro.gov.br/token"
# URL base da API Consulta NFE DF
base_url = "https://gateway.apiserpro.serpro.gov.br/consulta-nfe-df/api/v1/nfe"
# Lista de notas
my_file = open("resultado_c.txt", "r") 
  
# reading the file 
data = my_file.read() 
  
# replacing end splitting the text  
# when newline ('\n') is seen. 
items = data.split("\n") 
my_file.close() 
items = items[1000:100000]
print("Fim carregamento")

# Informações de autenticação da API de obtenção do token
client_id = "26swrMnAY4EQkFukCWLsJvolguMa"
client_secret = "nSSTNzbs6xPACsEnfqeQD0_eiCca"

# Número máximo de solicitações concorrentes
max_concurrent_requests = 14

batch_size = 1000
# Variável para armazenar o token atual e o horário de expiração
token_refresh_interval = None
current_token = None
token_expiration_time = None

# Define um semáforo para controlar as solicitações concorrentes
semaphore = asyncio.Semaphore(max_concurrent_requests)

async def get_bearer_token():
    global current_token, token_expiration_time

    # Verifica se já temos um token e se ele ainda é válido
    if current_token and token_expiration_time > datetime.now():
        return current_token

    auth = f"{client_id}:{client_secret}"
    base64_auth = base64.b64encode(auth.encode()).decode()
    headers = {
        "Authorization": f"Basic {base64_auth}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(bearer_api_url, headers=headers, data=data, timeout=100)
    except httpx.ConnectTimeout:
        raise Exception("Falha na conexão: não foi possível estabelecer uma conexão com o servidor")

    if response.status_code == 200:
        response_data = response.json()
        current_token = response_data.get("access_token", "")
        current_time = response_data.get("expires_in", "")
        # Define o tempo de expiração do token
        token_expiration_time = datetime.now() + timedelta(seconds=current_time)
        return current_token
    else:
        raise Exception("Falha ao obter o token Bearer")

async def fetch_data(item, session):
    bearer_token = await get_bearer_token()
    headers = {"Authorization": f"Bearer {bearer_token}"}
    url = f"{base_url}/{item}"
    async with semaphore:
        try:
            response = await session.get(url, headers=headers, timeout=100)
        except httpx.ConnectTimeout:
            print(f"Timeout de conexão ao acessar {url}")
            return

        if response.status_code == 200:
            data = response.json()
            filename = f"response_{response.status_code}_{item}.json"
            with open(filename, "w") as file:
                json.dump(data, file, indent=4)
            print(f"Salvou resposta 200 em {filename}")
        else:
            print(f"Requisição para {url} falhou com status {response.status_code}")

async def fetch_data_batch(batch):
    bearer_token = await get_bearer_token()
    async with httpx.AsyncClient() as session:
        tasks = [fetch_data(item, session) for item in batch]
        await asyncio.gather(*tasks)

# Divide a lista de itens em lotes de tamanho 'batch_size'
item_batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

async def main():
    for batch in item_batches:
        await fetch_data_batch(batch)

if __name__ == "__main__":
    asyncio.run(main())