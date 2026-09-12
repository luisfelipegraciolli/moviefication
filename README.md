# Movie Pub/Sub (gRPC)

Aplicação de publicar/assinar (pub/sub) em gRPC, focada em tópicos sobre
filmes específicos (ex: "Titanic"). Cada filme pode ter vários tipos de
tópico, e um assinante escolhe quais quer receber.

## Tópicos disponíveis

- `REVIEW` — nova review/crítica do filme
- `RATING` — nova nota/avaliação do filme
- `ACTOR_NEWS` — notícia sobre algum ator/atriz do elenco
- `TRAILER` — novo trailer/teaser
- `BOX_OFFICE` — atualização de bilheteria

(Para adicionar um novo tipo de tópico, basta incluir um valor no enum
`TopicType` em `proto/movie_pubsub.proto` e regerar os stubs — veja abaixo.)

## Instalação

```bash
pip install -r requirements.txt
```

## Como rodar

### 1. Suba o servidor (broker)

```bash
python server.py
```

Ele escuta por padrão em `localhost:50051`.

### 2. Assine um filme em um ou mais terminais

```bash
# Assina TODOS os tópicos de Titanic
python subscriber.py "Titanic"

# Assina só REVIEW e RATING
python subscriber.py "Titanic" REVIEW RATING

# Com nome de identificação (aparece nos logs do servidor)
python subscriber.py "Titanic" ACTOR_NEWS --name Ana
```

O assinante fica recebendo mensagens em tempo real (stream gRPC) até
você apertar Ctrl+C.

### 3. Publique mensagens em outro terminal

```bash
python publisher.py "Titanic" REVIEW "Um clássico atemporal" --author "Roger Ebert"
python publisher.py "Titanic" RATING "" --rating 9.5
python publisher.py "Titanic" ACTOR_NEWS "Leonardo DiCaprio anuncia novo filme" --author "Variety"
python publisher.py "Titanic" TRAILER "Novo trailer da versão remasterizada já está no ar"
python publisher.py "Titanic" BOX_OFFICE "Ultrapassa 1 bilhão em arrecadação"
```

Cada assinante interessado naquele filme e naquele tópico recebe a
mensagem instantaneamente.

## Como funciona

- **`proto/movie_pubsub.proto`** define o serviço `MoviePubSub` com dois
  métodos:
  - `Subscribe` (streaming server-side): o cliente assina um filme e uma
    lista de tópicos (vazia = todos) e recebe um stream contínuo de
    `Message`.
  - `Publish` (unário): o cliente publica uma `Message` em um tópico de
    um filme específico.
- **`server.py`** implementa o broker: mantém, para cada filme, a lista
  de assinantes conectados (cada um com sua fila e filtro de tópicos).
  Quando uma mensagem é publicada, o servidor faz o fan-out para todas
  as filas de assinantes interessados.
- **`subscriber.py`** e **`publisher.py`** são clientes de linha de
  comando prontos para uso e para servirem de exemplo de integração.

## Regenerando os stubs (caso edite o .proto)

```bash
python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/movie_pubsub.proto
```
# moviefication
