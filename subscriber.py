"""
Cliente assinante (Subscriber).

Uso:
    python subscriber.py "Titanic"
    python subscriber.py "Titanic" REVIEW RATING
    python subscriber.py "Titanic" ACTOR_NEWS --name Ana
    python subscriber.py --menu
"""

import argparse
import sys

import grpc

import movie_pubsub_pb2 as pb2
import movie_pubsub_pb2_grpc as pb2_grpc


def parse_args():
    parser = argparse.ArgumentParser(description="Assina tópicos de um filme.")
    parser.add_argument("movie", nargs="?", help="Nome do filme, ex: Titanic")
    parser.add_argument(
        "topics",
        nargs="*",
        help="Tópicos a assinar (REVIEW RATING ACTOR_NEWS TRAILER BOX_OFFICE). Vazio = todos.",
    )
    parser.add_argument(
        "--menu",
        action="store_true",
        help="Abre um menu interativo para escolher filme e tópicos",
    )
    parser.add_argument(
        "--name",
        default="assinante",
        help="Nome do assinante (aparece nos logs do servidor)",
    )
    parser.add_argument(
        "--host",
        default="localhost:50051",
        help="Endereço do servidor gRPC",
    )
    return parser.parse_args()


def topic_from_name(name: str) -> int:
    try:
        return pb2.TopicType.Value(name.upper())
    except ValueError:
        validos = ", ".join(pb2.TopicType.keys())
        print(f"Tópico inválido: '{name}'. Válidos: {validos}")
        sys.exit(1)


def escolher_topicos():
    topicos = list(pb2.TopicType.keys())

    print("\nEscolha os tópicos:")
    print("0 - Todos")

    for numero, topico in enumerate(topicos, start=1):
        print(f"{numero} - {topico}")

    while True:
        escolha = input("\nDigite os números separados por vírgula: ").strip()

        if escolha in ("", "0"):
            return []

        try:
            numeros = {int(item.strip()) for item in escolha.split(",")}

            if not all(1 <= numero <= len(topicos) for numero in numeros):
                raise ValueError

            return [topicos[numero - 1] for numero in sorted(numeros)]
        except ValueError:
            print("Opção inválida. Exemplo válido: 1,3,5")


def obter_dados_do_menu():
    movie = input("Nome do filme: ").strip()

    while not movie:
        print("O nome do filme é obrigatório.")
        movie = input("Nome do filme: ").strip()

    name = input("Seu nome: ").strip() or "assinante"
    topics = escolher_topicos()

    return movie, topics, name


def main():
    args = parse_args()

    if args.menu:
        movie, topics, name = obter_dados_do_menu()
    else:
        if not args.movie:
            print("Informe o filme ou use --menu.")
            sys.exit(1)

        movie = args.movie
        topics = args.topics
        name = args.name

    topic_values = [topic_from_name(t) for t in topics]

    channel = grpc.insecure_channel(args.host)
    stub = pb2_grpc.MoviePubSubStub(channel)

    request = pb2.SubscribeRequest(
        movie=movie,
        topics=topic_values,
        subscriber_name=name,
    )

    print(
        f"Assinando '{movie}' "
        f"({'todos os tópicos' if not topic_values else ', '.join(topics)}) "
        f"como '{name}'... (Ctrl+C para sair)\n"
    )

    try:
        for message in stub.Subscribe(request):
            topic_name = pb2.TopicType.Name(message.topic)
            linha = f"[{message.timestamp}] {message.movie} / {topic_name}: {message.content}"

            if message.author:
                linha += f" (por {message.author})"

            if topic_name == "RATING":
                linha += f" [nota: {message.rating_value}]"

            print(linha)

    except KeyboardInterrupt:
        print("\nSaindo...")

    except grpc.RpcError as e:
        print(f"Erro de conexão com o servidor: {e.details() if hasattr(e, 'details') else e}")


if __name__ == "__main__":
    main()