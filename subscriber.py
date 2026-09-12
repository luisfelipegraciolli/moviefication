"""
Cliente assinante (Subscriber).

Uso:
    python subscriber.py "Titanic"                       # assina TODOS os tópicos
    python subscriber.py "Titanic" REVIEW RATING          # assina só review e rating
    python subscriber.py "Titanic" ACTOR_NEWS --name Ana  # com nome de identificação

Tópicos disponíveis: REVIEW, RATING, ACTOR_NEWS, TRAILER, BOX_OFFICE
"""

import argparse
import sys

import grpc

import movie_pubsub_pb2 as pb2
import movie_pubsub_pb2_grpc as pb2_grpc


def parse_args():
    parser = argparse.ArgumentParser(description="Assina tópicos de um filme.")
    parser.add_argument("movie", help="Nome do filme, ex: Titanic")
    parser.add_argument(
        "topics", nargs="*",
        help="Tópicos a assinar (REVIEW RATING ACTOR_NEWS TRAILER BOX_OFFICE). Vazio = todos.",
    )
    parser.add_argument("--name", default="assinante", help="Nome do assinante (aparece nos logs do servidor)")
    parser.add_argument("--host", default="localhost:50051", help="Endereço do servidor gRPC")
    return parser.parse_args()


def topic_from_name(name: str) -> int:
    try:
        return pb2.TopicType.Value(name.upper())
    except ValueError:
        validos = ", ".join(pb2.TopicType.keys())
        print(f"Tópico inválido: '{name}'. Válidos: {validos}")
        sys.exit(1)


def main():
    args = parse_args()
    topic_values = [topic_from_name(t) for t in args.topics]

    channel = grpc.insecure_channel(args.host)
    stub = pb2_grpc.MoviePubSubStub(channel)

    request = pb2.SubscribeRequest(
        movie=args.movie,
        topics=topic_values,
        subscriber_name=args.name,
    )

    print(f"Assinando '{args.movie}' "
          f"({'todos os tópicos' if not topic_values else ', '.join(args.topics)}) "
          f"como '{args.name}'... (Ctrl+C para sair)\n")

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
