"""
Cliente publicador (Publisher).

Uso:
    python publisher.py "Titanic" REVIEW "Um clássico atemporal" --author "Roger Ebert"
    python publisher.py "Titanic" RATING "" --rating 9.5
    python publisher.py "Titanic" ACTOR_NEWS "Leonardo DiCaprio anuncia novo filme" --author "Variety"

Tópicos disponíveis: REVIEW, RATING, ACTOR_NEWS, TRAILER, BOX_OFFICE
"""

import argparse
import sys

import grpc

import movie_pubsub_pb2 as pb2
import movie_pubsub_pb2_grpc as pb2_grpc


def parse_args():
    parser = argparse.ArgumentParser(description="Publica uma mensagem em um tópico de um filme.")
    parser.add_argument("movie", help="Nome do filme, ex: Titanic")
    parser.add_argument("topic", help="Tópico: REVIEW, RATING, ACTOR_NEWS, TRAILER ou BOX_OFFICE")
    parser.add_argument("content", help="Conteúdo da mensagem (texto da review/notícia/etc.)")
    parser.add_argument("--author", default="", help="Autor da review ou fonte da notícia")
    parser.add_argument("--rating", type=float, default=0.0, help="Nota (usada apenas no tópico RATING)")
    parser.add_argument("--host", default="localhost:50051", help="Endereço do servidor gRPC")
    return parser.parse_args()


def main():
    args = parse_args()

    try:
        topic_value = pb2.TopicType.Value(args.topic.upper())
    except ValueError:
        validos = ", ".join(pb2.TopicType.keys())
        print(f"Tópico inválido: '{args.topic}'. Válidos: {validos}")
        sys.exit(1)

    channel = grpc.insecure_channel(args.host)
    stub = pb2_grpc.MoviePubSubStub(channel)

    request = pb2.PublishRequest(
        movie=args.movie,
        topic=topic_value,
        content=args.content,
        author=args.author,
        rating_value=args.rating,
    )

    try:
        response = stub.Publish(request)
        status = "OK" if response.success else "FALHOU"
        print(f"[{status}] {response.detail} "
              f"({response.subscribers_notified} assinante(s) notificado(s))")
    except grpc.RpcError as e:
        print(f"Erro de conexão com o servidor: {e.details() if hasattr(e, 'details') else e}")


if __name__ == "__main__":
    main()
