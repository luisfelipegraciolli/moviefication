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
import threading

import movie_pubsub_pb2 as pb2
import movie_pubsub_pb2_grpc as pb2_grpc
from server import Subscriber


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

#Thread separada
def escutar(stub, movie, name, topics_names, subscriptions, movie_key):
    topic_values = [topic_from_name(t) for t in topics_names]
    request = pb2.SubscribeRequest(
        movie=movie,
        topics=topic_values,
        subscriber_name=name
    )
    call = stub.Subscribe(request)
    subscriptions[movie_key]["call"] = call

    print(
        f"\n[stream] Ouvindo '{movie}' "
        f"({'todos os tópicos' if not topic_values else ', '.join(topics_names)})\n"
    )

    try:
        for message in call:
            topic_name = pb2.TopicType.Name(message.topic)
            linha = f"[{message.timestamp}] {message.movie} / {topic_name}: {message.content}"
            if message.author:
                linha += f" (por {message.author})"
            if topic_name == "RATING":
                linha += f" [nota: {message.rating_value}]"
            print(linha)
    except grpc.RpcError:
        pass


def iniciar_assinatura(stub, movie, name, topics_names, subscriptions):
    """Cria (ou substitui) a assinatura de um filme, em sua própria thread."""
    movie_key = movie.lower()
    subscriptions[movie_key] = {"topics": set(topics_names), "call": None, "thread": None}

    t = threading.Thread(
        target=escutar,
        args=(stub, movie, name, topics_names, subscriptions, movie_key),
        daemon=True,
    )
    subscriptions[movie_key]["thread"] = t
    t.start()


def main():
    args = parse_args()

    if args.menu:
        movie = input("Nome do filme: ").strip()
        while not movie:
            print("O nome do filme é obrigatório.")
            movie = input("Nome do filme: ").strip()
        name = input("Seu nome: ").strip() or "assinante"
        topics = escolher_topicos()
    else:
        if not args.movie:
            print("Informe o filme ou use --menu.")
            sys.exit(1)
        movie = args.movie
        name = args.name
        topics = args.topics

    channel = grpc.insecure_channel(args.host)
    stub = pb2_grpc.MoviePubSubStub(channel)

    subscriptions = {}  # movie_key -> {"topics": set, "call": ..., "thread": ...}
    iniciar_assinatura(stub, movie, name, topics, subscriptions)

    try:
        while True:
            print("\nFilmes assinados no momento:",
                  ", ".join(sorted(subscriptions)) or "(nenhum)")
            comando = input(
                "Digite 'novo' para assinar outro filme, "
                "'add' para adicionar tópicos a um filme já assinado, "
                "ou 'sair': "
            ).strip().lower()

            if comando == "sair":
                break

            elif comando == "novo":
                novo_filme = input("Nome do filme: ").strip()
                if not novo_filme:
                    print("Nome do filme é obrigatório.")
                    continue
                novos_topicos = escolher_topicos()
                iniciar_assinatura(stub, novo_filme, name, novos_topicos, subscriptions)

            elif comando == "add":
                if not subscriptions:
                    print("Você ainda não assina nenhum filme.")
                    continue
                alvo = input(
                    f"Qual filme? ({', '.join(sorted(subscriptions))}): "
                ).strip().lower()
                if alvo not in subscriptions:
                    print("Filme não encontrado entre os assinados.")
                    continue

                novos = escolher_topicos()
                atuais = subscriptions[alvo]["topics"]

                if not novos or not atuais:
                    topicos_final = []  # "todos"
                else:
                    topicos_final = list(atuais | set(novos))

                # cancela o stream antigo desse filme e abre um novo já atualizado
                call_antigo = subscriptions[alvo]["call"]
                if call_antigo is not None:
                    call_antigo.cancel()

                # usa o nome "bonito" original do filme (não o lowercase da chave)
                iniciar_assinatura(stub, alvo, name, topicos_final, subscriptions)

    except KeyboardInterrupt:
        pass
    finally:
        for info in subscriptions.values():
            if info["call"] is not None:
                info["call"].cancel()
        print("\nSaindo...")


if __name__ == "__main__":
    main()