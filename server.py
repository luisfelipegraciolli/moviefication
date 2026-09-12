"""
Servidor gRPC - Broker de Pub/Sub para tópicos de filmes.

Mantém, para cada filme, uma lista de assinantes. Cada assinante tem
uma fila (queue.Queue) própria e um filtro opcional de tópicos.
Quando alguém publica uma mensagem, o servidor a distribui (fan-out)
para todas as filas de assinantes daquele filme que estejam
interessados naquele tópico.
"""

import queue
import threading
import time
from concurrent import futures

import grpc

import movie_pubsub_pb2 as pb2
import movie_pubsub_pb2_grpc as pb2_grpc


class Broker:
    """Estrutura central de dados: filme -> lista de assinantes."""

    def __init__(self):
        self._lock = threading.Lock()
        # movie -> list[Subscriber]
        self._subscribers: dict[str, list["Subscriber"]] = {}

    def subscribe(self, movie: str, topics: set, name: str) -> "Subscriber":
        sub = Subscriber(movie=movie, topics=topics, name=name)
        with self._lock:
            self._subscribers.setdefault(movie, []).append(sub)
        print(f"[+] '{name}' assinou '{movie}' "
              f"(tópicos: {[pb2.TopicType.Name(t) for t in topics] if topics else 'TODOS'})")
        return sub

    def unsubscribe(self, sub: "Subscriber"):
        with self._lock:
            subs = self._subscribers.get(sub.movie, [])
            if sub in subs:
                subs.remove(sub)
        print(f"[-] '{sub.name}' saiu de '{sub.movie}'")

    def publish(self, message: pb2.Message) -> int:
        """Envia a mensagem para todos os assinantes interessados. Retorna quantos foram notificados."""
        count = 0
        with self._lock:
            subs = list(self._subscribers.get(message.movie, []))
        for sub in subs:
            if not sub.topics or message.topic in sub.topics:
                sub.queue.put(message)
                count += 1
        return count


class Subscriber:
    def __init__(self, movie: str, topics: set, name: str):
        self.movie = movie
        self.topics = topics  # set vazio = todos os tópicos
        self.name = name or "anonimo"
        self.queue: "queue.Queue[pb2.Message]" = queue.Queue()


class MoviePubSubServicer(pb2_grpc.MoviePubSubServicer):
    def __init__(self):
        self.broker = Broker()

    def Subscribe(self, request, context):
        topics = set(request.topics)  # vazio = todos
        sub = self.broker.subscribe(request.movie, topics, request.subscriber_name)

        def on_cancel():
            self.broker.unsubscribe(sub)

        context.add_callback(on_cancel)

        try:
            while context.is_active():
                try:
                    # timeout curto para poder checar se o contexto ainda está ativo
                    msg = sub.queue.get(timeout=1.0)
                    yield msg
                except queue.Empty:
                    continue
        except Exception:
            pass
        finally:
            self.broker.unsubscribe(sub)

    def Publish(self, request, context):
        # Para o tópico RATING, o conteúdo textual é opcional (o que importa é a nota).
        content_required = request.topic != pb2.RATING
        if not request.movie or (content_required and not request.content):
            return pb2.PublishResponse(
                success=False,
                detail="Campo 'movie' é obrigatório e 'content' é obrigatório (exceto para RATING).",
                subscribers_notified=0,
            )

        message = pb2.Message(
            movie=request.movie,
            topic=request.topic,
            content=request.content,
            timestamp=time.strftime("%Y-%m-%d %H:%M:%S"),
            author=request.author,
            rating_value=request.rating_value,
        )

        notified = self.broker.publish(message)
        topic_name = pb2.TopicType.Name(request.topic)
        print(f"[>] Publicado em '{request.movie}' / {topic_name}: {request.content!r} "
              f"-> {notified} assinante(s) notificado(s)")

        return pb2.PublishResponse(
            success=True,
            detail=f"Mensagem publicada em {request.movie}/{topic_name}",
            subscribers_notified=notified,
        )


def serve(port: int = 50051):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    pb2_grpc.add_MoviePubSubServicer_to_server(MoviePubSubServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Servidor MoviePubSub rodando na porta {port}. Ctrl+C para parar.")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    serve()
