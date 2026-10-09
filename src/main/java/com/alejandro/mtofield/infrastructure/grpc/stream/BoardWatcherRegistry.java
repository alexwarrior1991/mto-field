package com.alejandro.mtofield.infrastructure.grpc.stream;

import com.alejandro.mtofield.application.dto.BoardSnapshot;
import com.alejandro.mtofield.application.service.BoardPublisher;
import com.alejandro.mtofield.grpc.v1.PossessionBoard;
import com.alejandro.mtofield.infrastructure.grpc.mapper.FieldProtoMapper;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Los observadores del tablero de cada posesion en esta replica; convierte cada tablero una vez para todos. */
@Component
public class BoardWatcherRegistry implements BoardPublisher {

    private final Map<UUID, Set<BoardWatcher>> watchers = new ConcurrentHashMap<>();

    /** Registra antes de la primera publicacion: lo que se publique entre medias no se pierde. */
    public void register(UUID possessionId, BoardWatcher watcher) {
        watchers.computeIfAbsent(possessionId, id -> ConcurrentHashMap.newKeySet()).add(watcher);
    }

    public void unregister(UUID possessionId, BoardWatcher watcher) {
        Set<BoardWatcher> set = watchers.get(possessionId);
        if (set != null) {
            set.remove(watcher);
            watchers.computeIfPresent(possessionId, (id, current) -> current.isEmpty() ? null : current);
        }
    }

    @Override
    public boolean hasWatchers(UUID possessionId) {
        Set<BoardWatcher> set = watchers.get(possessionId);
        return set != null && !set.isEmpty();
    }

    @Override
    public Set<UUID> watchedPossessions() {
        return Set.copyOf(watchers.keySet());
    }

    @Override
    public void publish(BoardSnapshot board) {
        Set<BoardWatcher> set = watchers.get(board.possessionId());
        if (set == null || set.isEmpty()) {
            return;
        }
        PossessionBoard proto = FieldProtoMapper.toProto(board);
        for (BoardWatcher watcher : set) {
            watcher.publish(proto);
        }
    }

    @Override
    public void completeAll(BoardSnapshot last) {
        Set<BoardWatcher> set = watchers.remove(last.possessionId());
        if (set == null) {
            return;
        }
        PossessionBoard proto = FieldProtoMapper.toProto(last);
        for (BoardWatcher watcher : List.copyOf(set)) {
            watcher.publish(proto);
            watcher.complete();
        }
    }

    /** Todos los observadores abiertos en esta replica, de cualquier posesion. */
    public List<BoardWatcher> all() {
        return watchers.values().stream().flatMap(Set::stream).toList();
    }

    public int watchers(UUID possessionId) {
        Set<BoardWatcher> set = watchers.get(possessionId);
        return set == null ? 0 : set.size();
    }
}
