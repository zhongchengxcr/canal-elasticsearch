package com.totoro.canal.es.consum.es;

public interface Consumer<T> {

    void consume(T object);
}
