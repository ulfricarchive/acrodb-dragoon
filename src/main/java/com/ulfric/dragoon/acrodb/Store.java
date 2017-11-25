package com.ulfric.dragoon.acrodb;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.ulfric.acrodb.Bucket;
import com.ulfric.acrodb.DocumentStore;
import com.ulfric.dragoon.extension.intercept.asynchronous.Asynchronous;

public final class Store<T extends Document> implements DocumentStore {

	private final Bucket bucket;
	private final Class<T> type;

	public Store(Bucket bucket, Class<T> type) {
		Objects.requireNonNull(bucket, "bucket");
		Objects.requireNonNull(type, "type");

		this.bucket = bucket;
		this.type = type;
	}

	@Asynchronous
	public CompletableFuture<T> getAsynchronous(Object key) {
		return CompletableFuture.completedFuture(get(key));
	}

	@Asynchronous
	public CompletableFuture<Void> persistAsynchronous(T document) {
		persist(document);
		return CompletableFuture.completedFuture(null);
	}

	public T get(Object key) {
		T document = openDocument(key(key)).read(type);
		if (document.getIdentifier() == null) {
			document.setIdentifier(key);
		}
		return document;
	}

	public void edit(Object key, Consumer<T> consumer) {
		openDocument(key(key)).edit(type, consumer);
	}

	public void persist(T document) {
		openDocument(key(document.getIdentifier())).write(document);
	}

	@Override
	public com.ulfric.acrodb.Document openDocument(String name) {
		return bucket.openDocument(name);
	}

	private String key(Object key) {
		Objects.requireNonNull(key, "key");
		return key.toString();
	}

}
