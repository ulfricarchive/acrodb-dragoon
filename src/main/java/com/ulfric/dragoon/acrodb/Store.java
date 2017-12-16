package com.ulfric.dragoon.acrodb;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

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
	public CompletableFuture<T> getAsynchronous(String key) {
		return CompletableFuture.completedFuture(get(key));
	}

	@Asynchronous
	public CompletableFuture<Void> persistAsynchronous(T document) {
		persist(document);
		return CompletableFuture.completedFuture(null);
	}

	public T get(String key) {
		T document = openDocument(key(key)).read(type);
		if (document.getIdentifier() == null) {
			document.setIdentifier(key);
		}
		return document;
	}

	public void edit(Object key, Consumer<T> consumer) {
		openDocument(key(key)).editAndWrite(type, consumer);
	}

	public boolean edit(Object key, Predicate<T> consumer) {
		return openDocument(key(key)).editAndWriteIf(type, consumer);
	}

	public void persist(T document) {
		openDocument(key(document.getIdentifier())).write(document);
	}

	public Stream<T> getAllDocuments() {
		return bucket.loadAllDocuments()
				.map(document -> document.read(type));
	}

	@Override
	public com.ulfric.acrodb.Document openDocument(String name) {
		return bucket.openDocument(name);
	}

	private String key(Object key) {
		Objects.requireNonNull(key, "key");
		return key.toString();
	}

	@Override
	public void deleteDocument(String name) {
		bucket.deleteDocument(name);
	}

	@Override
	public Stream<com.ulfric.acrodb.Document> loadAllDocuments() {
		return bucket.loadAllDocuments();
	}

}
