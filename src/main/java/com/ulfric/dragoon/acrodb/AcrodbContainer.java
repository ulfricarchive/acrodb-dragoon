package com.ulfric.dragoon.acrodb;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;

import com.ulfric.acrodb.Bucket;
import com.ulfric.dragoon.ObjectFactory;
import com.ulfric.dragoon.application.Container;
import com.ulfric.dragoon.extension.inject.Inject;
import com.ulfric.dragoon.qualifier.Qualifier;
import com.ulfric.dragoon.reflect.Classes;
import com.ulfric.dragoon.stereotype.Stereotypes;

public class AcrodbContainer extends Container {

	@Inject
	private ObjectFactory factory;

	public AcrodbContainer() {
		addBootHook(this::bindAcrodb);
		addShutdownHook(this::unbindAcrodb);
	}

	private void bindAcrodb() {
		Bucket acrodb = new Bucket();
		factory.bind(Bucket.class).toFunction(parameters -> {
			Qualifier qualifier = parameters.getQualifier();
			if (qualifier != null) {
				Database database = Stereotypes.getFirst(qualifier, Database.class);

				if (database != null) {
					Bucket bucket = acrodb;
					for (String nextBucket : database.value()) {
						bucket = bucket.openBucket(nextBucket);
					}
					return bucket;
				}
			}

			return acrodb;
		});

		factory.bind(Store.class).toFunction(parameters -> {
			Objects.requireNonNull(parameters, "parameters");

			Qualifier qualifier = parameters.getQualifier();
			Objects.requireNonNull(qualifier, "qualifier");

			Type store = qualifier.getType();
			Class<? extends Document> type = getStoreType(store);

			Bucket bucket = factory.request(Bucket.class, parameters);

			return new Store<>(bucket, type);
		});
	}

	private Class<? extends Document> getStoreType(Type type) {
		if (type instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) type;
			Type[] arguments = parameterizedType.getActualTypeArguments();
			if (arguments.length > 0) {
				return Classes.getRawType(arguments[0]).asSubclass(Document.class);
			}
		}

		throw new IllegalStateException("Could not find store type from " + type);
	}

	private void unbindAcrodb() {
		factory.bind(Bucket.class).toNothing();
		factory.bind(Store.class).toNothing();
	}

}